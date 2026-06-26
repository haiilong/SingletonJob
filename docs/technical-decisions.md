# Technical decisions in SingletonJob

A deep-dive into the design choices in this library, why each one was made, the trade-offs
involved, and the follow-up questions you should be ready for. Every section points at the
real code so you can speak to specifics, not hand-waving.

The library does one thing: across N replicas of a process, make sure **exactly one** runs a
given periodic job, and hand leadership over quickly when that one dies. Everything below is
in service of that, plus making it AOT-safe and cheap.

---

## 1. Distributed lock = leader election (the core idea)

A "singleton job across N pods" is just a distributed mutual-exclusion problem. The chosen
primitive is a **single Redis key with a TTL** whose value is the owning node's id:

```
{ProjectName}:{JobName}:lock   →   key
{NodeId}                       →   value
```

- One key per job class (`SingletonBackgroundJob.cs:143`).
- The **value is a fencing identity**: only the node whose id matches the value may renew or
  release the lock. This is what stops node B from renewing or deleting node A's lock.
- The key has a **TTL** (`LockExpiry`, default 10 s). If the holder dies, the key expires and
  someone else can take it. The TTL is the failure-detection mechanism — no heartbeat registry,
  no separate liveness service.

**Why Redis and not, say, a database row or ZooKeeper/etcd?** The job is high-frequency and
disposable. We don't want persistence, history, or consensus overhead. A single Redis key with
a TTL is ~50 bytes and one round trip per heartbeat. The README frames this explicitly as the
anti-Hangfire: no durable job store, no retries, no dashboard.

**Interview framing — be honest about the correctness ceiling.** A single-instance Redis lock is
*not* a provably-safe distributed lock (this is the Kleppmann/Redlock debate). Under a GC pause
or network partition, two nodes can briefly believe they're leader. This library does **not**
claim perfect mutual exclusion; it claims *best-effort singleton with a bounded, shrinkable
duplicate-execution window*. The mitigations (sections 2 and 6) are what make that window small,
and the docs tell users to keep jobs idempotent. If an interviewer pushes on "is this a correct
lock?", the right answer is: "No general distributed lock over a single Redis is fully safe; this
trades that for simplicity and is layered with self-fencing and optional mid-run cancellation to
make the unsafe window small. For workloads needing hard exclusivity you'd want fencing tokens
enforced at the *resource*, which is out of scope here."

---

## 2. One atomic Lua script for acquire-OR-renew (not SETNX + a separate renew)

`SingletonBackgroundJob.cs:84-88`:

```lua
local v = redis.call('GET', KEYS[1])
if not v then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1
elseif v == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1
else return 0 end
```

The naive design is `SET key val NX PX ttl` to acquire, and a separate script to renew. This
collapses both into **one round trip per heartbeat**:

- Key free → take it, return 1 (became/stayed leader).
- Key holds *my* id → extend the TTL, return 1 (steady-state leader: this is the common path).
- Key holds *someone else's* id → return 0 (I'm a follower).

**Why one script instead of `SET NX`?** Because the steady-state leader does this every 3 s
forever. A single GET-branch-SET/PEXPIRE script means the leader's renewal and a follower's
acquire attempt are *the same call*. No "try SETNX, it fails, now run a different renew command"
two-step. Fewer round trips, one code path.

**Why Lua / why atomic?** GET-then-SET from the client is a race: between your GET and your SET
another node can win. Running it server-side as a Lua script makes the check-and-set atomic
(Redis executes a script with no interleaving). The `v == ARGV[1]` ownership check inside the
script is the fencing guarantee — it's impossible to renew a lock you don't own, even under
concurrency, because the comparison and the PEXPIRE happen atomically.

**Release is the mirror image** (`SingletonBackgroundJob.cs:91-92`): `GET == myId ? DEL : 0`.
Atomic compare-and-delete so you never delete a lock that already rolled over to a peer.

Follow-ups to expect: *Why PX/PEXPIRE (milliseconds) not EX?* Sub-second precision for
high-frequency jobs. *What if the script partially runs?* Redis scripts are atomic and run to
completion or not at all — there's no partial state.

---

## 3. Self-fencing on missed renewals (the subtle correctness piece)

The Lua script only detects "I lost the lock" when **Redis is reachable** and returns 0. But the
dangerous case is a one-sided partition: *this* node can't reach Redis, the key expires
server-side, a peer takes over — and meanwhile this node is happily still running the job because
its last renewal call is just hanging or erroring.

The fix is a **local lease deadline** (`SingletonBackgroundJob.cs:50-64`):

```csharp
private bool LeaseIsValid =>
    TimeProvider.GetElapsedTime(Volatile.Read(ref _leaseStartTimestamp)) < _options.LockExpiry;

protected bool IsLeader => Volatile.Read(ref _isLeader) && LeaseIsValid;
```

Every successful acquire/renew records `_leaseStartTimestamp`. `IsLeader` is **`_isLeader` AND the
lease hasn't lapsed locally**. So even if the election loop is stuck retrying a dead Redis, the
job loop stops running work once `LockExpiry` elapses since the last confirmed renewal. The node
self-demotes without needing to hear from Redis.

**The detail worth memorizing:** the timestamp is taken **before** the Redis call
(`SingletonBackgroundJob.cs:392`, with the comment at `:50-52`). The server applies its TTL at
some point *after* the client takes the timestamp, so a before-the-call timestamp makes the local
fence **at least as strict** as the server-side expiry. If you took it *after* the call returned,
your local deadline could outlive the server's key, reopening the duplicate-execution window. This
is the kind of off-by-one-instant reasoning that's easy to get wrong and great to be able to
explain.

`GetElapsedTime` over a captured timestamp uses the **monotonic clock**, not wall time, so an NTP
adjustment can't corrupt the lease math.

---

## 4. Release-on-shutdown for fast failover

On graceful shutdown the leader runs the release script instead of just letting the key expire
(`SingletonBackgroundJob.cs:434-450`, `ExecuteAsync` finally at `:215-225`).

**Why bother?** Without release, peers wait up to `LockExpiry` (10 s) before the key expires and
they can acquire. With release, the key is gone immediately and the next pod acquires on its next
heartbeat — within `HeartbeatInterval` (3 s). On a Kubernetes rolling deploy that's "10 s of
nobody running the job" turning into "~3 s". That's the single biggest operational win for the
common case (deploys), and it's basically free.

The release path is gated on hard kill: SIGKILL/OOM/partition never run the finally, so the lock
just expires — correctness is preserved, you only lose the fast-failover optimization.

**Ordering detail:** the finally **cancels the election loop and awaits it to drain before
releasing** (`:217-224`). Otherwise you'd `DEL` the key and then have an in-flight renewal
re-`SET` it a millisecond later, resurrecting a lock nobody owns.

---

## 5. Two parallel loops + lock-free leadership flag

Each job is **one `BackgroundService`** running **two concurrent loops** (`ExecuteAsync` at
`:194-226`):

- **Election loop** (`RunLeaderElectionLoopAsync`) — heartbeats Redis, the *only writer* of
  `_isLeader`.
- **Job loop** (`ExecuteJobLoopAsync`, shape-specific) — checks `IsLeader` each iteration and runs
  work only if true.

`_isLeader` is a plain `bool` accessed through `Volatile.Read`/`Volatile.Write` — **single writer,
N readers**, no lock. The reasoning (`architecture.md:69`): losing leadership only needs to be
*observed* by the job loop within one tick. Stale-by-one-tick visibility is acceptable because the
lease fence (section 3) is the real safety net. So a full lock or `Interlocked` would buy nothing;
`Volatile` gives the publication guarantee (no torn/cached reads) at the lowest cost.

**The linked-CTS detail** (`:204-209`): the election loop is started on a token *linked* to the
job loop. If the job loop exits for a non-shutdown reason — invalid config, an exception escaping,
a cron with no future occurrences — the election loop is torn down too. Without this, the finally
would await the election loop until host shutdown, **keeping the lock alive while no work runs** —
a silent "leader holds the lock but does nothing" failure. Linking makes failure visible and
releases the lock.

---

## 6. Per-leadership-term CancellationToken (`CancelOnLostLeadership`)

By default a started iteration runs to completion even if the node loses leadership mid-run
(matches 1.0 behavior). Opt into `CancelOnLostLeadership` and the token handed to your job *also*
fires when this node's leadership term ends — lease expiry, preemption, live-disable, or shutdown
(`SingletonJobOptions.cs:52-59`, `ExecuteIterationAsync` at `:247-260`).

Implementation is a **CTS per leadership term**: created on promotion, cancelled on every demotion
path (`_termCts`, `:46-47`, `EndLeadershipTerm` at `:262`). The iteration links the stopping token
with the current term's token so cancelling the term cancels the work.

Two subtle bits worth calling out:

- **Publish ordering** (`MaintainLeadershipAsync`, `:402-408`): the new term CTS is written
  *before* `_isLeader = true`. So any reader that sees `IsLeader == true` is guaranteed to also see
  a non-null term source — you can't observe leadership without its cancellation handle.
- **The CTS is cancelled but never disposed** (comment at `:46-47`). An in-flight iteration may
  still hold a linked source over the term token, and a CTS with no timer registered needs no
  disposal. Disposing it while a linked child references it would be a race for no benefit.

This is the second half of the duplicate-execution mitigation: section 3 stops *new* iterations;
this optionally cancels the *current* one. It only helps if your job actually honors its token —
which the docs are explicit about.

---

## 7. Three job shapes, three different timing semantics

All three derive from `SingletonBackgroundJob` and differ only in `ExecuteJobLoopAsync`.

| Shape | Semantics | Mechanism |
|---|---|---|
| `SingletonIntervalJob` | "at least N between runs" — wait measured from **end** of previous run | `await work; await Task.Delay(interval)` loop (`SingletonIntervalJob.cs:40-78`) |
| `SingletonFixedRateJob` | fire on a **fixed wall-clock cadence**, drop overlapping ticks | `PeriodicTimer` (`SingletonFixedRateJob.cs:44-71`) |
| `SingletonCronJob` | wall-clock schedule ("3am daily") | Cronos + sleep-until-next (`SingletonCronJob.cs`) |

**Interval vs fixed-rate is a real distinction**, not cosmetic. Interval re-reads `GetJobInterval()`
*after every iteration* so it can be dynamic; the gap is end-to-start, so a slow run pushes the
next one out. Fixed-rate reads the interval *once* (the `PeriodicTimer` is constructed with it) and
ticks on absolute instants regardless of how long work takes.

### Drop-on-overlap (the fixed-rate headline feature)

`SingletonFixedRateJob.cs:52-56`:

```csharp
if (IsLeader && IsEnabled && !_isJobRunning)
{
    _isJobRunning = true;
    _currentRun = ExecuteAndResetFlagAsync(stoppingToken);
}
```

A `volatile bool _isJobRunning` guards execution. If a tick arrives while the previous run is
still going, the tick is **dropped** — no queueing, no overlap, no unbounded backlog. This is the
semantic Hangfire's recurring runner doesn't give you (overlapping runs queue up). The run is
fire-and-forget (`_currentRun` is not awaited in the loop) so the timer keeps ticking; the flag is
reset in the run's `finally`.

**Graceful-shutdown detail** (`:64-70`): on exit the loop awaits the most recent `_currentRun`, so
shutdown actually waits for in-flight work instead of abandoning it.

---

## 8. Cron correctness details (Cronos + absolute-instant arithmetic)

`SingletonCronJob.cs` has more edge-case handling than the other two shapes:

- **Absolute-instant pivot** (`:71`, `:90`): the loop tracks the next-occurrence pivot as a UTC
  instant and passes `TimeZone` to Cronos. Doing the arithmetic on absolute instants — and letting
  Cronos handle the zone — keeps DST transitions unambiguous. (Computing "next 2:30am" by adding to
  a local time breaks twice a year.)
- **Strict-advance guard** (`:65-67`, `:84-88`): for second-precision expressions like `* * * * * *`,
  Cronos could return a value at/before the pivot; the loop forces the pivot strictly forward to
  avoid a busy loop.
- **Chunked sleep** (`MaxSleepChunk = 1 day`, `:24-25`, `:124-128`): `Task.Delay` can't represent
  more than ~49.7 days, and a sparse cron (yearly) easily exceeds that. Sleeping in ≤1-day chunks
  and recomputing the remainder also keeps the wake-up accurate if the **system clock is adjusted**
  during a long sleep.

### Misfire policy (`CronMisfirePolicy.cs`, handled at `:92-118`)

When an occurrence passes without firing (previous run overran, process suspended, clock jumped):

- **`Skip`** (default): drop everything missed, resume at the next future occurrence. Right for
  frequent schedules where the next run supersedes the missed one.
- **`FireOnce`**: collapse all missed occurrences into one immediate catch-up run.
- **`CatchUp`**: replay each missed occurrence back-to-back (distinct time-bucket processing).

**The honesty note matters in an interview** (`CronMisfirePolicy.cs:5-7`, `configuration.md:124-125`):
this policy only covers occurrences missed *while the process was running*. An occurrence missed
because **no replica held leadership at that instant** (mid-failover) is *not* detected — from each
node's view its loop fired on time, it just wasn't leader. Knowing the limit of your own feature is
a strong signal.

---

## 9. Backoff that exempts the leader (and the math behind it)

On consecutive Redis errors the follower backs off exponentially with jitter
(`ComputeHeartbeatDelay`, `:325-348`):

```
delay = min(HeartbeatInterval × 2^failures, MaxBackoffDelay) ± 20% jitter
```

Two decisions here:

1. **A leader with a still-valid lease does NOT back off** (`:335`). It keeps retrying at the plain
   `HeartbeatInterval`. The reasoning is arithmetic: with the recommended `LockExpiry ≥ 3 ×
   HeartbeatInterval`, *two* doubled delays already exceed the TTL. If a leader backed off on a
   couple of transient hiccups it would forfeit its own lock before the next attempt — a
   self-inflicted leadership flap. So it retries aggressively until the lease actually lapses, at
   which point it self-demotes (section 3) and the *follower* backoff takes over.

2. **±20% jitter** (`:344-347`) so N replicas that all lost Redis at the same instant don't
   reconnect in lockstep and stampede Redis when it recovers (thundering herd). Classic, but the
   leader-exemption interplay is the non-obvious part.

`ComputeHeartbeatDelay` is `internal static` and **pure** (`:324-325`) specifically so it can be
unit-tested without a Redis connection — a deliberate testability decision.

---

## 10. EVALSHA with a precomputed SHA and NOSCRIPT fallback

Scripts are sent by **SHA1 hash via EVALSHA**, not as full text each call
(`AcquireOrRenewScriptSha` at `:96-97`, `EvalScriptAsync` at `:422-432`):

```csharp
try { return await _db.ScriptEvaluateAsync(scriptSha, keys, args); }
catch (RedisServerException ex) when (ex.Message.StartsWith("NOSCRIPT", ...))
{ return await _db.ScriptEvaluateAsync(script, keys, args); }  // EVAL re-registers it
```

- The SHA is computed **once at type init** so each heartbeat ships a 40-byte hash instead of the
  full script body. Over a high-frequency job that's a real bandwidth saving.
- `SHA1` here is the **Redis protocol's script id**, not a security primitive — worth saying out
  loud so nobody thinks it's a crypto smell.
- **NOSCRIPT fallback**: the first call against a fresh server, or any time Redis is
  restarted/failed-over and its script cache is flushed, EVALSHA throws `NOSCRIPT`. The catch falls
  back to full `EVAL`, which *also re-registers* the script server-side, so subsequent EVALSHA
  calls succeed again. Self-healing, no warm-up step.

---

## 11. Source generator for registration (the AOT story)

`AddSingletonJobs` is **emitted at compile time** by an incremental Roslyn generator
(`SingletonJobGenerator.cs`), not discovered by reflection at runtime.

**Why:** the obvious implementation is `Assembly.GetTypes()` + `IsSubclassOf` at startup. That's
reflection over all types — incompatible with trimming and NativeAOT (the trimmer can't see which
types are needed, so it either keeps everything or breaks). The generator scans the compilation,
finds every non-abstract `SingletonBackgroundJob` subclass, and emits explicit
`TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ConcreteJob>())` lines
(`:104-107`). Zero reflection in the registration path → `IsAotCompatible` and `IsTrimmable` hold.

Decisions inside the generator:

- **Incremental** (`IIncrementalGenerator`, syntax predicate at `:48-55`): a cheap syntax-level
  filter (class, has a base list, not `abstract`) runs on every keystroke; the expensive semantic
  check (walk the base-type chain, `:63-67`) only runs on candidates. Keeps the IDE responsive.
- **Emitted as `internal`** (`:94`): each consuming assembly gets its own private copy, so two
  projects in one solution can't collide on the symbol.
- **Deterministic output**: candidates are de-duped and ordered ordinally (`:72`) so the generated
  file is stable build-to-build (no spurious diffs / cache busts).
- **Generic jobs get a diagnostic, not silent failure** (`SJOB001`, `:21-29`, `:74-77`): a
  `ServiceDescriptor` needs a closed constructible type, so an open generic job can't be
  registered. The generator skips it and emits a warning telling you to register it manually —
  rather than producing code that won't compile.
- **`TryAddEnumerable`** (not `AddSingleton`): idempotent registration into the `IHostedService`
  collection; calling `AddSingletonJobs` twice won't double-register.

**The honest trade-off** (README `:59-61`, troubleshooting `:51-53`): before the first `dotnet
build` the symbol doesn't exist, so the IDE red-squiggles `AddSingletonJobs` with CS1061. That's
the cost of compile-time generation, and the docs lead with it so nobody files it as a bug. Being
able to name the downside of your own design is the point.

---

## 12. AOT-safe options binding (manual, not `ConfigurationBinder.Bind`)

The config binding (`ServiceCollectionExtensions.cs:56-72`) reads each key explicitly and parses
with `TimeSpan.TryParse`/`bool.TryParse`:

```csharp
if (TimeSpan.TryParse(section["HeartbeatInterval"], out var hb)) o.HeartbeatInterval = hb;
```

**Why not `section.Bind(options)`?** `ConfigurationBinder.Bind` is reflection-based and trips the
trimmer/AOT analyzer. Hand-binding the half-dozen known fields keeps the whole library
reflection-free, consistent with the source-generator decision. It's more code, but it's the price
of a genuinely AOT-clean library — and there are only a handful of options, so it's cheap.

`ConfigureAll` is used (`:40`) so the same binding applies to *every named* options instance
including the default — a separate `Configure<T>` for the default name would just run the same
binding twice.

---

## 13. Per-job config via named options, frozen at startup

Each job resolves its options with `IOptionsFactory<SingletonJobOptions>.Create(JobName)` once in
`StartAsync` (`:138`). This is the standard *named options* pattern repurposed as per-job config:

- Defaults from `appsettings.json` apply to all jobs (via `ConfigureAll`).
- Override one job with `PostConfigureSingletonJob("heavy-job", o => o.LockExpiry = ...)`
  (`ServiceCollectionExtensions.cs:81-88`) — a `PostConfigure(name, ...)` that runs *after* the
  base binding, so you only specify what changes.
- **Frozen at startup** (`configuration.md:33`): `Create` snapshots the value; no `IOptionsMonitor`
  live-reload subscription. The rationale is that lock timing changing under a running election
  loop would be a footgun; redeploy to change it. Simpler and safer.

The application order (defaults → section bind → ConfigureAll → PostConfigure) is documented at
`configuration.md:42-48`.

---

## 14. Fail-fast options validation at host start

Validation runs through `IValidateOptions<SingletonJobOptions>` (`SingletonJobOptionsValidator`,
`:96-112`) plus a **tiny hosted service that just touches `IOptions.Value`** at startup
(`SingletonJobOptionsValidationStartup`, `:125-134`).

**Why the extra hosted service?** Touching `.Value` is what triggers the validators. The default
instance would otherwise not be validated until something resolves it — potentially the first job
tick. The startup service forces validation **the moment the host starts**, so bad config throws
`OptionsValidationException` immediately, not minutes later mid-run.

Two more touches:
- Per-job overrides are validated too, because `IOptionsFactory.Create(name)` runs the validators
  for that named instance during the job's `StartAsync`; the failure message is prefixed
  `[Job: name]` (`:107-109`) so you know which override is wrong.
- The startup service is implemented locally against `Hosting.Abstractions` only (`:120-123`),
  deliberately avoiding a dependency on the full `Microsoft.Extensions.Hosting` package just to get
  `ValidateOnStart()`. Keeps the dependency footprint minimal — relevant for a library.

`ProjectName` has **no default and is required** (`SingletonJobOptions.cs:17-21`, `Validate` at
`:71-79`). A shared default like `"default"` would let two unrelated deployments on the same Redis
silently collide on `default:heartbeat:lock`. Forcing an explicit value trades a tiny bit of
friction for eliminating a nasty cross-deployment bug.

`Validate` also enforces `LockExpiry > HeartbeatInterval` and `MaxBackoffDelay >=
HeartbeatInterval` (`:80-87`) — the invariants the timing logic depends on.

---

## 15. Duplicate-JobName detection via a static type registry

`SingletonBackgroundJob.cs:79`, checked at `:149-156`:

```csharp
private static readonly ConcurrentDictionary<string, Type> RegisteredLockKeys = new();
...
var owner = RegisteredLockKeys.GetOrAdd(_lockKey, GetType());
if (owner != GetType()) throw new InvalidOperationException("Duplicate job name ...");
```

Two different job classes accidentally sharing a `JobName` produce the same lock key, silently
contend for the same lock, and **only one ever runs** — a maddening bug. The static registry
catches it at startup with a clear error.

The cleverness is **keying by lock key but storing the `Type`**: multiple *instances of the same
class* (e.g. the test suite simulating N replicas in one process) are allowed — `owner ==
GetType()` passes. Only a *different type* on the same key throws. Cleaned up in `Dispose`
(`:163-169`) so re-running in the same process works.

---

## 16. `TimeProvider` injected everywhere for virtual-time tests

Every wait, every lease timestamp, and every cron evaluation goes through `TimeProvider`
(`:36`, constructors at `:107-130`) — `Task.Delay(..., TimeProvider, ...)`, `PeriodicTimer(...,
TimeProvider)`, `TimeProvider.GetTimestamp()`, `TimeProvider.GetUtcNow()`.

**Why:** leader election is full of timing — heartbeats, lease expiry, backoff, cron sleeps.
Testing that against the wall clock means real `sleep`s and flaky tests. With `TimeProvider`
injected, the test suite passes a `FakeTimeProvider` and advances virtual time instantly:
"fast-forward 11 seconds, assert the lease expired and the node self-demoted" runs in microseconds
and deterministically. The `TimeProviderTests` and `HeartbeatDelayTests` in the suite exercise
exactly this.

Note the deliberate split: durations use `GetTimestamp`/`GetElapsedTime` (**monotonic**, immune to
clock changes — used for the lease and execution-time measurement), while cron uses `GetUtcNow`
(**wall clock** — because a cron schedule *is* defined in wall-clock terms). Picking the right
clock for each purpose is itself a decision.

---

## 17. Smaller decisions worth a sentence each

- **Interval bounds guard** (`ValidateJobInterval`, `:231-240`): `Task.Delay`/`PeriodicTimer` reject
  anything ≤0 or >~49.7 days with a bare `ArgumentOutOfRangeException` from deep in the loop. The
  guard fails early with a message that **names the job**, turning a cryptic stack trace into an
  actionable error.
- **80%-of-LockExpiry warning** (`WarnIfExecutionTimeTooLong`, `:268-277`): if an iteration takes
  >80% of `LockExpiry`, log a warning — that's the canary that the job is about to overrun its
  lease and open a duplicate-execution window. Cheap early warning for the #1 misconfiguration.
- **Logging-level discipline** (`README.md:131-139`): per-iteration start/end is at `Debug`, not
  `Information`. A 500 ms job would otherwise emit 2 `Information` lines/sec/pod and flood logs.
  Lifecycle events (leader transitions, release) stay at `Information`.
- **The `Logger` field / CS9124 trap** (`:19`, troubleshooting `:69-111`): the base stores the
  logger in `protected ILogger Logger`. If a derived job uses a primary constructor and *also*
  references the `logger` parameter in a method, the compiler synthesizes a second backing field
  (CS9124). The fix — "treat the constructor param as write-only, use the inherited `Logger`" — is
  a documented consequence of the primary-constructor + base-class design.
- **`NodeId` resolution** (`ResolveNodeId`, `:171-178`): `Options.NodeId ?? POD_NAME ??
  MachineName`, plus an always-appended 8-char GUID suffix so two processes on one host (or two
  pods that somehow share a name) never collide on identity. The k8s downward API feeds `POD_NAME`,
  making leader logs instantly attributable to a pod.
- **Sealed `ExecuteAsync`** (`:194`): the orchestration (two loops, release-on-shutdown) is sealed
  so subclasses customize only `ExecuteJobLoopAsync`/`ExecuteJobAsync` and can't accidentally break
  the lifecycle.

---

## 18. A 60-second whiteboard summary

> Each replica runs a `BackgroundService` with two loops. The **election loop** runs one atomic
> Lua script every 3 s — acquire the Redis key if free, renew its 10 s TTL if I own it (value =
> my node id, which fences renew/release to the owner). First to acquire becomes leader; others
> poll. The **job loop** only runs work while `IsLeader`. `IsLeader` is `_isLeader` AND a
> **local lease** that lapses `LockExpiry` after the last confirmed renewal — so a node that loses
> Redis self-demotes even when it can't hear the bad news (timestamp taken *before* the call to
> stay stricter than the server). On graceful shutdown the leader atomically releases the key so
> failover is ~3 s instead of waiting 10 s. A leader never backs off on transient Redis errors
> (that would forfeit the lock); followers back off exponentially with jitter. Registration is
> **source-generated** (no reflection → NativeAOT/trim-safe), config is hand-bound and
> validated at host start, and everything timing-related goes through an injected `TimeProvider`
> so the whole thing is testable in virtual time.

It is a *best-effort* singleton, not a provably-safe distributed lock — the design shrinks the
duplicate-execution window (self-fencing + optional mid-run cancellation) and assumes idempotent
jobs, rather than claiming exclusivity it can't guarantee over a single Redis.
