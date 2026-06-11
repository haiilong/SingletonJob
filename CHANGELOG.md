# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Invalid `GetJobInterval()` values now fail with a clear error, and a dead job loop no longer keeps the lock alive.** A zero or negative interval used to surface as a bare `ArgumentOutOfRangeException` from deep inside `Task.Delay` or `PeriodicTimer`; it now throws `InvalidOperationException` naming the job. In addition, when the job loop exits for any non-shutdown reason (invalid configuration, an escaping exception, a cron schedule with no future occurrences), the election loop is now stopped and the lock released immediately. Previously the failure stayed invisible until host shutdown while the node kept renewing a lock for a job that no longer ran, starving healthy replicas.
- **`SingletonCronJob` no longer crashes on schedules more than ~49.7 days away.** `Task.Delay` rejects delays above `uint.MaxValue - 1` milliseconds, so a yearly cron threw `ArgumentOutOfRangeException` and killed the job loop. Long waits are now slept in one-day chunks with the remaining time recomputed after each chunk, which also keeps the wake-up accurate across system clock adjustments.
- **`SingletonCronJob` no longer replays missed occurrences back-to-back.** When an execution ran longer than the cron period, every missed occurrence used to fire immediately after the previous run finished (an every-minute job that once took 10 minutes would then run 10 times in a row). Missed occurrences are now skipped and the job resumes at the next future occurrence, matching the drop semantics of the rest of the library.
- **A leader with a still-valid lease no longer backs off exponentially on Redis errors.** With the recommended settings two doubled delays already exceed `LockExpiry`, so any two consecutive hiccups used to forfeit the lock. The leader now retries at the plain `HeartbeatInterval` while its lease is valid; exponential backoff with jitter still applies to followers and to a demoted ex-leader.
- **Split-brain on Redis partition: a leader that cannot reach Redis now demotes itself once `LockExpiry` elapses without a successful renewal.** Previously `IsLeader` stayed true while heartbeats failed, so a node partitioned from Redis kept executing its job while a peer acquired the expired lock and ran it too. The lease deadline is computed from a timestamp taken before each acquire/renew call, making the local fence at least as strict as the server-side TTL.

### Added

- **Job enable/disable.**
  - `SingletonJobOptions.Enabled` (default `true`): a static kill switch, evaluated once at startup. Set `"SingletonJob": { "Enabled": false }` to disable every job in the project, or `PostConfigureSingletonJob("name", o => o.Enabled = false)` for one job. A statically disabled job starts, logs one line, and idles: no election, no Redis traffic.
  - `protected virtual ValueTask<bool> IsJobEnabledAsync(CancellationToken)` on `SingletonBackgroundJob`: a live toggle, re-evaluated once per `HeartbeatInterval`. Override it to bridge to a DI-injected feature-flag service; flips take effect within one heartbeat without redeploy. While disabled the node releases and stops competing for the leadership lock so an enabled replica can take over (relevant for per-node/canary flags). Exceptions from the override are logged and the previous state is kept.
  - `protected bool IsEnabled`: the last observed enabled state, checked by all three job loops before each iteration.

## [1.0.0] - 2026-05-11

First stable release. The public API is now considered stable and follows semver; breaking changes will only ship with a major version bump.

### Overview

`SingletonJob` is a Redis-backed library for running high-frequency background jobs where **exactly one replica** across an N-node deployment executes the work. It is a focused alternative to Hangfire for the case where you want periodic execution, leader election, and graceful failover, but do not need persistence, retries, dashboards, or distributed work fan-out.

Targets `net8.0` and `net10.0`. `net9.0` consumers resolve to the `net8.0` TFM.

### Features

#### Three job shapes

- **`SingletonIntervalJob`** — run, then wait `GetJobInterval()`, then run again. Wait time is measured from the end of the previous iteration. Use when "at least N seconds between runs" semantics are wanted and slow iterations naturally back off.
- **`SingletonFixedRateJob`** — fire on a fixed-rate `PeriodicTimer` tick. If a previous run is still in flight when a tick arrives, the tick is **dropped** (no queueing, no overlap). On shutdown, the loop awaits the most recent in-flight run so termination remains graceful.
- **`SingletonCronJob`** — wall-clock schedule via [Cronos](https://github.com/HangfireIO/Cronos). Override `TimeZone` to evaluate the expression in something other than UTC. Supports second-precision expressions and is hardened against `Cronos` returning a non-advancing occurrence (no busy-spin).

All three derive from `SingletonBackgroundJob`, which owns the leader-election loop and exposes:

- `JobName` (abstract) — used as the lock-key suffix and the named-options key.
- `ExecuteJobAsync(CancellationToken)` (abstract) — your work.
- `protected ILogger Logger` — log via this from derived classes (avoids CS9124 with primary-constructor jobs).
- `protected bool IsLeader { get; }` — checked by each shape's loop before invoking work.

#### Leader election and failover

- Lock key format: `{ProjectName}:{JobName}:lock`, value = `{NodeId}-{Guid8}`.
- Acquisition via `SET key value NX PX <LockExpiry>`. The first replica wins.
- Renewal via an atomic Lua script: `GET == nodeId ? PEXPIRE : 0`. Only the holder can extend the TTL.
- **Atomic release on graceful shutdown** via Lua: `GET == nodeId ? DEL : 0`. This enables **fast failover** — the next replica acquires the lock within `HeartbeatInterval` instead of waiting `LockExpiry` for it to expire. On hard kill (SIGKILL, OOM, partition), the lock simply expires after `LockExpiry`.
- Exponential backoff with ±20% jitter on Redis errors, capped at `MaxBackoffDelay`. Prevents thundering-herd reconnects when Redis recovers.
- `Volatile.Read`/`Volatile.Write` on the `IsLeader` flag; single writer (election loop), N readers (job loop).

#### Configuration

`SingletonJobOptions` exposes:

| Option              | Default    | Notes                                                                |
|---------------------|------------|----------------------------------------------------------------------|
| `ProjectName`       | `default`  | Lock-key prefix. Unique per deployment.                              |
| `HeartbeatInterval` | `00:00:03` | How often to renew. Must be `<` `LockExpiry`.                         |
| `LockExpiry`        | `00:00:10` | Redis TTL on the lock key. Recommend `>= 3 × HeartbeatInterval`.     |
| `NodeId`            | `null`     | Override. Falls back to env `POD_NAME`, then `Environment.MachineName`. An 8-char random suffix is always appended. |
| `MaxBackoffDelay`   | `00:00:30` | Ceiling on exponential backoff between Redis error retries.          |

Validation runs in `StartAsync` and throws `InvalidOperationException` on bad config so the host fails fast.

#### Per-job overrides

`PostConfigureSingletonJob("job-name", o => ...)` runs after the base configuration. Each job receives its frozen options via `IOptionsFactory<SingletonJobOptions>.Create(JobName)` once at startup. No live reload — redeploy to change settings.

#### Source-generated DI registration

A bundled Roslyn source generator (`SingletonJob.SourceGenerator`, shipped in the NuGet package's `analyzers/dotnet/cs` folder) scans your compilation for every non-abstract `SingletonBackgroundJob` subclass and emits an `internal` `services.AddSingletonJobs(IConfiguration?)` extension method into your assembly.

- **No reflection** in the registration path. Fully trimming- and NativeAOT-safe (`IsTrimmable=true`, `IsAotCompatible=true`).
- Configuration binding is manual (`section["Key"]` + `TimeSpan.TryParse` / `int.TryParse`), not `ConfigurationBinder.Bind`.
- Emitted as `internal` so each consuming assembly gets its own copy with no cross-project collision.

#### Logging

Predictable, low-noise structured logging:

| Event                                       | Level         |
|---------------------------------------------|---------------|
| Service start, leader transitions, release  | `Information` |
| Per-iteration start/end + duration          | `Debug`       |
| Iteration ≥80% of `LockExpiry`              | `Warning`     |
| Lost leadership                             | `Warning`     |
| Redis / job exception                       | `Error`       |

Per-iteration noise is at `Debug` on purpose — high-frequency jobs would otherwise flood `Information`.

### Documentation

- [README.md](README.md) — elevator pitch and 30-line quickstart.
- [docs/getting-started.md](docs/getting-started.md) — install plus the first three jobs.
- [docs/configuration.md](docs/configuration.md) — every option and per-job overrides.
- [docs/architecture.md](docs/architecture.md) — leader election end-to-end, with Lua scripts.
- [docs/aot.md](docs/aot.md) — NativeAOT, trimming, source-generator details.
- [docs/deployment-kubernetes.md](docs/deployment-kubernetes.md) — pod manifest, SIGTERM, sizing.
- [docs/deployment-redis.md](docs/deployment-redis.md) — standalone, Sentinel, Cluster, Memurai.
- [docs/troubleshooting.md](docs/troubleshooting.md) — common pitfalls, CS9124 explained, log lines decoded.

### Samples

`samples/SingletonJob.Sample` includes one job of each shape, a `docker-compose.yml` that spins up Redis plus three workers, and `run-3-instances.ps1` for Windows local dev.

```sh
cd samples
docker compose up --build --scale worker=3
```

Exactly one replica logs `became LEADER`. Kill it and another takes over within `HeartbeatInterval`.

### Dependencies

- [`StackExchange.Redis`](https://www.nuget.org/packages/StackExchange.Redis) `2.8.16`
- [`Cronos`](https://www.nuget.org/packages/Cronos) `0.8.4`
- `Microsoft.Extensions.Hosting.Abstractions`, `Microsoft.Extensions.Options.ConfigurationExtensions`, `Microsoft.Extensions.Logging.Abstractions`, `Microsoft.Extensions.DependencyInjection.Abstractions` — all on the `8.0.x` line.

### Known limitations

These are documented constraints, not bugs. They may be revisited in future versions.

- **No persistence, retries, or dashboard.** If a job tick is dropped (overlap) or its replica dies mid-execution, it is not retried. Use Hangfire if you need durable work.
- **No automatic cancellation on lost leadership.** An iteration that started while leader will run to completion even if the lock is preempted partway through. Honor `CancellationToken` in your own work if you want tighter behavior.
- **No live config reload.** Options are frozen in `StartAsync` per job. Redeploy to change them.
- **Redis-only backend.** SQL Server / PostgreSQL backends are not on the near-term roadmap.

### Roadmap (not in this release)

- Built-in `IHealthCheck` so Kubernetes readiness probes can detect a wedged election loop.
- Metrics via `System.Diagnostics.Metrics` (counters for ticks, dropped ticks, leadership flips, durations).
- `ActivitySource` tracing per iteration.
- Configurable cancellation on lost leadership.

## [0.1.0] - earlier

Initial preview. Same public surface as `1.0.0`, marked unstable. See the git history for incremental changes leading up to this stable release.

[1.0.0]: https://github.com/haiilong/SingletonJob/releases/tag/v1.0.0
[0.1.0]: https://github.com/haiilong/SingletonJob/releases/tag/v0.1.0
