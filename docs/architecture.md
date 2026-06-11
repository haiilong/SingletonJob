# Architecture

## Lock key

```
{ProjectName}:{JobName}:lock     ← Redis key
{NodeId}                         ← value held in the key
```

Each job class produces one lock key. Each replica generates one `NodeId` per process. `NodeId = (Options.NodeId ?? POD_NAME ?? MachineName) + "-" + Guid8`.

## Acquire or renew (Lua, atomic)

Every `HeartbeatInterval` each replica runs one Lua script that acquires the lock if it is free, or extends the TTL if this node already owns it:

```lua
local v = redis.call('GET', KEYS[1])
if not v then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
elseif v == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
```

`ARGV[1]` is the node id, `ARGV[2]` is `LockExpiry` in milliseconds. The first replica to run it against a free key wins and becomes leader; for the steady-state leader every heartbeat is a single round trip that renews the TTL. Followers get 0 back and stay followers.

If the script returns 0 for a node that thought it was leader, it drops `IsLeader`. It was preempted, presumably because too many renewals were missed and the lock expired before another node acquired it.

## Self-fencing on missed renewals

The renewal path above only detects preemption when Redis is reachable. To cover the partition case (only this node loses Redis connectivity, the key expires server-side, a peer takes over), the leader also fences itself locally. Every successful acquire/renew records a lease deadline: a timestamp taken before the Redis call plus `LockExpiry`. `IsLeader` returns false once that deadline passes without another successful renewal, so the job loop stops executing even while the election loop is still failing and backing off. Taking the timestamp before the call makes the local fence at least as strict as the server-side TTL.

## Release on graceful shutdown (Lua, atomic)

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
```

This is intentional. **Why release rather than letting the lock expire?**

Without explicit release, peers must wait up to `LockExpiry` (10 s default) for `SET NX` to succeed. With release, the next pod becomes leader within `HeartbeatInterval` (3 s default). On a Kubernetes rolling deploy, this turns "10 s of nobody running the job" into "3 s of nobody running the job".

On a hard kill (SIGKILL, OOM, network partition) release does not run; the lock just expires.

## Backoff on Redis errors

If `SETNX` or the renewal script throws, the heartbeat loop counts consecutive failures and increases the next delay:

```
delay = min(HeartbeatInterval × 2^failures, MaxBackoffDelay) ± 20% jitter
```

The jitter prevents a thundering herd of N replicas reconnecting in lockstep when Redis comes back. Reset to 0 on the first successful call.

A leader whose lease is still valid is exempt from the backoff and retries at the plain `HeartbeatInterval`. Backing off would forfeit the lock: with the recommended `LockExpiry >= 3 × HeartbeatInterval`, two doubled delays already exceed the TTL. Once the lease lapses the node self-demotes (see self-fencing above) and the follower backoff takes over.

## Concurrency model

- One `BackgroundService` per `SingletonBackgroundJob` subclass, registered as `IHostedService`.
- Inside each service, two parallel loops run: election (`RunLeaderElectionLoopAsync`) and job execution (`ExecuteJobLoopAsync`).
- `IsLeader` is a `bool` field with `Volatile.Read` / `Volatile.Write`. Single writer (election loop), N readers (job loop, release path). Eventually-consistent publication is acceptable here because losing leadership only delays a single iteration check by one tick of the job loop.

## Drop-on-overlap (`SingletonFixedRateJob`)

`PeriodicTimer.WaitForNextTickAsync` produces ticks at fixed wall-clock instants. A `_isJobRunning` `volatile bool` guards `ExecuteJobAsync`. When a tick arrives while a previous run is still in flight, the tick is dropped. This is the semantic Hangfire's recurring-job runner does not give you.

On shutdown, the loop awaits the most recent in-flight task before returning, so graceful shutdown is actually graceful.

## Diagram

```
                   ┌──────────────┐
                   │ IConnection  │  ← shared, owned by the host
                   │ Multiplexer  │
                   └──────┬───────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
   ┌──────────┐     ┌──────────┐     ┌──────────┐
   │ Replica  │     │ Replica  │     │ Replica  │
   │   A      │     │   B      │     │   C      │
   └────┬─────┘     └────┬─────┘     └────┬─────┘
        │                │                │
        ▼                ▼                ▼
   ┌──────────────────────────────────────────┐
   │  Redis :  myapp:heartbeat:lock = NodeA   │  ← only one wins SETNX
   └──────────────────────────────────────────┘
```
