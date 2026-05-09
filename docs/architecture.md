# Architecture

## Lock key

```
{ProjectName}:{JobName}:lock     ← Redis key
{NodeId}                         ← value held in the key
```

Each job class produces one lock key. Each replica generates one `NodeId` per process. `NodeId = (Options.NodeId ?? POD_NAME ?? MachineName) + "-" + Guid8`.

## Acquisition (`SETNX`)

Every `HeartbeatInterval` each replica issues:

```redis
SET {lockKey} {nodeId} NX PX {LockExpiry}
```

The first replica to issue this wins and becomes leader. The others get `null` back and stay followers.

## Renewal (Lua, atomic)

Once leader, the same loop renews the TTL using a Lua script that only succeeds if the lock value still matches our `NodeId`:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
    return 0
end
```

If the script returns 0 the leader drops `IsLeader`. It was preempted, presumably because too many renewals were missed and the lock expired before another node acquired it.

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
delay = HeartbeatInterval × min(2^failures, MaxBackoffMultiplier) ± 20% jitter
```

The jitter prevents a thundering herd of N replicas reconnecting in lockstep when Redis comes back. Reset to 0 on the first successful call.

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
