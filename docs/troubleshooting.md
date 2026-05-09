# Troubleshooting

## "Two instances are running my job at the same time"

Causes, in order of likelihood:

1. **Job is taking longer than `LockExpiry`.** Library logs a `Warning` once you exceed 80% of `LockExpiry`. Increase `LockExpiry` or shorten the job. The default 10 s tolerates an 8 s iteration; for slower jobs raise both `LockExpiry` and `HeartbeatInterval` proportionally (keep `LockExpiry >= 3 × HeartbeatInterval`).
2. **Different `ProjectName` per replica.** Lock keys differ → no contention → both win. Make sure every replica gets the same `SingletonJob:ProjectName`.
3. **Two different Redis instances.** Same idea: the `IConnectionMultiplexer` must point at the same backing store.

Run this against Redis to inspect the live lock:

```redis
KEYS myapp:*:lock
GET  myapp:heartbeat:lock
PTTL myapp:heartbeat:lock
```

## "Nobody is the leader"

Look for `Leader election error` lines. Most likely Redis is unreachable; the heartbeat loop will keep retrying with exponential backoff up to `MaxBackoffMultiplier × HeartbeatInterval`. Once Redis returns, leadership is reacquired automatically.

If logs are silent, your log level is filtering them out. `SingletonJob` events are at `Information`. Check your config.

## "Failover takes ~10 s on graceful shutdown"

The release Lua should run on `SIGTERM`. If it doesn't:

- Container runtime is sending `SIGKILL` not `SIGTERM`. Check `terminationGracePeriodSeconds` in k8s, or `--time` on `docker stop`.
- Your job is blocking shutdown for so long the host hits its own timeout before the release runs. Add a cancellation check inside `ExecuteJobAsync`.

If you don't care about graceful release (hard-kill environments), nothing breaks. Peers wait `LockExpiry` and one of them takes over.

## "Per-iteration logs are missing"

They're at `Debug`. Raise the level for `SingletonJob`:

```json
"Logging": { "LogLevel": { "SingletonJob": "Debug" } }
```

## "I see `IL2026` / `IL3050` warnings under `PublishAot`"

You called `AddSingletonJobs` instead of `AddSingletonJobsGenerated`. The reflection version is annotated as not AOT-safe. Switch to the source-generated registration.

## "The source generator did not run on my project"

If you reference SingletonJob via NuGet, the generator should run automatically (it's in the package's `analyzers/dotnet/cs` folder). If you reference via `<ProjectReference>`, analyzers do not flow. See [aot.md](aot.md) for the explicit project reference incantation.

To inspect generator output:

```sh
dotnet build -p:EmitCompilerGeneratedFiles=true -p:CompilerGeneratedFilesOutputPath=Generated
```

Look in `Generated/SingletonJob.SourceGenerator/.../SingletonJobGeneratedRegistration.g.cs`.

## "I want to override `LockExpiry` for one job only"

```csharp
services.PostConfigureSingletonJob("heavy-job", o =>
{
    o.LockExpiry = TimeSpan.FromMinutes(5);
});
```

The base configuration from `appsettings.json` is applied first; this delegate runs after.
