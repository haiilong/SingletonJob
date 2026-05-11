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

Look for `Leader election error` lines. Most likely Redis is unreachable; the heartbeat loop will keep retrying with exponential backoff capped at `MaxBackoffDelay` (default 30 s). Once Redis returns, leadership is reacquired automatically.

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

## "`AddSingletonJobs` is not recognized" / "CS1061 ... no definition for `AddSingletonJobs`"

The source generator only runs as part of a build, so on a fresh checkout the symbol does not exist yet and the IDE will red-squiggle the call. **Run `dotnet build` once.** The symbol resolves and IntelliSense works from then on.

If the error persists after a clean build, jump to the next section.

## "The source generator did not run on my project"

If you reference SingletonJob via NuGet, the generator should run automatically (it's in the package's `analyzers/dotnet/cs` folder). If you reference via `<ProjectReference>`, analyzers do not flow. See [aot.md](aot.md) for the explicit project reference incantation.

To inspect generator output:

```sh
dotnet build -p:EmitCompilerGeneratedFiles=true -p:CompilerGeneratedFilesOutputPath=Generated
```

Look in `Generated/SingletonJob.SourceGenerator/.../SingletonJobGeneratedRegistration.g.cs`. If the file exists but `AddSingletonJobs` is missing or empty, no concrete subclass of `SingletonBackgroundJob` was found in the compilation.

## "CS9124: parameter 'logger' is captured into the state of the enclosing type"

You're using a primary constructor on your job and referencing the `logger` parameter inside a method body:

```csharp
public sealed class MyJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<MyJob> logger)
    : SingletonIntervalJob(redis, options, logger)
{
    protected override Task ExecuteJobAsync(CancellationToken ct)
    {
        logger.LogInformation("..."); // CS9124
        return Task.CompletedTask;
    }
}
```

The base class `SingletonBackgroundJob` already stores the logger in a `protected ILogger Logger` field. Referencing the primary-constructor `logger` after forwarding it to `base(...)` makes the compiler synthesize a *second* backing field on your type for the same value. Switch to the inherited `Logger`:

```csharp
protected override Task ExecuteJobAsync(CancellationToken ct)
{
    Logger.LogInformation("...");
    return Task.CompletedTask;
}
```

Same applies to `[LoggerMessage]` source-generated logging: pass `Logger` to the generated method, not the constructor parameter:

```csharp
protected override Task ExecuteJobAsync(CancellationToken ct)
{
    LogTick(Logger, DateTimeOffset.Now);
    return Task.CompletedTask;
}

[LoggerMessage(LogLevel.Information, "tick at {Time:HH:mm:ss.fff}")]
static partial void LogTick(ILogger logger, DateTimeOffset time);
```

Treat the `logger` constructor parameter as write-only: forward it to `base(...)` and never touch it again.

## "InvalidOperationException: SingletonJobOptions.ProjectName must be set"

You called `services.AddSingletonJobs(...)` without supplying a `ProjectName`. The library deliberately ships **no default** for this value: a shared default would let two unrelated deployments sharing the same Redis instance silently collide on lock keys like `default:heartbeat:lock`.

Set it in any of:

```json
// appsettings.json
{
  "SingletonJob": {
    "ProjectName": "myapp"
  }
}
```

```sh
# environment variable
SingletonJob__ProjectName=myapp
```

```csharp
// programmatic
services.PostConfigure<SingletonJobOptions>(o => o.ProjectName = "myapp");
```

The check runs at host startup so a missing value fails fast rather than at the first job tick.

## "I want to override `LockExpiry` for one job only"

```csharp
services.PostConfigureSingletonJob("heavy-job", o =>
{
    o.LockExpiry = TimeSpan.FromMinutes(5);
});
```

The base configuration from `appsettings.json` is applied first; this delegate runs after.
