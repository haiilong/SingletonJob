# Configuration

## Options

| Option | Type | Default | Notes |
|---|---|---|---|
| `ProjectName` | `string` | _(required)_ | Lock-key prefix. Must be set explicitly. A shared default would silently let two unrelated deployments collide on the same Redis instance. The host throws `InvalidOperationException` at startup if this is empty. |
| `HeartbeatInterval` | `TimeSpan` | `00:00:03` | How often to renew the lock. Must be `< LockExpiry`. |
| `LockExpiry` | `TimeSpan` | `00:00:10` | Redis TTL on the lock key. Recommend `>= 3 * HeartbeatInterval`. |
| `NodeId` | `string?` | `null` | Override identifier. When null, falls back to env `POD_NAME` then `Environment.MachineName`. An 8-char random suffix is always appended. |
| `MaxBackoffDelay` | `TimeSpan` | `00:00:30` | Ceiling on the exponential backoff delay applied between heartbeats when Redis throws. Must be `>= HeartbeatInterval`. |
| `Enabled` | `bool` | `true` | Static kill switch, evaluated once at startup. When `false` the job never executes and never participates in leader election. For live toggling see [Disabling jobs](#disabling-jobs). |

`appsettings.json`:

```json
{
  "ConnectionStrings": { "Redis": "localhost:6379" },
  "SingletonJob": {
    "ProjectName": "myapp",
    "HeartbeatInterval": "00:00:03",
    "LockExpiry": "00:00:10",
    "MaxBackoffDelay": "00:00:30"
  }
}
```

Validation is wired through `IValidateOptions<SingletonJobOptions>`. A tiny hosted service resolves `IOptions<SingletonJobOptions>.Value` at host start, so bad base config throws `OptionsValidationException` before any job iteration runs. Per-job overrides are validated when each job calls `IOptionsFactory.Create(JobName)` during its `StartAsync`; the error message is prefixed with `[Job: name]` so you can see which override is at fault.

## Per-job overrides

Each job receives its options via `IOptionsFactory<SingletonJobOptions>.Create(JobName)` once at `StartAsync`. The value is **frozen at startup**. No live reload. Redeploy to pick up config changes. Defaults from `appsettings.json` apply to every job. Override a single job with `PostConfigureSingletonJob`:

```csharp
builder.Services.PostConfigureSingletonJob("daily-report", o =>
{
    o.LockExpiry = TimeSpan.FromMinutes(5);
});
```

Order of application:
1. Defaults from `new SingletonJobOptions()`.
2. `Configure<SingletonJobOptions>(section)`: bound from `SingletonJob` config section.
3. `ConfigureAll<SingletonJobOptions>(...)`: same section bound to all named instances.
4. `PostConfigureSingletonJob("name", ...)`: your per-job override.

So you only need to specify the values that change.

## Disabling jobs

Two mechanisms, layered. The static one wins.

### Static: `Options.Enabled` (evaluated once at startup)

Project level, disabling every job in the deployment:

```json
{
  "SingletonJob": { "Enabled": false }
}
```

Job level:

```csharp
builder.Services.PostConfigureSingletonJob("price-tick", o => o.Enabled = false);
```

A statically disabled job logs one `Information` line at startup and then idles: no Redis traffic, no election, no execution. Because options are frozen at `StartAsync`, changing this requires a redeploy.

### Live: override `IsJobEnabledAsync` (re-evaluated every heartbeat)

For runtime toggling (feature flags, ops kill switches, A/B canaries), inject your flag service into the job and override `IsJobEnabledAsync`:

```csharp
public sealed class PriceTickJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<PriceTickJob> logger,
    IFeatureFlags flags)                                          // any DI service you like
    : SingletonFixedRateJob(redis, options, logger)
{
    public override string JobName => "price-tick";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(500);

    protected override async ValueTask<bool> IsJobEnabledAsync(CancellationToken ct)
        => await flags.IsEnabledAsync("jobs-enabled", ct)         // project-level flag
        && await flags.IsEnabledAsync($"job-{JobName}", ct);      // per-job flag

    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}
```

Semantics:

- The election loop calls `IsJobEnabledAsync` once per `HeartbeatInterval` (default 3 s), so a flag flip takes effect within one heartbeat. The job loops also check the cached `IsEnabled` before each iteration, so no extra load is put on your flag backend by high-frequency jobs.
- **While disabled, the node releases the leadership lock and stops competing for it.** This matters when the flag evaluates differently per node (canary rollouts): a disabled leader would otherwise hold the lock and starve enabled replicas. The handover shows up in logs as `disabled while leader. Releasing ...`.
- An iteration already in flight when the flag flips is **not cancelled**; it runs to completion (same rule as lost leadership).
- If your override throws, the error is logged at `Warning` and the previous state is kept, so a flaky flag backend does not flap leadership.
- `Options.Enabled = false` short-circuits everything: `IsJobEnabledAsync` is never called.

If you want the same flag logic on every job, put the override in an intermediate base class per shape and derive your jobs from that.

## Kubernetes / environment

`POD_NAME` is read automatically when `Options.NodeId` is null. With the standard k8s downward API:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

Logs become `Node my-app-7d4-x8k9 became LEADER for myapp:heartbeat:lock`. Instantly attributable to a single pod.

## Logging levels

| Event | Level |
|---|---|
| Service start, leader transitions, release | `Information` |
| Per-iteration start/end + duration | `Debug` |
| Iteration close to `LockExpiry` (≥80%) | `Warning` |
| Lost leadership | `Warning` |
| Redis / job exception | `Error` |

To silence per-iteration noise (default), keep `Information`. To trace tick timing during incidents, raise `SingletonJob` to `Debug`:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "SingletonJob": "Debug"
    }
  }
}
```
