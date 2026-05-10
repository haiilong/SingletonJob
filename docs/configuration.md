# Configuration

## Options

| Option | Type | Default | Notes |
|---|---|---|---|
| `ProjectName` | `string` | `"default"` | Lock-key prefix. Use a unique value per deployment so multiple projects sharing a Redis instance don't collide. |
| `HeartbeatInterval` | `TimeSpan` | `00:00:03` | How often to renew the lock. Must be `< LockExpiry`. |
| `LockExpiry` | `TimeSpan` | `00:00:10` | Redis TTL on the lock key. Recommend `>= 3 * HeartbeatInterval`. |
| `NodeId` | `string?` | `null` | Override identifier. When null, falls back to env `POD_NAME` then `Environment.MachineName`. An 8-char random suffix is always appended. |
| `MaxBackoffDelay` | `TimeSpan` | `00:00:30` | Ceiling on the exponential backoff delay applied between heartbeats when Redis throws. Must be `>= HeartbeatInterval`. |

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

Validation runs on `StartAsync`. Bad config throws `InvalidOperationException` and the host fails to start.

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
