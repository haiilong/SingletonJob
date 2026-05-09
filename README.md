# SingletonJob

Lightweight Redis-backed singleton background jobs for multi-instance .NET deployments. High-frequency, drop-on-overlap, no persistence overhead. A focused alternative to [Hangfire](https://www.hangfire.io/) for the case where you just want **exactly one pod** to run a global periodic job.

[![NuGet](https://img.shields.io/nuget/v/SingletonJob.svg)](https://www.nuget.org/packages/SingletonJob/)
[![Build](https://github.com/haiilong/SingletonJob/actions/workflows/ci.yml/badge.svg)](https://github.com/haiilong/SingletonJob/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Why this exists

Hangfire is great for durable, retryable, observable background work. It is a poor fit when:

- The job is **high frequency** (every second, every 500 ms). Hangfire's job storage becomes a bottleneck. In fact Hangfire does not support jobs faster than 1 second because of limitation of cron.
- You don't want **persistence**, retries, or a dashboard.
- You need **drop-on-overlap** semantics. If the previous tick is still running, skip the next one.
- You need **periodic** scheduling, not just cron; and you want more control of how jobs run.
- You need **exactly-one-instance** execution across pods, and round-robin distribution is really not important. Distributing computation is an entire different problem.
- You need the library to be AOT compatible, which as far as I know, Hangfire is not.

|                                 | SingletonJob                  | Hangfire                               |
|---------------------------------|-------------------------------|----------------------------------------|
| Storage                         | Redis lock key (~50 B)        | SQL/Redis with state, history, retries |
| High-frequency (≤1 s) jobs      | first-class                   | discouraged                            |
| Drop-on-overlap                 | yes (`SingletonFixedRateJob`) | no, overlapping runs queue up          |
| Run-then-wait periodic          | yes (`SingletonIntervalJob`)  | no, only cron                          |
| Cron schedules                  | yes (`SingletonCronJob`)      | yes                                    |
| Single-instance leader election | yes                           | no, round-robin                        |
| Dashboard, retries, history     | no                            | yes                                    |
| Dependencies                    | StackExchange.Redis, Cronos   | many                                   |
| AOT compatibility               | yes                           | no                                     |

## Install

```sh
dotnet add package SingletonJob
```

Targets `net8.0` and `net10.0` (If you use `net9.0` then it's the same as `net8.0`).

## Quickstart

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SingletonJob;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis")!));

// Source-generated, AOT-safe: registers every SingletonBackgroundJob subclass at compile time.
builder.Services.AddSingletonJobsGenerated(builder.Configuration);

await builder.Build().RunAsync();
```

> The bundled Roslyn source generator emits `AddSingletonJobsGenerated` for your project. A reflection-based `AddSingletonJobs` exists too, but is annotated `[RequiresUnreferencedCode]`/`[RequiresDynamicCode]`. The source-generated path is the recommended one for trimming and NativeAOT.

`appsettings.json`:

```json
{
  "ConnectionStrings": { "Redis": "localhost:6379" },
  "SingletonJob": {
    "ProjectName": "myapp",
    "HeartbeatInterval": "00:00:03",
    "LockExpiry": "00:00:10"
  }
}
```

### Three job shapes

```csharp
// 1) Run, wait, run. "At least N seconds between runs."
public sealed class HeartbeatJob(IConnectionMultiplexer r, IOptionsFactory<SingletonJobOptions> o, ILogger<HeartbeatJob> l)
    : SingletonIntervalJob(r, o, l)
{
    public override string JobName => "heartbeat";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromSeconds(1);
    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}


// 2) Fire on a fixed rate. Drop the tick if the previous run is still in flight.
public sealed class PriceTickJob(IConnectionMultiplexer r, IOptionsFactory<SingletonJobOptions> o, ILogger<PriceTickJob> l)
    : SingletonFixedRateJob(r, o, l)
{
    public override string JobName => "price-tick";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(500);
    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}

// 3) Cron schedule.
public sealed class DailyReportJob(IConnectionMultiplexer r, IOptionsFactory<SingletonJobOptions> o, ILogger<DailyReportJob> l)
    : SingletonCronJob(r, o, l)
{
    private static readonly CronExpression Expr = CronExpression.Parse("0 3 * * *");
    public override string JobName => "daily-report";
    protected override CronExpression GetCronExpression() => Expr;
    // optional: protected override TimeZoneInfo TimeZone => TimeZoneInfo.FindSystemTimeZoneById("Asia/Singapore");
    // or if you prefer local time: protected override TimeZoneInfo TimeZone => TimeZoneInfo.Local;
    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}
```

That's it. Deploy N replicas. Exactly one runs the job.

## How it works

1. Each replica derives `_lockKey = "{ProjectName}:{JobName}:lock"` and a unique `_nodeId`.
2. Every `HeartbeatInterval` (default 3 s), each replica issues a Redis `SET key value NX PX <LockExpiry>`. The first one wins and becomes leader.
3. The leader renews the TTL with an atomic Lua script (`GET == nodeId ? PEXPIRE : 0`) so only the holder can extend it.
4. The job loop checks `IsLeader` each iteration and only runs work if true.
5. On graceful shutdown the leader runs an atomic release Lua script (`GET == nodeId ? DEL : 0`). This enables **fast failover**: the next replica acquires the lock within `HeartbeatInterval` instead of waiting `LockExpiry` for it to expire.
6. On hard kill (SIGKILL, OOM), the lock simply expires after `LockExpiry`.

```
HeartbeatInterval  ──▶ how often to renew         (default 3s)
LockExpiry         ──▶ TTL on the Redis key       (default 10s)
```

`HeartbeatInterval` must be strictly less than `LockExpiry`. Recommend `LockExpiry >= 3 * HeartbeatInterval` so a single dropped network call doesn't cost leadership.

## Logging levels

| Event                                     | Level       |
|-------------------------------------------|-------------|
| Service start, leader transitions, release| Information |
| Per-iteration start/end + duration         | Debug       |
| Iteration close to `LockExpiry` (≥80%)     | Warning     |
| Job exception                              | Error       |

Per-iteration noise is at Debug on purpose. High-frequency jobs would otherwise flood Information logs.

## Configuration

| Option                 | Default     | Description                                                              |
|------------------------|-------------|--------------------------------------------------------------------------|
| `ProjectName`          | `default`   | Lock key prefix. Pick a unique value per deployment.                     |
| `HeartbeatInterval`    | `00:00:03`  | How often to attempt acquire/renew.                                      |
| `LockExpiry`           | `00:00:10`  | TTL applied to the Redis lock key.                                       |
| `NodeId`               | `null`      | Override identifier. Falls back to env `POD_NAME`, then `MachineName`.   |
| `MaxBackoffMultiplier` | `8`         | Exponential backoff cap on consecutive Redis errors.                     |

Validation runs on `StartAsync`; bad config throws. See [docs/configuration.md](docs/configuration.md) for per-job overrides.

## Documentation

| | |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Install + first three jobs |
| [docs/configuration.md](docs/configuration.md) | Every option, per-job overrides |
| [docs/architecture.md](docs/architecture.md) | How leader election works end-to-end |
| [docs/aot.md](docs/aot.md) | NativeAOT + trimming, source generator details |
| [docs/deployment-kubernetes.md](docs/deployment-kubernetes.md) | Pod manifest, SIGTERM, sizing |
| [docs/deployment-redis.md](docs/deployment-redis.md) | Standalone, Sentinel, Cluster, Memurai |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Common pitfalls and how to debug them |

## Try it locally

See [`samples/`](samples/): a worker template with all three job types, a `docker-compose.yml` that spins up one Redis and three workers, and a `run-3-instances.ps1` for Windows local dev.

```sh
cd samples
docker compose up --build --scale worker=3
```

## Roadmap

- Built-in `IHealthCheck` so Kubernetes readiness probes can detect a wedged election loop.
- Metrics via `System.Diagnostics.Metrics` (counters for ticks, dropped ticks, leadership flips, durations).
- `ActivitySource` tracing per iteration for distributed tracing.
- Configurable cancellation on lost leadership (today: started iterations always run to completion).
- SQL Server / PostgreSQL backends. (Not on near roadmap. Redis remains the supported backend.)

## License

MIT
