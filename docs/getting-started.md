# Getting started

## Install

```sh
dotnet add package SingletonJob
```

`net8.0` and `net10.0`. Pulls in `StackExchange.Redis` and `Cronos`.

## Minimal worker

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SingletonJob;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(builder.Configuration.GetConnectionString("Redis")!));

builder.Services.AddSingletonJobs(builder.Configuration);

await builder.Build().RunAsync();
```

`AddSingletonJobs` is emitted at compile time by the bundled source generator. There is no reflection in the registration path, so the library is fully trimming- and NativeAOT-safe.

## Three job shapes

### Interval (run, then wait)

```csharp
public sealed class HeartbeatJob(IConnectionMultiplexer r, IOptionsFactory<SingletonJobOptions> o, ILogger<HeartbeatJob> l)
    : SingletonIntervalJob(r, o, l)
{
    public override string JobName => "heartbeat";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromSeconds(1);
    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}
```

### Fixed-rate (fire on tick, drop overlapping ticks)

```csharp
public sealed class PriceTickJob(IConnectionMultiplexer r, IOptionsFactory<SingletonJobOptions> o, ILogger<PriceTickJob> l)
    : SingletonFixedRateJob(r, o, l)
{
    public override string JobName => "price-tick";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(500);
    protected override Task ExecuteJobAsync(CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}
```

If the previous run is still in flight when a tick arrives, that tick is dropped. No queueing, no overlap.

### Cron (wall-clock schedule)

```csharp
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

## Run multiple instances

```sh
cd samples
docker compose up --build --scale worker=3
```

Exactly one prints `became LEADER`. Others sit idle. Kill the leader with `docker kill <id>` and another takes over.

For Windows, see `samples/run-3-instances.ps1`.
