using Cronos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Tests;

internal sealed class CountingIntervalJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<CountingIntervalJob> logger,
    TimeSpan interval,
    string jobName)
    : SingletonIntervalJob(redis, options, logger)
{
    public int RunCount;

    public override string JobName { get; } = jobName;
    protected override TimeSpan GetJobInterval() => interval;

    protected override Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref RunCount);
        return Task.CompletedTask;
    }
}

internal sealed class CountingFixedRateJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<CountingFixedRateJob> logger,
    TimeSpan interval,
    TimeSpan workDuration,
    string jobName)
    : SingletonFixedRateJob(redis, options, logger)
{
    public int RunCount;

    public override string JobName { get; } = jobName;
    protected override TimeSpan GetJobInterval() => interval;

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref RunCount);
        if (workDuration > TimeSpan.Zero)
            await Task.Delay(workDuration, cancellationToken);
    }
}

internal sealed class CountingCronJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<CountingCronJob> logger,
    CronExpression expr,
    string jobName)
    : SingletonCronJob(redis, options, logger)
{
    public int RunCount;

    public override string JobName { get; } = jobName;
    protected override CronExpression GetCronExpression() => expr;

    protected override Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref RunCount);
        return Task.CompletedTask;
    }
}
