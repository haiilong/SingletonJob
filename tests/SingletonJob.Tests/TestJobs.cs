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

internal sealed class ToggleableIntervalJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<ToggleableIntervalJob> logger,
    TimeSpan interval,
    string jobName)
    : SingletonIntervalJob(redis, options, logger)
{
    public int RunCount;
    private volatile bool _enabled = true;

    public bool Enabled
    {
        get => _enabled;
        set => _enabled = value;
    }

    public override string JobName { get; } = jobName;
    protected override TimeSpan GetJobInterval() => interval;

    protected override ValueTask<bool> IsJobEnabledAsync(CancellationToken cancellationToken) => new(_enabled);

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
    string jobName,
    TimeSpan workDuration = default)
    : SingletonCronJob(redis, options, logger)
{
    public int RunCount;
    public readonly List<DateTimeOffset> RunStarts = [];

    public override string JobName { get; } = jobName;
    protected override CronExpression GetCronExpression() => expr;

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        lock (RunStarts) RunStarts.Add(DateTimeOffset.UtcNow);
        Interlocked.Increment(ref RunCount);
        if (workDuration > TimeSpan.Zero)
            await Task.Delay(workDuration, cancellationToken);
    }
}
