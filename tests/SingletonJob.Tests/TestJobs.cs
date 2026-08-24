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

// Blocks inside ExecuteJobAsync until its token fires; used to observe CancelOnLostLeadership behavior.
internal sealed class BlockingIntervalJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<BlockingIntervalJob> logger,
    string jobName)
    : SingletonIntervalJob(redis, options, logger)
{
    public int StartedCount;
    public volatile bool IterationCancelled;
    private volatile bool _enabled = true;

    public bool Enabled
    {
        get => _enabled;
        set => _enabled = value;
    }

    public override string JobName { get; } = jobName;
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(100);

    protected override ValueTask<bool> IsJobEnabledAsync(CancellationToken cancellationToken) => new(_enabled);

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref StartedCount);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
        }
        catch (OperationCanceledException)
        {
            IterationCancelled = true;
            throw;
        }
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
    TimeSpan workDuration = default,
    TimeProvider? timeProvider = null,
    CronMisfirePolicy misfirePolicy = CronMisfirePolicy.Skip)
    : SingletonCronJob(redis, options, logger, timeProvider ?? TimeProvider.System)
{
    public int RunCount;
    public readonly List<DateTimeOffset> RunStarts = [];

    public override string JobName { get; } = jobName;
    protected override CronExpression GetCronExpression() => expr;
    protected override CronMisfirePolicy MisfirePolicy => misfirePolicy;

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        lock (RunStarts) RunStarts.Add(DateTimeOffset.UtcNow);
        Interlocked.Increment(ref RunCount);
        if (workDuration > TimeSpan.Zero)
            await Task.Delay(workDuration, cancellationToken);
    }
}

internal sealed class SlowIntervalJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<SlowIntervalJob> logger,
    TimeSpan workDuration,
    bool warnOnLongExecution,
    string jobName)
    : SingletonIntervalJob(redis, options, logger)
{
    public int RunCount;

    public override string JobName { get; } = jobName;

    protected override bool WarnOnLongExecution => warnOnLongExecution;

    // Long enough that only the first iteration runs inside a test.
    protected override TimeSpan GetJobInterval() => TimeSpan.FromHours(1);

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref RunCount);
        await Task.Delay(workDuration, cancellationToken);
    }
}
