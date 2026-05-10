using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Sample;

public sealed partial class HeartbeatJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<HeartbeatJob> logger)
    : SingletonIntervalJob(redis, options, logger)
{
    public override string JobName => "heartbeat";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromSeconds(1);

    protected override Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        LogHeartbeatJob(Logger, DateTimeOffset.Now);
        return Task.CompletedTask;
    }

    [LoggerMessage(LogLevel.Information, "[heartbeat] tick at {Time:HH:mm:ss.fff}")]
    static partial void LogHeartbeatJob(ILogger logger, DateTimeOffset time);
}
