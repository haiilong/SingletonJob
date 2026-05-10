using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Sample;

public sealed partial class PriceTickJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<PriceTickJob> logger)
    : SingletonFixedRateJob(redis, options, logger)
{
    public override string JobName => "price-tick";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(500);

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        LogPriceTickJob(Logger, DateTimeOffset.Now);
        await Task.Delay(50, cancellationToken);
    }

    [LoggerMessage(LogLevel.Information, "[price-tick] polling prices at {Time:HH:mm:ss.fff}")]
    static partial void LogPriceTickJob(ILogger logger, DateTimeOffset time);
}
