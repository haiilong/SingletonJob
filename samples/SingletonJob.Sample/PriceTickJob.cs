using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Sample;

public sealed class PriceTickJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<PriceTickJob> logger)
    : SingletonFixedRateJob(redis, options, logger)
{
    public override string JobName => "price-tick";
    protected override TimeSpan GetJobInterval() => TimeSpan.FromMilliseconds(500);

    protected override async Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("[price-tick] polling prices at {Time:HH:mm:ss.fff}", DateTimeOffset.Now);
        await Task.Delay(50, cancellationToken);
    }
}
