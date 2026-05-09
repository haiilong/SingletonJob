using Cronos;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Sample;

public sealed class DailyReportJob(
    IConnectionMultiplexer redis,
    IOptionsFactory<SingletonJobOptions> options,
    ILogger<DailyReportJob> logger)
    : SingletonCronJob(redis, options, logger)
{
    private static readonly CronExpression Expr = CronExpression.Parse("0 3 * * *");

    public override string JobName => "daily-report";
    protected override CronExpression GetCronExpression() => Expr;
    protected override TimeZoneInfo TimeZone => TimeZoneInfo.Utc;

    protected override Task ExecuteJobAsync(CancellationToken cancellationToken)
    {
        Logger.LogInformation("[daily-report] generating at {Time:O}", DateTimeOffset.UtcNow);
        return Task.CompletedTask;
    }
}
