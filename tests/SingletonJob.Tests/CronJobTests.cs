using Cronos;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class CronJobTests(RedisFixture fx)
{
    [Fact]
    public async Task Fires_on_every_second_cron()
    {
        await using var redis = await fx.ConnectAsync();
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(2),
        });

        // Cronos seconds-precision cron: "* * * * * *" = every second.
        var expr = CronExpression.Parse("* * * * * *", CronFormat.IncludeSeconds);
        var job = new CountingCronJob(redis, opts,
            NullLogger<CountingCronJob>.Instance,
            expr, "everysecond");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await Task.Delay(3500, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        // 3.5s window with cron firing every second. Allow some slack for startup + final-tick rounding.
        job.RunCount.Should().BeInRange(2, 6);
    }
}
