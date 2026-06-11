using Cronos;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class TimeProviderTests(RedisFixture fx)
{
    [Fact]
    public async Task Cron_job_fires_on_schedule_under_fake_time()
    {
        await using var redis = await fx.ConnectAsync();
        var fake = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));

        // Long heartbeat/expiry so leadership survives the large virtual-time jumps between renewals.
        // Redis holds the lock with a real-time TTL of 30 hours, far beyond the test's real runtime.
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromHours(1),
            LockExpiry = TimeSpan.FromHours(30),
            MaxBackoffDelay = TimeSpan.FromHours(2),
        });

        var expr = CronExpression.Parse("0 3 * * *"); // daily at 03:00 UTC
        var job = new CountingCronJob(redis, opts,
            NullLogger<CountingCronJob>.Instance,
            expr, "faketime", workDuration: default, timeProvider: fake);

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        await Task.Delay(300); // let the initial election round trip to Redis complete

        fake.Advance(TimeSpan.FromHours(2));
        await Task.Delay(200);
        job.RunCount.Should().Be(0, "02:00 is before the daily 03:00 schedule");

        fake.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));
        await Task.Delay(300);
        job.RunCount.Should().Be(1, "the 03:00 occurrence is now due");

        fake.Advance(TimeSpan.FromHours(24));
        await Task.Delay(300);
        job.RunCount.Should().Be(2, "the next day's 03:00 occurrence is now due");

        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
    }
}
