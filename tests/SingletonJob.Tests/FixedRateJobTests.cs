using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class FixedRateJobTests(RedisFixture fx)
{
    [Fact]
    public async Task Slow_iteration_drops_overlapping_ticks()
    {
        await using var redis = await fx.ConnectAsync();
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(5),
        });

        var job = new CountingFixedRateJob(redis, opts,
            NullLogger<CountingFixedRateJob>.Instance,
            interval: TimeSpan.FromMilliseconds(100),
            workDuration: TimeSpan.FromMilliseconds(500),
            "slow");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        // Window: ~2s. Tick every 100ms = ~20 ticks. Work = 500ms. So expected runs ~= 4.
        await Task.Delay(2200, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().BeInRange(2, 6, "overlapping ticks must be dropped while previous run is in flight");
    }
}
