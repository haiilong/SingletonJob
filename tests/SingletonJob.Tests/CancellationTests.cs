using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class CancellationTests(RedisFixture fx)
{
    private static IOptionsFactory<SingletonJobOptions> Opts(bool cancelOnLostLeadership) =>
        new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(2),
            CancelOnLostLeadership = cancelOnLostLeadership,
        });

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!condition() && Environment.TickCount64 < deadline)
            await Task.Delay(50);
        condition().Should().BeTrue("condition should be met within {0}ms", timeoutMs);
    }

    [Fact]
    public async Task In_flight_iteration_is_cancelled_when_leadership_ends()
    {
        await using var redis = await fx.ConnectAsync();
        var job = new BlockingIntervalJob(redis, Opts(cancelOnLostLeadership: true),
            NullLogger<BlockingIntervalJob>.Instance, "cancel-on");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        await WaitUntilAsync(() => job.StartedCount > 0);

        // Live disable releases the lock and ends the leadership term, which must cancel the
        // 30 second iteration that is still in flight.
        job.Enabled = false;
        await WaitUntilAsync(() => job.IterationCancelled);

        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task In_flight_iteration_keeps_running_by_default()
    {
        await using var redis = await fx.ConnectAsync();
        var job = new BlockingIntervalJob(redis, Opts(cancelOnLostLeadership: false),
            NullLogger<BlockingIntervalJob>.Instance, "cancel-off");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        await WaitUntilAsync(() => job.StartedCount > 0);

        job.Enabled = false;
        // Give the election loop several heartbeats to observe the disable and release the lock.
        await Task.Delay(1000);
        job.IterationCancelled.Should().BeFalse("the 1.0 default lets a started iteration run to completion");

        // Shutdown still cancels the iteration so the host is not held for 30 seconds.
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
        job.IterationCancelled.Should().BeTrue();
    }
}
