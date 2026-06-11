using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class EnablementTests(RedisFixture fx)
{
    private static IOptionsFactory<SingletonJobOptions> Opts(string project, bool enabled = true) =>
        new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = project,
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(2),
            Enabled = enabled,
        });

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!condition() && Environment.TickCount64 < deadline)
            await Task.Delay(50);
        condition().Should().BeTrue("condition should be met within {0}ms", timeoutMs);
    }

    [Fact]
    public async Task Statically_disabled_job_never_runs_or_takes_lock()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var job = new CountingIntervalJob(redis, Opts(project, enabled: false),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "static-off");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await Task.Delay(1200, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().Be(0);
        var exists = await redis.GetDatabase().KeyExistsAsync($"{project}:static-off:lock");
        exists.Should().BeFalse("a disabled job must not participate in leader election");
    }

    [Fact]
    public async Task Live_toggle_stops_runs_releases_lock_and_resumes()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var lockKey = $"{project}:toggle:lock";
        var job = new ToggleableIntervalJob(redis, Opts(project),
            NullLogger<ToggleableIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(50), "toggle");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await WaitUntilAsync(() => job.RunCount > 0);

        job.Enabled = false;
        // Disable is observed on the next heartbeat (200ms); give it a few cycles, then the lock must be gone.
        await WaitUntilAsync(() => !redis.GetDatabase().KeyExists(lockKey));

        var countAfterDisable = job.RunCount;
        await Task.Delay(600, cts.Token);
        job.RunCount.Should().Be(countAfterDisable, "a disabled job must not execute iterations");

        job.Enabled = true;
        await WaitUntilAsync(() => job.RunCount > countAfterDisable);

        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Disabled_leader_yields_to_enabled_follower()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var jobA = new ToggleableIntervalJob(redis, Opts(project),
            NullLogger<ToggleableIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(50), "yield");
        var jobB = new ToggleableIntervalJob(redis, Opts(project),
            NullLogger<ToggleableIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(50), "yield");

        using var cts = new CancellationTokenSource();
        // Start A first so it deterministically wins the election.
        await jobA.StartAsync(cts.Token);
        await WaitUntilAsync(() => jobA.RunCount > 0);
        await jobB.StartAsync(cts.Token);

        jobA.Enabled = false;
        // A releases on its next heartbeat; B acquires on its own next heartbeat and starts running.
        await WaitUntilAsync(() => jobB.RunCount > 0);

        var aAfterYield = jobA.RunCount;
        await Task.Delay(600, cts.Token);
        jobA.RunCount.Should().Be(aAfterYield, "the disabled ex-leader must stay idle");

        await cts.CancelAsync();
        await jobA.StopAsync(CancellationToken.None);
        await jobB.StopAsync(CancellationToken.None);
    }
}
