using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class LeaderElectionTests(RedisFixture fx)
{
    private static IOptionsFactory<SingletonJobOptions> Opts(string project) =>
        new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = project,
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(2),
        });

    [Fact]
    public async Task Single_instance_runs_job()
    {
        await using var redis = await fx.ConnectAsync();
        var job = new CountingIntervalJob(redis, Opts(Guid.NewGuid().ToString("N")),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "single");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await Task.Delay(1500, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().BeGreaterThan(3);
    }

    [Fact]
    public async Task Two_instances_only_one_runs_job()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var jobA = new CountingIntervalJob(redis, Opts(project),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "shared");
        var jobB = new CountingIntervalJob(redis, Opts(project),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "shared");

        using var cts = new CancellationTokenSource();
        await jobA.StartAsync(cts.Token);
        await jobB.StartAsync(cts.Token);

        await Task.Delay(2000, cts.Token);
        await cts.CancelAsync();
        await jobA.StopAsync(CancellationToken.None);
        await jobB.StopAsync(CancellationToken.None);

        // Total runs equals what one instance would have run; the loser should be near zero.
        var min = Math.Min(jobA.RunCount, jobB.RunCount);
        var max = Math.Max(jobA.RunCount, jobB.RunCount);
        max.Should().BeGreaterThan(5);
        min.Should().BeLessThan(3); // small slack for failover transitions
    }

    [Fact]
    public async Task Duplicate_job_name_across_different_classes_throws_at_startup()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var jobA = new CountingIntervalJob(redis, Opts(project),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "dup");
        var jobB = new ToggleableIntervalJob(redis, Opts(project),
            NullLogger<ToggleableIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "dup");

        using var cts = new CancellationTokenSource();
        await jobA.StartAsync(cts.Token);

        var act = () => jobB.StartAsync(cts.Token);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*dup*");

        await cts.CancelAsync();
        await jobA.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Invalid_interval_fails_fast_with_job_name_in_message()
    {
        await using var redis = await fx.ConnectAsync();
        var job = new CountingIntervalJob(redis, Opts(Guid.NewGuid().ToString("N")),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.Zero, "badinterval");

        using var cts = new CancellationTokenSource();
        Exception? startupFailure = null;
        try
        {
            await job.StartAsync(cts.Token);
        }
        catch (InvalidOperationException ex)
        {
            // ExecuteAsync can fault before StartAsync returns, in which case the host surfaces it here.
            startupFailure = ex;
        }

        if (startupFailure is null)
        {
            var deadline = Environment.TickCount64 + 5000;
            while (job.ExecuteTask is { IsCompleted: false } && Environment.TickCount64 < deadline)
                await Task.Delay(50);

            job.ExecuteTask!.IsFaulted.Should().BeTrue("a non-positive interval must fail the job, not hang it");
            startupFailure = job.ExecuteTask.Exception!.GetBaseException();
        }

        startupFailure.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("badinterval");

        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Leader_self_demotes_when_redis_is_unreachable()
    {
        await using var redis = await fx.ConnectAsync();
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromMilliseconds(800),
        });
        var job = new CountingIntervalJob(redis, opts,
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(50), "fence");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        var deadline = Environment.TickCount64 + 5000;
        while (job.RunCount == 0 && Environment.TickCount64 < deadline)
            await Task.Delay(50, cts.Token);
        job.RunCount.Should().BeGreaterThan(0, "the job should become leader and run first");

        // Sever connectivity. Heartbeats now throw, so the lease can no longer be renewed.
        await redis.CloseAsync();

        // Wait past LockExpiry (plus slack for an in-flight iteration); the node must self-demote.
        await Task.Delay(1500);
        var afterFence = job.RunCount;
        await Task.Delay(600);
        job.RunCount.Should().Be(afterFence,
            "a leader that cannot renew within LockExpiry must stop executing (self-fencing)");

        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Killing_leader_promotes_follower()
    {
        await using var redis = await fx.ConnectAsync();
        var project = Guid.NewGuid().ToString("N");
        var jobA = new CountingIntervalJob(redis, Opts(project),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "fail");
        var jobB = new CountingIntervalJob(redis, Opts(project),
            NullLogger<CountingIntervalJob>.Instance,
            TimeSpan.FromMilliseconds(100), "fail");

        using var ctsA = new CancellationTokenSource();
        using var ctsB = new CancellationTokenSource();
        await jobA.StartAsync(ctsA.Token);
        await jobB.StartAsync(ctsB.Token);

        await Task.Delay(800, CancellationToken.None);
        var leaderRunsBefore = Math.Max(jobA.RunCount, jobB.RunCount);
        leaderRunsBefore.Should().BeGreaterThan(0);

        // Stop the one that's leader (whichever ran more so far).
        if (jobA.RunCount > jobB.RunCount)
        {
            await ctsA.CancelAsync();
            await jobA.StopAsync(CancellationToken.None);
        }
        else
        {
            await ctsB.CancelAsync();
            await jobB.StopAsync(CancellationToken.None);
        }

        var beforeFailover = jobA.RunCount + jobB.RunCount;
        // Graceful release should let other side take over within HeartbeatInterval.
        await Task.Delay(1500, CancellationToken.None);
        var afterFailover = jobA.RunCount + jobB.RunCount;

        afterFailover.Should().BeGreaterThan(beforeFailover);

        await ctsB.CancelAsync();
        await jobB.StopAsync(CancellationToken.None);
    }
}
