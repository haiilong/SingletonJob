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
