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

    [Fact]
    public async Task Far_future_occurrence_does_not_crash_the_job_loop()
    {
        await using var redis = await fx.ConnectAsync();
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(2),
        });

        // First of the month six months out: always 150+ days away, well past Task.Delay's ~49.7 day limit.
        // The unfixed code threw ArgumentOutOfRangeException from Task.Delay and killed the job loop.
        var month = DateTime.UtcNow.AddMonths(6).Month;
        var expr = CronExpression.Parse($"0 0 1 {month} *");
        var job = new CountingCronJob(redis, opts,
            NullLogger<CountingCronJob>.Instance,
            expr, "farfuture");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await Task.Delay(1000, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.ExecuteTask.Should().NotBeNull();
        job.ExecuteTask!.IsFaulted.Should().BeFalse("long sleeps must be chunked, not passed to Task.Delay whole");
        job.RunCount.Should().Be(0);
    }

    [Fact]
    public async Task Slow_job_skips_missed_occurrences_instead_of_replaying()
    {
        await using var redis = await fx.ConnectAsync();
        var opts = new StaticOptionsFactory<SingletonJobOptions>(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            LockExpiry = TimeSpan.FromSeconds(5),
        });

        // Fires every 2 seconds but each run takes 3 seconds, so every run overlaps the next occurrence.
        // The overlapped occurrence must be skipped: starts stay on the cron grid 4 seconds apart.
        // The pre-fix behavior replayed the missed occurrence immediately, giving ~3 second back-to-back gaps.
        var expr = CronExpression.Parse("*/2 * * * * *", CronFormat.IncludeSeconds);
        var job = new CountingCronJob(redis, opts,
            NullLogger<CountingCronJob>.Instance,
            expr, "slowcron", workDuration: TimeSpan.FromSeconds(3));

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);

        await Task.Delay(11000, cts.Token);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().BeGreaterThanOrEqualTo(2);
        List<DateTimeOffset> starts;
        lock (job.RunStarts) starts = [.. job.RunStarts];
        for (var i = 1; i < starts.Count; i++)
        {
            (starts[i] - starts[i - 1]).Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(3.5),
                "missed occurrences must be skipped, not fired immediately after the previous run");
        }
    }
}
