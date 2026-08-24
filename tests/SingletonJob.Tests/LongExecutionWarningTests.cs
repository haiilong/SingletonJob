using FluentAssertions;

namespace SingletonJob.Tests;

[Collection(nameof(RedisCollection))]
public class LongExecutionWarningTests(RedisFixture fx)
{
    private const string WarningFragment = "close to LockExpiry";

    // Threshold is 80% of LockExpiry, so 400ms here. The heartbeat is far shorter, so the lease stays
    // renewed throughout the iteration and the run is never demoted mid-flight.
    private static StaticOptionsFactory<SingletonJobOptions> Options() =>
        new(new SingletonJobOptions
        {
            ProjectName = Guid.NewGuid().ToString("N"),
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            LockExpiry = TimeSpan.FromMilliseconds(500),
            MaxBackoffDelay = TimeSpan.FromMilliseconds(200),
        });

    [Fact]
    public async Task Warns_when_an_iteration_runs_for_most_of_the_lock_expiry()
    {
        await using var redis = await fx.ConnectAsync();
        var logger = new CapturingLogger<SlowIntervalJob>();
        var job = new SlowIntervalJob(
            redis,
            Options(),
            logger,
            workDuration: TimeSpan.FromMilliseconds(700),
            warnOnLongExecution: true,
            "slow-warns");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        await Task.Delay(1_500);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().BeGreaterThan(0, "the iteration must actually have run");
        logger.HasWarningContaining(WarningFragment).Should().BeTrue();
    }

    [Fact]
    public async Task Does_not_warn_when_the_job_opts_out()
    {
        // A job whose iteration is deliberately long-lived — a connection held open for hours — would
        // otherwise warn on every single iteration, with advice that cannot be acted on: no LockExpiry
        // exceeds hours, and shortening the job would mean abandoning the pattern. Renewal runs on the
        // election loop regardless of iteration length, so the duration carries no signal here.
        await using var redis = await fx.ConnectAsync();
        var logger = new CapturingLogger<SlowIntervalJob>();
        var job = new SlowIntervalJob(
            redis,
            Options(),
            logger,
            workDuration: TimeSpan.FromMilliseconds(700),
            warnOnLongExecution: false,
            "slow-quiet");

        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        await Task.Delay(1_500);
        await cts.CancelAsync();
        await job.StopAsync(CancellationToken.None);

        job.RunCount.Should().BeGreaterThan(0, "the opt-out must suppress the warning, not the work");
        logger.HasWarningContaining(WarningFragment).Should().BeFalse();
    }
}
