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

    private static async Task RunUntilIterationCompletesAsync(SlowIntervalJob job)
    {
        using var cts = new CancellationTokenSource();
        await job.StartAsync(cts.Token);
        try
        {
            var deadline = Environment.TickCount64 + 5_000;
            while (Volatile.Read(ref job.CompletedCount) == 0 && Environment.TickCount64 < deadline)
                await Task.Delay(50);
        }
        finally
        {
            await cts.CancelAsync();
            await job.StopAsync(CancellationToken.None);
        }

        job.CompletedCount.Should().BeGreaterThan(0, "an iteration must complete within five seconds");
    }

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

        await RunUntilIterationCompletesAsync(job);

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

        await RunUntilIterationCompletesAsync(job);

        job.RunCount.Should().BeGreaterThan(0, "the opt-out must suppress the warning, not the work");
        logger.HasWarningContaining(WarningFragment).Should().BeFalse();
    }
}
