using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob;

/// <summary>
/// Runs at a fixed rate using <see cref="PeriodicTimer"/>. If the previous run is still in flight when a tick
/// arrives the tick is dropped (no overlapping execution, no queueing). Use this when "fire every N ms but
/// never overlap" semantics are wanted, for example a price-tick poller or a health-check writer.
/// </summary>
public abstract class SingletonFixedRateJob : SingletonBackgroundJob
{
    private volatile bool _isJobRunning;
    private Task? _currentRun;

    /// <summary>
    /// Implement to return the period between ticks. Read once when the job starts (the
    /// <see cref="PeriodicTimer"/> is created with it), so later changes have no effect.
    /// Must be positive and at most 49.7 days.
    /// </summary>
    protected abstract TimeSpan GetJobInterval();

    /// <inheritdoc />
    protected SingletonFixedRateJob(
        IConnectionMultiplexer redis,
        IOptionsFactory<SingletonJobOptions> options,
        ILogger logger)
        : base(redis, options, logger)
    {
    }

    /// <inheritdoc />
    protected SingletonFixedRateJob(
        IConnectionMultiplexer redis,
        IOptionsFactory<SingletonJobOptions> options,
        ILogger logger,
        TimeProvider timeProvider)
        : base(redis, options, logger, timeProvider)
    {
    }

    /// <inheritdoc />
    protected override async Task ExecuteJobLoopAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(ValidateJobInterval(GetJobInterval(), JobName), TimeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                if (IsLeader && IsEnabled && !_isJobRunning)
                {
                    _isJobRunning = true;
                    _currentRun = ExecuteAndResetFlagAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutdown, fall through to await any in-flight run
        }
        finally
        {
            // Wait for the most recent fire-and-forget run to finish so shutdown is graceful.
            if (_currentRun is { } run)
            {
                try { await run.ConfigureAwait(false); } catch { /* logged inside */ }
            }
        }
    }

    private async Task ExecuteAndResetFlagAsync(CancellationToken cancellationToken)
    {
        Logger.LogDebug("Job {JobName} iteration starting", JobName);
        var startTs = TimeProvider.GetTimestamp();
        try
        {
            await ExecuteIterationAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // graceful shutdown
        }
        catch (OperationCanceledException)
        {
            Logger.LogInformation("Job {JobName} iteration cancelled after leadership was lost.", JobName);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Job {JobName} failed during fixed-rate execution.", JobName);
        }
        finally
        {
            var elapsed = TimeProvider.GetElapsedTime(startTs);
            Logger.LogDebug("Job {JobName} iteration completed in {ElapsedMs}ms", JobName, elapsed.TotalMilliseconds);
            WarnIfExecutionTimeTooLong(elapsed);
            _isJobRunning = false;
        }
    }
}
