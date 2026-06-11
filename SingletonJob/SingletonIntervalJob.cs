using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob;

/// <summary>
/// Runs <see cref="SingletonBackgroundJob.ExecuteJobAsync"/>, then waits for <see cref="GetJobInterval"/> before
/// running again. The interval is measured from the end of the previous run, so a slow iteration delays
/// the next one. Use this when "at least N seconds between runs" semantics are wanted.
/// </summary>
public abstract class SingletonIntervalJob : SingletonBackgroundJob
{
    /// <summary>
    /// Implement to return the wait time between iterations. Re-read after every iteration, so a dynamic
    /// value takes effect on the next wait. Must be positive and at most 49.7 days.
    /// </summary>
    protected abstract TimeSpan GetJobInterval();

    /// <inheritdoc />
    protected SingletonIntervalJob(
        IConnectionMultiplexer redis,
        IOptionsFactory<SingletonJobOptions> options,
        ILogger logger)
        : base(redis, options, logger)
    {
    }

    /// <inheritdoc />
    protected SingletonIntervalJob(
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
        while (!stoppingToken.IsCancellationRequested)
        {
            if (IsLeader && IsEnabled)
            {
                Logger.LogDebug("Job {JobName} iteration starting", JobName);
                var startTs = TimeProvider.GetTimestamp();
                try
                {
                    await ExecuteIterationAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (OperationCanceledException)
                {
                    Logger.LogInformation("Job {JobName} iteration cancelled after leadership was lost.", JobName);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Job {JobName} execution failed.", JobName);
                }
                var elapsed = TimeProvider.GetElapsedTime(startTs);
                Logger.LogDebug("Job {JobName} iteration completed in {ElapsedMs}ms", JobName, elapsed.TotalMilliseconds);
                WarnIfExecutionTimeTooLong(elapsed);
            }

            try
            {
                await Task.Delay(ValidateJobInterval(GetJobInterval(), JobName), TimeProvider, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
