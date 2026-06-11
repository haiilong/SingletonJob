using System.Diagnostics;
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
    /// <summary>Implement to return the wait time between iterations.</summary>
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
    protected override async Task ExecuteJobLoopAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (IsLeader && IsEnabled)
            {
                Logger.LogDebug("Job {JobName} iteration starting", JobName);
                var startTs = Stopwatch.GetTimestamp();
                try
                {
                    await ExecuteJobAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "Job {JobName} execution failed.", JobName);
                }
                var elapsed = Stopwatch.GetElapsedTime(startTs);
                Logger.LogDebug("Job {JobName} iteration completed in {ElapsedMs}ms", JobName, elapsed.TotalMilliseconds);
                WarnIfExecutionTimeTooLong(elapsed);
            }

            try
            {
                await Task.Delay(GetJobInterval(), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
