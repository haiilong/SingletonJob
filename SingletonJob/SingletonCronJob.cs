using System.Diagnostics;
using Cronos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob;

/// <summary>
/// Runs on a cron schedule. The job sleeps until the next occurrence of <see cref="GetCronExpression"/>,
/// then runs once if this node is the leader. Use this for "once a day at 03:00", "every 15 minutes
/// past the hour", and similar wall-clock schedules.
/// </summary>
/// <remarks>
/// Cron expressions are parsed by <see href="https://github.com/HangfireIO/Cronos">Cronos</see>. By default
/// the schedule is interpreted in UTC; override <see cref="TimeZone"/> to use a different zone.
/// </remarks>
public abstract class SingletonCronJob : SingletonBackgroundJob
{
    /// <summary>Implement to return the parsed cron expression. Use <see cref="CronExpression.Parse(string)"/>.</summary>
    protected abstract CronExpression GetCronExpression();

    /// <summary>Time zone used to evaluate the cron expression. Defaults to UTC.</summary>
    protected virtual TimeZoneInfo TimeZone => TimeZoneInfo.Utc;

    /// <inheritdoc />
    protected SingletonCronJob(
        IConnectionMultiplexer redis,
        IOptionsFactory<SingletonJobOptions> options,
        ILogger logger)
        : base(redis, options, logger)
    {
    }

    /// <inheritdoc />
    protected override async Task ExecuteJobLoopAsync(CancellationToken stoppingToken)
    {
        var expr = GetCronExpression();

        // Track the pivot for the next-occurrence lookup so the loop strictly advances even if Cronos
        // returns a value at or before the pivot for second-precision expressions like "* * * * * *".
        // The pivot starts in the past so the first lookup returns the very next occurrence.
        var pivot = DateTimeOffset.UtcNow.AddTicks(-1);

        while (!stoppingToken.IsCancellationRequested)
        {
            var next = expr.GetNextOccurrence(pivot, TimeZone, inclusive: false);
            if (next is null)
            {
                Logger.LogWarning("Cron job {JobName} has no future occurrences. Stopping loop.", JobName);
                return;
            }

            // Defensive guard: Cronos should always return a value strictly greater than `pivot` when
            // inclusive=false, but guard against any edge case to avoid a busy loop.
            if (next.Value <= pivot)
            {
                pivot = pivot.AddTicks(1);
                continue;
            }

            pivot = next.Value;

            var delay = next.Value - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            if (!IsLeader) continue;

            Logger.LogDebug("Cron job {JobName} firing for scheduled time {ScheduledTime:O}", JobName, next.Value);
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
                Logger.LogError(ex, "Cron job {JobName} failed.", JobName);
            }
            var elapsed = Stopwatch.GetElapsedTime(startTs);
            Logger.LogDebug("Cron job {JobName} completed in {ElapsedMs}ms", JobName, elapsed.TotalMilliseconds);
            WarnIfExecutionTimeTooLong(elapsed);
        }
    }
}
