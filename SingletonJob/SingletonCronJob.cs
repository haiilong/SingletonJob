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
/// Occurrences that pass while an execution is still running are skipped, not replayed: a job slower than
/// its cron period resumes at the next future occurrence instead of firing once per missed occurrence.
/// </remarks>
public abstract class SingletonCronJob : SingletonBackgroundJob
{
    // Task.Delay rejects anything above uint.MaxValue - 1 milliseconds (~49.7 days), which a sparse cron
    // (yearly, specific dates) easily exceeds. Sleeping in bounded chunks and recomputing the remaining
    // time also keeps the wake-up accurate if the system clock is adjusted during a long sleep.
    private static readonly TimeSpan MaxSleepChunk = TimeSpan.FromDays(1);

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
    protected SingletonCronJob(
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
        var expr = GetCronExpression()
            ?? throw new InvalidOperationException($"Job '{JobName}': GetCronExpression() returned null.");

        // Track the pivot for the next-occurrence lookup so the loop strictly advances even if Cronos
        // returns a value at or before the pivot for second-precision expressions like "* * * * * *".
        // The pivot starts in the past so the first lookup returns the very next occurrence.
        // The pivot is an absolute instant (UTC); TimeZone is passed to Cronos below, which interprets
        // the cron fields in that zone (including DST transitions) and returns an absolute instant back.
        // Doing the arithmetic on absolute instants keeps it unambiguous across DST changes.
        var pivot = TimeProvider.GetUtcNow().AddTicks(-1);

        while (!stoppingToken.IsCancellationRequested)
        {
            // Occurrences that passed while the previous iteration was running are skipped, not replayed.
            // Without this clamp a job slower than its cron period would fire back-to-back once per missed
            // occurrence (a catch-up storm). Skipping matches the drop semantics of the rest of the library.
            var now = TimeProvider.GetUtcNow();
            if (pivot < now) pivot = now;

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

            try
            {
                var delay = next.Value - TimeProvider.GetUtcNow();
                while (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay < MaxSleepChunk ? delay : MaxSleepChunk, TimeProvider, stoppingToken).ConfigureAwait(false);
                    delay = next.Value - TimeProvider.GetUtcNow();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (!IsLeader || !IsEnabled) continue;

            Logger.LogDebug("Cron job {JobName} firing for scheduled time {ScheduledTime:O}", JobName, next.Value);
            var startTs = TimeProvider.GetTimestamp();
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
            var elapsed = TimeProvider.GetElapsedTime(startTs);
            Logger.LogDebug("Cron job {JobName} completed in {ElapsedMs}ms", JobName, elapsed.TotalMilliseconds);
            WarnIfExecutionTimeTooLong(elapsed);
        }
    }
}
