namespace SingletonJob;

/// <summary>
/// What <see cref="SingletonCronJob"/> does when a scheduled occurrence passes without firing, for example
/// because the previous execution overran the cron period, the process was suspended, or the clock jumped
/// forward. It does not cover occurrences missed while no replica held leadership: the loop only evaluates
/// the schedule on the node that is running it.
/// </summary>
public enum CronMisfirePolicy
{
    /// <summary>
    /// Drop everything missed and resume at the next future occurrence. The default, and the right choice
    /// for frequent schedules where the next run supersedes the missed one.
    /// </summary>
    Skip,

    /// <summary>
    /// Run one execution immediately to cover all missed occurrences, then resume the schedule. Use for
    /// infrequent jobs (hourly, daily) where running late is better than not running at all.
    /// </summary>
    FireOnce,

    /// <summary>
    /// Replay every missed occurrence back-to-back. Only use when each occurrence processes a distinct
    /// time bucket and must not be lost; a long gap causes a burst of consecutive runs.
    /// </summary>
    CatchUp,
}
