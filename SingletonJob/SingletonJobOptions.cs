namespace SingletonJob;

/// <summary>
/// Configuration for <see cref="SingletonBackgroundJob"/>-derived jobs.
/// Bound from configuration by the source-generated <c>services.AddSingletonJobs(config)</c>,
/// or override per-job with <c>services.PostConfigureSingletonJob("job-name", o =&gt; ...)</c>.
/// </summary>
public class SingletonJobOptions
{
    /// <summary>Default appsettings section name: <c>"SingletonJob"</c>.</summary>
    public const string SectionName = "SingletonJob";

    /// <summary>
    /// Prefix for Redis lock keys. <b>Required.</b> Set a unique value per deployment so multiple projects sharing
    /// a Redis instance do not collide. Final key format: <c>{ProjectName}:{JobName}:lock</c>.
    /// </summary>
    /// <remarks>
    /// No default value is supplied on purpose: a shared default would silently let two unrelated deployments collide
    /// on the same Redis instance. If unset, <see cref="Validate"/> throws at host startup.
    /// </remarks>
    public string ProjectName { get; set; } = "";

    /// <summary>
    /// How often the leader election loop checks/renews the Redis lock. Default 3 seconds.
    /// Must be strictly less than <see cref="LockExpiry"/> (recommend at least 3x smaller).
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// TTL applied to the Redis lock key. If the leader fails to renew within this window the lock expires
    /// and another instance can acquire it. Default 10 seconds.
    /// </summary>
    public TimeSpan LockExpiry { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Optional override for the node identifier used inside lock values and log lines.
    /// When null, the library resolves it as: <c>POD_NAME</c> environment variable if set, else
    /// <see cref="Environment.MachineName"/>; an 8-char random suffix is always appended so multiple
    /// processes on the same host stay distinguishable.
    /// </summary>
    public string? NodeId { get; set; }

    /// <summary>
    /// On consecutive Redis errors the heartbeat delay doubles each time (exponential backoff), capped at
    /// this absolute ceiling. Each delay also has ±20% jitter applied to avoid thundering-herd reconnects
    /// when Redis recovers. Default 30 seconds. Only applies while the node is a follower: a leader whose
    /// lease is still valid retries at the plain <see cref="HeartbeatInterval"/>, since backing off would
    /// forfeit the lock before the next attempt.
    /// </summary>
    public TimeSpan MaxBackoffDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// When true, the <see cref="CancellationToken"/> passed to a job iteration also fires when this node
    /// loses leadership while the iteration is in flight (lease expiry, preemption, live disable, graceful
    /// shutdown). This shrinks the window where two nodes can run the same job concurrently, provided the
    /// job honors its token. When false (the default, matching 1.0 behavior) a started iteration only
    /// observes host shutdown and otherwise runs to completion.
    /// </summary>
    public bool CancelOnLostLeadership { get; set; }

    /// <summary>
    /// Hard kill switch, evaluated once at startup. When false the job neither executes nor participates in
    /// leader election; the hosted service starts and immediately idles. Set <c>"Enabled": false</c> in the
    /// <c>SingletonJob</c> config section to disable every job in the project, or per job via
    /// <c>PostConfigureSingletonJob("job-name", o =&gt; o.Enabled = false)</c>. For live (runtime) toggling,
    /// e.g. from a feature-flag service, override <see cref="SingletonBackgroundJob.IsJobEnabledAsync"/>
    /// instead. Default true.
    /// </summary>
    public bool Enabled { get; set; } = true;

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ProjectName))
            throw new InvalidOperationException(
                $"{nameof(SingletonJobOptions)}.{nameof(ProjectName)} must be set. " +
                "Configure it via appsettings.json (\"SingletonJob:ProjectName\"), environment variable " +
                "(\"SingletonJob__ProjectName\"), or programmatically " +
                "(services.PostConfigure<SingletonJobOptions>(o => o.ProjectName = \"...\")). " +
                "No default is supplied so unrelated deployments sharing the same Redis instance do not collide on lock keys.");
        if (HeartbeatInterval <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(SingletonJobOptions)}.{nameof(HeartbeatInterval)} must be positive.");
        if (LockExpiry <= HeartbeatInterval)
            throw new InvalidOperationException(
                $"{nameof(SingletonJobOptions)}.{nameof(LockExpiry)} ({LockExpiry}) must be greater than {nameof(HeartbeatInterval)} ({HeartbeatInterval}). Recommend LockExpiry >= 3x HeartbeatInterval.");
        if (MaxBackoffDelay < HeartbeatInterval)
            throw new InvalidOperationException(
                $"{nameof(SingletonJobOptions)}.{nameof(MaxBackoffDelay)} ({MaxBackoffDelay}) must be >= {nameof(HeartbeatInterval)} ({HeartbeatInterval}).");
    }
}
