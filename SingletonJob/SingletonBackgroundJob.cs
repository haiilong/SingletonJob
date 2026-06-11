using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SingletonJob;

/// <summary>
/// Base class for distributed singleton background jobs. Implements Redis-based leader election so that
/// across N replicas of a process, exactly one instance executes <see cref="ExecuteJobAsync"/> at a time.
/// Concrete jobs should derive from <see cref="SingletonIntervalJob"/>, <see cref="SingletonFixedRateJob"/>,
/// or <see cref="SingletonCronJob"/>.
/// </summary>
public abstract class SingletonBackgroundJob : BackgroundService
{
    /// <summary>Logger for derived classes to use.</summary>
    protected readonly ILogger Logger;

    private readonly IConnectionMultiplexer _redis;
    private readonly IOptionsFactory<SingletonJobOptions> _optionsFactory;
    private SingletonJobOptions _options = null!;

    /// <summary>Unique name for this job. Combined with <see cref="SingletonJobOptions.ProjectName"/> to form the Redis lock key.</summary>
    public abstract string JobName { get; }

    /// <summary>Implement to perform a single iteration of the job.</summary>
    protected abstract Task ExecuteJobAsync(CancellationToken cancellationToken);

    private IDatabaseAsync? _db;
    private string? _nodeId;
    private string? _lockKey;
    private bool _isLeader;
    private bool _isEnabled = true;

    // Lease deadline in Environment.TickCount64 milliseconds. Set on every successful acquire/renew from a
    // timestamp taken BEFORE the Redis call, so the local fence is always at least as strict as the
    // server-side TTL (the server applies the TTL at some point after we took the timestamp).
    private long _leaseValidUntil;

    /// <summary>
    /// True when this node currently holds the Redis leadership lock and the lease confirmed by the last
    /// successful acquire/renew has not yet expired. If renewals fail (for example, Redis is unreachable
    /// from this node only), this turns false once <see cref="SingletonJobOptions.LockExpiry"/> has elapsed
    /// since the last successful call: at that point the key has expired server-side and a peer may already
    /// own the lock, so this node self-demotes rather than risk duplicate execution.
    /// </summary>
    protected bool IsLeader =>
        Volatile.Read(ref _isLeader) && Environment.TickCount64 < Volatile.Read(ref _leaseValidUntil);

    /// <summary>
    /// True when the job is currently enabled, as last observed by the election loop. Refreshed once per
    /// <see cref="SingletonJobOptions.HeartbeatInterval"/> from <see cref="IsJobEnabledAsync"/>.
    /// </summary>
    protected bool IsEnabled => Volatile.Read(ref _isEnabled);

    /// <summary>The configured options for this job (resolved on <see cref="StartAsync"/>).</summary>
    protected SingletonJobOptions Options => _options;

    // Process-wide registry of lock keys, used to catch two different job classes sharing a JobName.
    // Without the check both classes silently contend for the same lock and only one of them ever runs.
    // Keyed by lock key and storing the concrete type so multiple instances of the SAME class (for
    // example, multi-replica simulations in tests) stay allowed.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type> RegisteredLockKeys = new();

    // Atomic acquire-or-renew: take the lock if it is free, extend the TTL if this node already owns it.
    // One round trip per heartbeat instead of a failed SETNX followed by a separate renew script.
    // Returns 1 when this node holds the lock after the call, 0 when another node does.
    private const string AcquireOrRenewScript =
        "local v = redis.call('GET', KEYS[1]) " +
        "if not v then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 " +
        "elseif v == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1 " +
        "else return 0 end";

    // Atomic release: delete lock only if we own it.
    private const string ReleaseScript =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

    /// <summary>Initializes the base job with Redis, options, and a logger.</summary>
    protected SingletonBackgroundJob(
        IConnectionMultiplexer redis,
        IOptionsFactory<SingletonJobOptions> optionsFactory,
        ILogger logger)
    {
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _optionsFactory = optionsFactory ?? throw new ArgumentNullException(nameof(optionsFactory));
        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolve named options keyed by JobName so callers can override per-job:
        //   services.PostConfigureSingletonJob("heavy-job", o => o.LockExpiry = ...)
        // Using IOptionsFactory.Create freezes the options at startup, no live-reload subscription.
        _options = _optionsFactory.Create(JobName);
        _options.Validate();

        _db = _redis.GetDatabase();
        _nodeId = ResolveNodeId(_options);
        _lockKey = $"{_options.ProjectName}:{JobName}:lock";

        var owner = RegisteredLockKeys.GetOrAdd(_lockKey, GetType());
        if (owner != GetType())
        {
            throw new InvalidOperationException(
                $"Duplicate job name: '{JobName}' (lock key '{_lockKey}') is used by both " +
                $"{owner.FullName} and {GetType().FullName}. Each job class must have a unique JobName; " +
                "with a shared name the two jobs silently contend for the same lock and only one of them runs.");
        }

        Logger.LogInformation("SingletonJob started: {LockKey}. Node: {NodeId}", _lockKey, _nodeId);
        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        if (_lockKey is not null)
            RegisteredLockKeys.TryRemove(new KeyValuePair<string, Type>(_lockKey, GetType()));
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    private static string ResolveNodeId(SingletonJobOptions options)
    {
        var basePart = options.NodeId
            ?? Environment.GetEnvironmentVariable("POD_NAME")
            ?? Environment.MachineName;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return basePart + "-" + suffix;
    }

    /// <summary>
    /// Controls whether this job may run, re-evaluated by the election loop once per
    /// <see cref="SingletonJobOptions.HeartbeatInterval"/>. The default returns true. Override to plug in a
    /// live toggle: inject your feature-flag service into the derived job and query it here. While the
    /// result is false this node skips iterations and releases / stops competing for the leadership lock,
    /// so an enabled replica can take over; once it returns true again the node rejoins the election within
    /// one heartbeat. An iteration already in flight when the flag flips is not cancelled.
    /// Exceptions are logged and the previous value is kept, so a flaky flag backend does not flap the job.
    /// Note: <see cref="SingletonJobOptions.Enabled"/> is checked first and wins; a statically disabled job
    /// never calls this method.
    /// </summary>
    protected virtual ValueTask<bool> IsJobEnabledAsync(CancellationToken cancellationToken) => new(true);

    /// <inheritdoc />
    protected sealed override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            Logger.LogInformation(
                "Job {JobName} is disabled by configuration (SingletonJobOptions.Enabled = false). " +
                "It will not run or participate in leader election.", JobName);
            return;
        }

        // Linked so the election loop also stops when the job loop exits for any non-shutdown reason
        // (invalid configuration, an exception escaping the loop, a cron schedule with no future
        // occurrences). Without this the finally below would await the election loop until host shutdown,
        // keeping the lock alive and the failure invisible while no work runs.
        using var electionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var electionTask = RunLeaderElectionLoopAsync(electionCts.Token);

        try
        {
            await ExecuteJobLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            // Stop the heartbeat loop and wait for it to drain so we don't release the lock and then race
            // with a renewal.
            electionCts.Cancel();
            try { await electionTask.ConfigureAwait(false); } catch { /* swallow, already logged in loop */ }

            // Releasing on graceful shutdown enables fast failover: peers can SETNX immediately instead of
            // waiting up to LockExpiry. Important for k8s rolling deploys where SIGTERM is followed by quick replacement.
            await ReleaseLockAsync().ConfigureAwait(false);
        }
    }

    // Shared guard for the interval-returning shapes. Task.Delay and PeriodicTimer both reject values
    // outside (0, uint.MaxValue - 1 ms]; fail with a message that names the job instead of surfacing a
    // bare ArgumentOutOfRangeException from deep inside the loop.
    internal static TimeSpan ValidateJobInterval(TimeSpan interval, string jobName)
    {
        if (interval <= TimeSpan.Zero || interval.TotalMilliseconds > 4294967294)
        {
            throw new InvalidOperationException(
                $"Job '{jobName}': GetJobInterval() returned {interval}. " +
                "The interval must be positive and at most 49.7 days (uint.MaxValue - 1 milliseconds).");
        }
        return interval;
    }

    /// <summary>Implemented by job-shape classes (interval, fixed-rate, cron) to define the execution loop.</summary>
    protected abstract Task ExecuteJobLoopAsync(CancellationToken stoppingToken);

    /// <summary>
    /// Logs a warning if a single iteration of the job took longer than 80% of <see cref="SingletonJobOptions.LockExpiry"/>.
    /// When that happens duplicate execution becomes possible because the leader may not renew in time.
    /// </summary>
    protected void WarnIfExecutionTimeTooLong(TimeSpan duration)
    {
        if (duration > _options.LockExpiry * 0.8)
        {
            Logger.LogWarning(
                "Job {JobName} took {DurationMs}ms which is close to LockExpiry {LockExpiryMs}ms. " +
                "Increase LockExpiry or shorten the job to avoid duplicate execution windows.",
                JobName, duration.TotalMilliseconds, _options.LockExpiry.TotalMilliseconds);
        }
    }

    private async Task RunLeaderElectionLoopAsync(CancellationToken stoppingToken)
    {
        var consecutiveFailures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (await EvaluateEnabledAsync(stoppingToken).ConfigureAwait(false))
                {
                    await MaintainLeadershipAsync().ConfigureAwait(false);
                }
                else if (Volatile.Read(ref _isLeader))
                {
                    Logger.LogInformation(
                        "Job {JobName} disabled while leader. Releasing {LockKey} so an enabled replica can take over.",
                        JobName, _lockKey);
                    await ReleaseLockAsync().ConfigureAwait(false);
                }
                consecutiveFailures = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                consecutiveFailures++;
                Logger.LogError(ex, "Leader election error for {LockKey} (attempt {Attempt})", _lockKey, consecutiveFailures);
            }

            try
            {
                await Task.Delay(NextHeartbeatDelay(consecutiveFailures), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private TimeSpan NextHeartbeatDelay(int consecutiveFailures) =>
        ComputeHeartbeatDelay(_options, consecutiveFailures, IsLeader);

    // Static and pure so it can be unit tested without a Redis connection.
    internal static TimeSpan ComputeHeartbeatDelay(
        SingletonJobOptions options, int consecutiveFailures, bool isLeaderWithValidLease)
    {
        if (consecutiveFailures == 0) return options.HeartbeatInterval;

        // A leader whose lease is still valid must not back off: with the recommended settings
        // (LockExpiry >= 3 x HeartbeatInterval) two doubled delays already exceed LockExpiry, so backing
        // off would forfeit the lock on any two consecutive hiccups. Retry at the plain heartbeat cadence
        // to renew before the TTL lapses. Once the lease expires the node self-demotes and the follower
        // backoff below takes over.
        if (isLeaderWithValidLease) return options.HeartbeatInterval;

        // Exponential backoff: HeartbeatInterval * 2^failures, capped at MaxBackoffDelay.
        var shift = Math.Min(consecutiveFailures, 30);
        var scaledTicks = options.HeartbeatInterval.Ticks * (1L << shift);
        if (scaledTicks < 0 || scaledTicks > options.MaxBackoffDelay.Ticks)
            scaledTicks = options.MaxBackoffDelay.Ticks;
        var baseDelay = TimeSpan.FromTicks(scaledTicks);

        // ±20% jitter so peers don't reconnect in lockstep.
        var jitterFraction = Random.Shared.NextDouble() * 0.4 - 0.2;
        var jitterTicks = (long)(baseDelay.Ticks * jitterFraction);
        return TimeSpan.FromTicks(Math.Max(baseDelay.Ticks + jitterTicks, options.HeartbeatInterval.Ticks));
    }

    private async ValueTask<bool> EvaluateEnabledAsync(CancellationToken stoppingToken)
    {
        var previous = Volatile.Read(ref _isEnabled);
        bool enabled;
        try
        {
            enabled = await IsJobEnabledAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // A flaky flag backend should not flap leadership; keep the last known state.
            Logger.LogWarning(ex,
                "IsJobEnabledAsync threw for job {JobName}. Keeping previous state ({Enabled}).", JobName, previous);
            return previous;
        }

        if (enabled != previous)
        {
            Volatile.Write(ref _isEnabled, enabled);
            Logger.LogInformation("Job {JobName} is now {State}", JobName, enabled ? "ENABLED" : "DISABLED");
        }
        return enabled;
    }

    private async Task MaintainLeadershipAsync()
    {
        // Self-fence: if the lease window lapsed without a successful renewal, the key has expired on the
        // server and a peer may already own it. IsLeader already reports false; clear the raw flag too so
        // a successful SETNX below logs the "became LEADER" transition correctly.
        if (Volatile.Read(ref _isLeader) && Environment.TickCount64 >= Volatile.Read(ref _leaseValidUntil))
        {
            Volatile.Write(ref _isLeader, false);
            Logger.LogWarning(
                "Node {NodeId} could not renew {LockKey} within LockExpiry. Lease expired, demoting to follower.",
                _nodeId, _lockKey);
        }

        var leaseStart = Environment.TickCount64;
        var leaseMs = (long)_options.LockExpiry.TotalMilliseconds;

        var result = await _db!.ScriptEvaluateAsync(
            AcquireOrRenewScript,
            [_lockKey!],
            [_nodeId!, leaseMs]
        ).ConfigureAwait(false);

        var holdsLock = !result.IsNull && (long)result == 1;

        if (holdsLock)
        {
            Volatile.Write(ref _leaseValidUntil, leaseStart + leaseMs);
            if (!Volatile.Read(ref _isLeader))
            {
                Volatile.Write(ref _isLeader, true);
                Logger.LogInformation("Node {NodeId} became LEADER for {LockKey}", _nodeId, _lockKey);
            }
        }
        else if (Volatile.Read(ref _isLeader))
        {
            Volatile.Write(ref _isLeader, false);
            Logger.LogWarning("Node {NodeId} lost leadership for {LockKey}", _nodeId, _lockKey);
        }
    }

    private async Task ReleaseLockAsync()
    {
        if (!Volatile.Read(ref _isLeader) || _db is null) return;

        try
        {
            await _db.ScriptEvaluateAsync(
                ReleaseScript,
                [_lockKey!],
                [_nodeId!]
            ).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to release lock {LockKey} during shutdown.", _lockKey);
        }

        Volatile.Write(ref _isLeader, false);
        Logger.LogInformation("Leadership released for {LockKey}", _lockKey);
    }
}
