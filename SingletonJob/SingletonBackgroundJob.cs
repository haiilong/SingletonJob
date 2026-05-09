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

    /// <summary>True when this node currently holds the Redis leadership lock.</summary>
    protected bool IsLeader => Volatile.Read(ref _isLeader);

    /// <summary>The configured options for this job (resolved on <see cref="StartAsync"/>).</summary>
    protected SingletonJobOptions Options => _options;

    // Atomic renew: extend lock TTL only if we still own it.
    private const string RenewScript =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";

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

        Logger.LogInformation("SingletonJob started: {LockKey}. Node: {NodeId}", _lockKey, _nodeId);
        return base.StartAsync(cancellationToken);
    }

    private static string ResolveNodeId(SingletonJobOptions options)
    {
        var basePart = options.NodeId
            ?? Environment.GetEnvironmentVariable("POD_NAME")
            ?? Environment.MachineName;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return basePart + "-" + suffix;
    }

    /// <inheritdoc />
    protected sealed override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var electionTask = RunLeaderElectionLoopAsync(stoppingToken);

        try
        {
            await ExecuteJobLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            // Wait for the heartbeat loop to drain so we don't release the lock and then race with a renewal.
            try { await electionTask.ConfigureAwait(false); } catch { /* swallow, already logged in loop */ }

            // Releasing on graceful shutdown enables fast failover: peers can SETNX immediately instead of
            // waiting up to LockExpiry. Important for k8s rolling deploys where SIGTERM is followed by quick replacement.
            await ReleaseLockAsync().ConfigureAwait(false);
        }
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
                await MaintainLeadershipAsync().ConfigureAwait(false);
                consecutiveFailures = 0;
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

    private TimeSpan NextHeartbeatDelay(int consecutiveFailures)
    {
        if (consecutiveFailures == 0) return _options.HeartbeatInterval;

        // Exponential backoff capped at MaxBackoffMultiplier × HeartbeatInterval.
        var multiplier = Math.Min(1 << Math.Min(consecutiveFailures, 30), _options.MaxBackoffMultiplier);
        var baseDelay = TimeSpan.FromTicks(_options.HeartbeatInterval.Ticks * multiplier);

        // ±20% jitter so peers don't reconnect in lockstep.
        var jitterFraction = Random.Shared.NextDouble() * 0.4 - 0.2;
        var jitterTicks = (long)(baseDelay.Ticks * jitterFraction);
        return TimeSpan.FromTicks(Math.Max(baseDelay.Ticks + jitterTicks, _options.HeartbeatInterval.Ticks));
    }

    private async Task MaintainLeadershipAsync()
    {
        // Attempt to acquire if we don't currently hold leadership.
        var acquired = await _db!.StringSetAsync(_lockKey!, _nodeId, _options.LockExpiry, When.NotExists).ConfigureAwait(false);

        if (acquired)
        {
            if (!Volatile.Read(ref _isLeader))
            {
                Volatile.Write(ref _isLeader, true);
                Logger.LogInformation("Node {NodeId} became LEADER for {LockKey}", _nodeId, _lockKey);
            }
            return;
        }

        // We didn't acquire. If we previously held the lock, try to renew.
        if (Volatile.Read(ref _isLeader))
        {
            var result = await _db.ScriptEvaluateAsync(
                RenewScript,
                [_lockKey!],
                [_nodeId!, (long)_options.LockExpiry.TotalMilliseconds]
            ).ConfigureAwait(false);

            if (!result.IsNull && (long)result == 0)
            {
                Volatile.Write(ref _isLeader, false);
                Logger.LogWarning("Node {NodeId} lost leadership for {LockKey}", _nodeId, _lockKey);
            }
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
