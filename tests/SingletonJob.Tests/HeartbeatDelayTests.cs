using System.Reflection;
using FluentAssertions;

namespace SingletonJob.Tests;

public class HeartbeatDelayTests
{
    private static readonly SingletonJobOptions Options = new()
    {
        ProjectName = "test",
        HeartbeatInterval = TimeSpan.FromSeconds(3),
        LockExpiry = TimeSpan.FromSeconds(10),
        MaxBackoffDelay = TimeSpan.FromSeconds(30),
    };

    private static TimeSpan Compute(int failures, bool isLeaderWithValidLease) =>
        (TimeSpan)typeof(SingletonBackgroundJob)
            .GetMethod("ComputeHeartbeatDelay", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [Options, failures, isLeaderWithValidLease])!;

    [Fact]
    public void No_failures_returns_heartbeat_interval()
    {
        Compute(0, isLeaderWithValidLease: false).Should().Be(Options.HeartbeatInterval);
        Compute(0, isLeaderWithValidLease: true).Should().Be(Options.HeartbeatInterval);
    }

    [Fact]
    public void Leader_with_valid_lease_never_backs_off()
    {
        for (var failures = 1; failures <= 10; failures++)
        {
            Compute(failures, isLeaderWithValidLease: true).Should().Be(Options.HeartbeatInterval,
                "a leader must keep retrying at heartbeat cadence to renew before LockExpiry");
        }
    }

    [Fact]
    public void Follower_backs_off_exponentially_with_jitter()
    {
        // failures = 1: base delay 6s, jitter up to 20% in either direction, so [4.8s, 7.2s].
        for (var i = 0; i < 20; i++)
        {
            var delay = Compute(1, isLeaderWithValidLease: false);
            delay.Should().BeGreaterThanOrEqualTo(TimeSpan.FromSeconds(4.8));
            delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(7.2));
        }
    }

    [Fact]
    public void Follower_backoff_is_capped_at_max_backoff_delay()
    {
        for (var i = 0; i < 20; i++)
        {
            var delay = Compute(20, isLeaderWithValidLease: false);
            delay.Should().BeGreaterThanOrEqualTo(Options.MaxBackoffDelay * 0.8);
            delay.Should().BeLessThanOrEqualTo(Options.MaxBackoffDelay * 1.2);
        }
    }

    [Fact]
    public void Follower_delay_never_drops_below_heartbeat_interval()
    {
        for (var i = 0; i < 20; i++)
        {
            Compute(1, isLeaderWithValidLease: false)
                .Should().BeGreaterThanOrEqualTo(Options.HeartbeatInterval);
        }
    }
}
