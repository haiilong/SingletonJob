using FluentAssertions;

namespace SingletonJob.Tests;

public class OptionsValidationTests
{
    [Fact]
    public void Defaults_are_valid()
    {
        var act = () => new SingletonJobOptions().GetType()
            .GetMethod("Validate", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .Invoke(new SingletonJobOptions(), null);
        act.Should().NotThrow();
    }

    [Fact]
    public void Empty_project_name_throws()
    {
        var opts = new SingletonJobOptions { ProjectName = "" };
        Invoking(opts).Should().Throw<InvalidOperationException>().WithMessage("*ProjectName*");
    }

    [Fact]
    public void Heartbeat_geq_lock_expiry_throws()
    {
        var opts = new SingletonJobOptions
        {
            HeartbeatInterval = TimeSpan.FromSeconds(10),
            LockExpiry = TimeSpan.FromSeconds(10),
        };
        Invoking(opts).Should().Throw<InvalidOperationException>().WithMessage("*LockExpiry*");
    }

    [Fact]
    public void Negative_heartbeat_throws()
    {
        var opts = new SingletonJobOptions { HeartbeatInterval = TimeSpan.Zero };
        Invoking(opts).Should().Throw<InvalidOperationException>().WithMessage("*HeartbeatInterval*");
    }

    private static Action Invoking(SingletonJobOptions opts) => () =>
    {
        try
        {
            typeof(SingletonJobOptions)
                .GetMethod("Validate", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(opts, null);
        }
        catch (System.Reflection.TargetInvocationException tie) when (tie.InnerException is not null)
        {
            throw tie.InnerException;
        }
    };
}
