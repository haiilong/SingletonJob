using FluentAssertions;

namespace SingletonJob.Tests;

public class OptionsValidationTests
{
    [Fact]
    public void Defaults_throw_because_project_name_must_be_set()
    {
        // ProjectName has no default on purpose to avoid silent collisions across deployments that share a Redis instance.
        Invoking(new SingletonJobOptions())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*ProjectName*must be set*");
    }

    [Fact]
    public void Setting_only_project_name_is_valid()
    {
        var opts = new SingletonJobOptions { ProjectName = "myapp" };
        Invoking(opts).Should().NotThrow();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t")]
    public void Empty_or_whitespace_project_name_throws(string projectName)
    {
        var opts = new SingletonJobOptions { ProjectName = projectName };
        Invoking(opts).Should().Throw<InvalidOperationException>().WithMessage("*ProjectName*");
    }

    [Fact]
    public void Project_name_error_includes_actionable_guidance()
    {
        var opts = new SingletonJobOptions();
        Invoking(opts).Should().Throw<InvalidOperationException>()
            .Where(e =>
                e.Message.Contains("appsettings.json", StringComparison.Ordinal) &&
                e.Message.Contains("PostConfigure", StringComparison.Ordinal));
    }

    [Fact]
    public void Heartbeat_geq_lock_expiry_throws()
    {
        var opts = new SingletonJobOptions
        {
            ProjectName = "myapp",
            HeartbeatInterval = TimeSpan.FromSeconds(10),
            LockExpiry = TimeSpan.FromSeconds(10),
        };
        Invoking(opts).Should().Throw<InvalidOperationException>().WithMessage("*LockExpiry*");
    }

    [Fact]
    public void Negative_heartbeat_throws()
    {
        var opts = new SingletonJobOptions { ProjectName = "myapp", HeartbeatInterval = TimeSpan.Zero };
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
