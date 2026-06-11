using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

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

    [Fact]
    public void Resolving_IOptionsValue_runs_IValidateOptions_and_throws_OptionsValidationException()
    {
        // ConfigureSingletonJobOptions wires up an IValidateOptions<SingletonJobOptions>. Resolving Value
        // triggers it. With no ProjectName set, this is the surfaced exception path users will hit.
        var services = new ServiceCollection().ConfigureSingletonJobOptions(configuration: null);
        using var sp = services.BuildServiceProvider();

        var act = () => sp.GetRequiredService<IOptions<SingletonJobOptions>>().Value;
        act.Should().Throw<OptionsValidationException>().WithMessage("*ProjectName*");
    }

    [Fact]
    public async Task Startup_hosted_service_validates_at_host_start_before_any_job_ticks()
    {
        // The hosted service registered by ConfigureSingletonJobOptions resolves IOptions<T>.Value at StartAsync.
        // Failing here means a misconfigured host never reaches the first job iteration.
        var services = new ServiceCollection().ConfigureSingletonJobOptions(configuration: null);
        using var sp = services.BuildServiceProvider();

        var startup = sp.GetServices<IHostedService>().Single();

        var act = async () => await startup.StartAsync(CancellationToken.None);
        await act.Should().ThrowAsync<OptionsValidationException>().WithMessage("*ProjectName*");
    }

    [Fact]
    public async Task Startup_hosted_service_passes_when_options_are_valid()
    {
        var services = new ServiceCollection()
            .ConfigureSingletonJobOptions(configuration: null);
        services.PostConfigure<SingletonJobOptions>(o => o.ProjectName = "myapp");
        using var sp = services.BuildServiceProvider();

        var startup = sp.GetServices<IHostedService>().Single();

        await startup.StartAsync(CancellationToken.None);
        // no throw
    }

    [Fact]
    public void Per_job_override_with_empty_project_name_fails_validation_with_job_name_prefix()
    {
        var services = new ServiceCollection()
            .ConfigureSingletonJobOptions(configuration: null);
        services.PostConfigure<SingletonJobOptions>(o => o.ProjectName = "myapp");
        services.PostConfigureSingletonJob("broken-job", o => o.ProjectName = "");
        using var sp = services.BuildServiceProvider();

        var factory = sp.GetRequiredService<IOptionsFactory<SingletonJobOptions>>();

        var act = () => factory.Create("broken-job");
        act.Should().Throw<OptionsValidationException>().WithMessage("*broken-job*ProjectName*");
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
