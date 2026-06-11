using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace SingletonJob;

/// <summary>DI registration helpers.</summary>
/// <remarks>
/// Registration of jobs themselves is performed by the source-generated <c>AddSingletonJobs</c> extension
/// method. The helpers in this class configure the options machinery that <c>AddSingletonJobs</c> wires up.
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures the default <see cref="SingletonJobOptions"/> instance and registers a
    /// <see cref="OptionsServiceCollectionExtensions.ConfigureAll{TOptions}(IServiceCollection, Action{TOptions})"/>
    /// pass that applies the same configuration to every named options instance, so per-job overrides only
    /// need to specify the values they want to change.
    /// </summary>
    /// <remarks>
    /// Called automatically by the source-generated <c>AddSingletonJobs</c>. You normally do not need to call
    /// this directly. Also registers an <see cref="IValidateOptions{TOptions}"/> validator plus a small
    /// <see cref="IHostedService"/> that resolves <see cref="IOptions{TOptions}.Value"/> at startup, so
    /// configuration errors throw <see cref="OptionsValidationException"/> the moment the host starts rather
    /// than at the first job tick.
    /// </remarks>
    public static IServiceCollection ConfigureSingletonJobOptions(
        this IServiceCollection services,
        IConfiguration? configuration)
    {
        if (configuration is not null)
        {
            var section = configuration.GetSection(SingletonJobOptions.SectionName);
            // Use Action<T> overloads (AOT-safe) and bind manually rather than the reflection-based
            // ConfigurationBinder.Bind. Keeps the lib trimming- and NativeAOT-clean.
            services.Configure<SingletonJobOptions>(o => BindFromSection(section, o));
            services.ConfigureAll<SingletonJobOptions>(o => BindFromSection(section, o));
        }
        else
        {
            services.AddOptions<SingletonJobOptions>();
        }

        // IValidateOptions runs whenever IOptionsFactory.Create(name) is invoked. Each job triggers it for its
        // own named instance during StartAsync; the hosted service below triggers it for the default instance
        // at host start so misconfiguration fails before any IHostedService is started.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<SingletonJobOptions>, SingletonJobOptionsValidator>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SingletonJobOptionsValidationStartup>());

        return services;
    }

    private static void BindFromSection(IConfigurationSection section, SingletonJobOptions o)
    {
        var projectName = section["ProjectName"];
        if (!string.IsNullOrEmpty(projectName)) o.ProjectName = projectName;

        if (TimeSpan.TryParse(section["HeartbeatInterval"], out var hb)) o.HeartbeatInterval = hb;
        if (TimeSpan.TryParse(section["LockExpiry"], out var le)) o.LockExpiry = le;

        var nodeId = section["NodeId"];
        if (!string.IsNullOrEmpty(nodeId)) o.NodeId = nodeId;

        if (TimeSpan.TryParse(section["MaxBackoffDelay"], out var mbd)) o.MaxBackoffDelay = mbd;

        if (bool.TryParse(section["Enabled"], out var enabled)) o.Enabled = enabled;
    }

    /// <summary>
    /// Registers a per-job override that runs after the base configuration. Use this to tune a single job's
    /// <see cref="SingletonJobOptions.LockExpiry"/>, <see cref="SingletonJobOptions.HeartbeatInterval"/>, etc.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="jobName">The <see cref="SingletonBackgroundJob.JobName"/> of the job to override.</param>
    /// <param name="configure">Configuration delegate.</param>
    public static IServiceCollection PostConfigureSingletonJob(
        this IServiceCollection services,
        string jobName,
        Action<SingletonJobOptions> configure)
    {
        services.PostConfigure(jobName, configure);
        return services;
    }
}

/// <summary>
/// Wraps <see cref="SingletonJobOptions"/>.Validate so it participates in the standard
/// <see cref="IValidateOptions{TOptions}"/> pipeline. Invoked for every named instance the options factory
/// creates, so per-job overrides are validated too.
/// </summary>
internal sealed class SingletonJobOptionsValidator : IValidateOptions<SingletonJobOptions>
{
    public ValidateOptionsResult Validate(string? name, SingletonJobOptions options)
    {
        try
        {
            options.Validate();
            return ValidateOptionsResult.Success;
        }
        catch (InvalidOperationException ex)
        {
            return string.IsNullOrEmpty(name)
                ? ValidateOptionsResult.Fail(ex.Message)
                : ValidateOptionsResult.Fail($"[Job: {name}] {ex.Message}");
        }
    }
}

/// <summary>
/// Tiny hosted service that resolves <see cref="IOptions{TOptions}.Value"/> on
/// <see cref="IHostedService.StartAsync(CancellationToken)"/>. Touching <c>Value</c> triggers the registered
/// <see cref="IValidateOptions{TOptions}"/> implementations, so misconfiguration surfaces at host startup
/// rather than at the first job iteration.
/// </summary>
/// <remarks>
/// Implemented locally with just <c>Microsoft.Extensions.Hosting.Abstractions</c> so the library does not need
/// to take a dependency on the full <c>Microsoft.Extensions.Hosting</c> package solely for
/// <c>OptionsBuilder.ValidateOnStart()</c>.
/// </remarks>
internal sealed class SingletonJobOptionsValidationStartup(IOptions<SingletonJobOptions> options) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = options.Value;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
