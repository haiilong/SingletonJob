using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
    /// this directly.
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
