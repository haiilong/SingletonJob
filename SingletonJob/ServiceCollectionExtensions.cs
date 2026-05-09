using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace SingletonJob;

/// <summary>DI registration helpers.</summary>
public static class ServiceCollectionExtensions
{
    private const string ReflectionAotMessage =
        "AddSingletonJobs uses Assembly.GetTypes(); for trimming and NativeAOT use the source-generated AddSingletonJobsGenerated() instead.";

    /// <summary>
    /// Reflection-based registration: scans the given assembly (defaults to the calling assembly) for every
    /// non-abstract subclass of <see cref="SingletonBackgroundJob"/> and registers each as an
    /// <see cref="IHostedService"/>. Optionally binds <see cref="SingletonJobOptions"/> from
    /// <paramref name="configuration"/>.
    /// </summary>
    /// <remarks>
    /// You must register <see cref="StackExchange.Redis.IConnectionMultiplexer"/> separately. The library does
    /// not own the Redis connection lifetime.
    ///
    /// For trimming / NativeAOT use the source-generated <c>AddSingletonJobsGenerated()</c> overload, which
    /// the bundled Roslyn generator emits at compile time.
    /// </remarks>
    [RequiresUnreferencedCode(ReflectionAotMessage)]
    [RequiresDynamicCode(ReflectionAotMessage)]
    public static IServiceCollection AddSingletonJobs(
        this IServiceCollection services,
        IConfiguration? configuration = null,
        Assembly? assembly = null)
    {
        ConfigureSingletonJobOptionsCore(services, configuration);

        assembly ??= Assembly.GetCallingAssembly();

        var jobTypes = assembly.GetTypes()
            .Where(t => t is { IsClass: true, IsAbstract: false }
                        && typeof(SingletonBackgroundJob).IsAssignableFrom(t));

        foreach (var type in jobTypes)
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton(typeof(IHostedService), type));
        }

        return services;
    }

    /// <summary>
    /// Internal helper used by both the reflection registration and the source generator output. Configures
    /// the default options instance and registers a <see cref="OptionsServiceCollectionExtensions.ConfigureAll"/>
    /// pass that applies the same configuration to every named options instance, so per-job overrides only
    /// need to specify the values they want to change.
    /// </summary>
    public static IServiceCollection ConfigureSingletonJobOptions(
        this IServiceCollection services,
        IConfiguration? configuration)
    {
        ConfigureSingletonJobOptionsCore(services, configuration);
        return services;
    }

    private static void ConfigureSingletonJobOptionsCore(IServiceCollection services, IConfiguration? configuration)
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
    }

    private static void BindFromSection(IConfigurationSection section, SingletonJobOptions o)
    {
        var projectName = section["ProjectName"];
        if (!string.IsNullOrEmpty(projectName)) o.ProjectName = projectName;

        if (TimeSpan.TryParse(section["HeartbeatInterval"], out var hb)) o.HeartbeatInterval = hb;
        if (TimeSpan.TryParse(section["LockExpiry"], out var le)) o.LockExpiry = le;

        var nodeId = section["NodeId"];
        if (!string.IsNullOrEmpty(nodeId)) o.NodeId = nodeId;

        if (int.TryParse(section["MaxBackoffMultiplier"], out var mb)) o.MaxBackoffMultiplier = mb;
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
