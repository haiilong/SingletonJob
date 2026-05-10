# NativeAOT and trimming

The library is `IsAotCompatible=true` and `IsTrimmable=true`. The bundled Roslyn source generator (`SingletonJob.SourceGenerator`) emits the `AddSingletonJobs` extension method specific to your project, so DI registration does not rely on `Assembly.GetTypes()` reflection.

## Registration

```csharp
builder.Services.AddSingletonJobs(builder.Configuration);
```

The generator scans your compilation, finds every non-abstract subclass of `SingletonBackgroundJob`, and emits:

```csharp
internal static class SingletonJobGeneratedRegistration
{
    internal static IServiceCollection AddSingletonJobs(this IServiceCollection services, IConfiguration? configuration = null)
    {
        services.ConfigureSingletonJobOptions(configuration);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, global::MyApp.Jobs.HeartbeatJob>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, global::MyApp.Jobs.PriceTickJob>());
        return services;
    }
}
```

The class is `internal` so each consuming assembly gets its own copy.

The configuration binding inside `ConfigureSingletonJobOptions` reads strings from `IConfigurationSection["..."]` and parses with `TimeSpan.TryParse` / `int.TryParse`. No reflection, no `ConfigurationBinder.Bind`.

## Project setup for AOT-published apps

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
  <InvariantGlobalization>true</InvariantGlobalization>
</PropertyGroup>
```

Build:

```sh
dotnet publish -c Release
```

The library itself adds zero AOT incompatibilities.

## ProjectReference vs PackageReference

When you install via NuGet, the generator DLL is delivered automatically through the `analyzers/dotnet/cs` folder of the package. Compilers running on your project pick it up.

When you reference the library via `<ProjectReference>`, analyzers do **not** flow automatically. Reference the generator project explicitly:

```xml
<ProjectReference Include="..\path\to\SingletonJob.SourceGenerator.csproj"
                  OutputItemType="Analyzer"
                  ReferenceOutputAssembly="false" />
```

This is what the `samples/SingletonJob.Sample` project does.
