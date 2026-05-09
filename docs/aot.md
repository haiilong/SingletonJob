# NativeAOT and trimming

The library is `IsAotCompatible=true` and `IsTrimmable=true`. The bundled Roslyn source generator (`SingletonJob.SourceGenerator`) emits a registration extension method specific to your project, so DI registration does not rely on `Assembly.GetTypes()` reflection.

## Preferred path: source-generated registration

```csharp
builder.Services.AddSingletonJobsGenerated(builder.Configuration);
```

The generator scans your compilation, finds every non-abstract subclass of `SingletonBackgroundJob`, and emits:

```csharp
internal static class SingletonJobGeneratedRegistration
{
    internal static IServiceCollection AddSingletonJobsGenerated(this IServiceCollection services, IConfiguration? configuration = null)
    {
        services.ConfigureSingletonJobOptions(configuration);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, global::MyApp.Jobs.HeartbeatJob>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, global::MyApp.Jobs.PriceTickJob>());
        return services;
    }
}
```

The class is `internal` so each consuming assembly gets its own copy without colliding with the library's own (empty) emission.

The configuration binding inside `ConfigureSingletonJobOptions` reads strings from `IConfigurationSection["..."]` and parses with `TimeSpan.TryParse` / `int.TryParse`. No reflection, no `ConfigurationBinder.Bind`.

## Avoid: reflection-based registration

```csharp
builder.Services.AddSingletonJobs(builder.Configuration); // produces IL2026 + IL3050
```

This overload uses `Assembly.GetTypes()` and is annotated with `[RequiresUnreferencedCode]` and `[RequiresDynamicCode]`. The trim/AOT analyzer will warn at the call site so it's hard to ship by accident under `<PublishAot>true</PublishAot>` or `<PublishTrimmed>true</PublishTrimmed>`.

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

The library itself adds zero AOT incompatibilities; warnings only appear if you call the reflection registration path.

## What about ProjectReference vs PackageReference?

When you install via NuGet, the generator DLL is delivered automatically through the `analyzers/dotnet/cs` folder of the package. Compilers running on your project pick it up.

When you reference the library via `<ProjectReference>`, analyzers do **not** flow automatically. Reference the generator project explicitly:

```xml
<ProjectReference Include="..\path\to\SingletonJob.SourceGenerator.csproj"
                  OutputItemType="Analyzer"
                  ReferenceOutputAssembly="false" />
```

This is what the `samples/SingletonJob.Sample` project does.
