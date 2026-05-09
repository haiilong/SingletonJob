using SingletonJob;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

var redisConn = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConn));

// AOT-safe registration emitted by the bundled source generator.
builder.Services.AddSingletonJobsGenerated(builder.Configuration);

// Example per-job override:
builder.Services.PostConfigureSingletonJob("daily-report", o => o.LockExpiry = TimeSpan.FromMinutes(2));

builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

await builder.Build().RunAsync();
