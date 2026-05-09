using StackExchange.Redis;
using Testcontainers.Redis;

namespace SingletonJob.Tests;

// ReSharper disable once ClassNeverInstantiated.Global
public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    public async Task<IConnectionMultiplexer> ConnectAsync()
    {
        var options = ConfigurationOptions.Parse(_container.GetConnectionString());
        options.AbortOnConnectFail = false;
        return await ConnectionMultiplexer.ConnectAsync(options);
    }

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition(nameof(RedisCollection))]
public sealed class RedisCollection : ICollectionFixture<RedisFixture>;
