# Redis topologies

The library uses `IConnectionMultiplexer` from StackExchange.Redis. Anything that interface supports works.

## Standalone

```
ConnectionStrings__Redis: "redis.example.com:6379"
```

Default. Single Redis instance. Failure of Redis = failure of leader election (every pod will become "leader-less" until Redis recovers, then one re-acquires).

## Sentinel

```
ConnectionStrings__Redis: "sentinel-1:26379,sentinel-2:26379,serviceName=mymaster"
```

StackExchange.Redis handles failover. The lock key follows the master.

## Cluster

The lock key is a single key, so no cross-slot operations are involved. Cluster works without changes:

```
ConnectionStrings__Redis: "node-1:6379,node-2:6379,node-3:6379"
```

If you run many job classes, all their lock keys land on different shards (good for load distribution). The library does not require any slot-affinity hinting.

## Memurai (Windows)

Memurai is a Redis-compatible service for Windows. Install it, leave the default `localhost:6379`, and the library works unmodified.

```pwsh
choco install memurai-developer
```

Then:

```
ConnectionStrings__Redis: "localhost:6379"
```

## Connection settings

Recommended `ConnectionMultiplexer` setup:

```csharp
var options = ConfigurationOptions.Parse(builder.Configuration.GetConnectionString("Redis")!);
options.AbortOnConnectFail = false;     // start even if Redis is briefly down
options.ConnectRetry = 3;
options.ConnectTimeout = 5000;
options.ReconnectRetryPolicy = new ExponentialRetry(1000);
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(options));
```

The library's own backoff loop will tolerate transient Redis errors regardless, but a healthy multiplexer config keeps log noise down.

Note: If you have your own Redis library or instance in your application, extract the `ConnectionMultiplexer` out and register it.

## Persistence is not required

The lock key carries no state worth persisting; it has a TTL of seconds. RDB/AOF settings on Redis don't matter for SingletonJob's correctness. If Redis loses the key, peers re-elect a leader on the next heartbeat.
