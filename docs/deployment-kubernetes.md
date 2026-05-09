# Deploying on Kubernetes

## Pod template

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-worker
spec:
  replicas: 3
  selector:
    matchLabels: { app: my-worker }
  template:
    metadata:
      labels: { app: my-worker }
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: worker
          image: ghcr.io/me/my-worker:1.0.0
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: ConnectionStrings__Redis
              value: "redis-master.redis.svc.cluster.local:6379"
            - name: SingletonJob__ProjectName
              value: "my-worker-prod"
          resources:
            requests: { cpu: 50m, memory: 128Mi }
            limits:   { cpu: 500m, memory: 512Mi }
```

`POD_NAME` from the downward API makes leader logs immediately attributable: `Node my-worker-7d4-x8k9 became LEADER for my-worker-prod:heartbeat:lock`.

## SIGTERM behavior

Kubernetes sends `SIGTERM` to the pod, waits up to `terminationGracePeriodSeconds`, then sends `SIGKILL`. The .NET host translates `SIGTERM` into `IHostApplicationLifetime.StopAsync()`, which cancels every `BackgroundService`'s `stoppingToken`. Each `SingletonBackgroundJob` then:

1. Lets the in-flight job iteration finish (or be canceled by the token).
2. Awaits the leader-election loop to drain.
3. If we held leadership, runs the atomic release Lua. Peers can acquire within `HeartbeatInterval`.

So in steady state, a rolling deploy moves leadership in `~3 s` per replacement, not `~10 s` (the default `LockExpiry`).

If the pod is hard-killed (OOM, node failure, network partition), the lock simply expires after `LockExpiry`.

## Sizing

- One Redis call per `HeartbeatInterval` per job per pod (acquire or renew).
- 3 replicas × 5 jobs × 0.33 Hz ≈ 5 ops/sec. Negligible. A single Redis can handle thousands of jobs.
- Memory: each job holds a few KB. The lock key itself is ~50 bytes in Redis.

## Health probes

This library does not yet ship an `IHealthCheck`. Until it does, a simple liveness probe is fine:

```yaml
livenessProbe:
  exec:
    command: ["pgrep", "-f", "MyWorker"]
  periodSeconds: 30
```

A future v1.x will add a built-in `IHealthCheck` that reports unhealthy if the election loop has been failing for `> N` consecutive heartbeats. See the [project roadmap](../README.md#roadmap).

## Don't stack the deployment on the same node

If you want true HA, ensure replicas don't co-locate:

```yaml
spec:
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: kubernetes.io/hostname
      whenUnsatisfiable: ScheduleAnyway
      labelSelector:
        matchLabels: { app: my-worker }
```
