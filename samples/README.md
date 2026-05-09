# SingletonJob samples

Three identical workers, one Redis. At any moment exactly one worker holds leadership and runs the jobs.

## Jobs in this sample

| Job              | Type      | Schedule                            |
|------------------|-----------|-------------------------------------|
| `HeartbeatJob`   | interval  | every 1 second (run, then wait)     |
| `PriceTickJob`   | fixed-rate| every 500 ms (skip if previous in flight) |
| `DailyReportJob` | cron      | `0 3 * * *` UTC (03:00 daily)       |

## Run with Docker (closest to k8s reality)

```sh
cd samples
docker compose up --build --scale worker=3
```

You will see exactly one of the three worker containers print `became LEADER` and start logging job ticks. The other two stay silent on jobs but still run their election loops.

To force a failover:
```sh
docker ps                          # find the leader's container id
docker kill <leader-container>     # SIGKILL, hard kill, no graceful release
```
Another worker prints `became LEADER` within `LockExpiry` (~10 s).

For graceful shutdown (SIGTERM, the k8s rolling-deploy case):
```sh
docker stop <leader-container>
```
Lock is released explicitly, so failover happens within `HeartbeatInterval` (~3 s).

## Run locally on Windows (no Docker)

Have Redis listening on `localhost:6379` (Memurai, WSL, or `docker run --rm -p 6379:6379 redis`).

```pwsh
.\run-3-instances.ps1
```

Three pwsh windows open. Close one to observe failover. Repeat to your heart's content.

## What to look for in logs

```
SingletonJob started: demo:heartbeat:lock. Node: my-host-a3f9c1d2
Node my-host-a3f9c1d2 became LEADER for demo:heartbeat:lock
[heartbeat] tick at 12:00:01.014
[heartbeat] tick at 12:00:02.018
...
Leadership released for demo:heartbeat:lock      <-- on graceful shutdown
```

Followers stay quiet on the job lines; they print only the `started` line and election errors (if any).
