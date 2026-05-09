#!/usr/bin/env pwsh
# Launches 3 instances of the SingletonJob.Sample worker against a local Redis at localhost:6379.
# Each instance opens in its own pwsh window so leadership transitions are visible.
# Stop one window to observe failover within ~3 seconds (HeartbeatInterval).

$ErrorActionPreference = 'Stop'
$proj = Join-Path $PSScriptRoot 'SingletonJob.Sample\SingletonJob.Sample.csproj'

if (-not (Test-Path $proj)) {
    Write-Error "Cannot find $proj"
    exit 1
}

for ($i = 1; $i -le 3; $i++) {
    Start-Process pwsh -ArgumentList @(
        '-NoExit',
        '-Command',
        "`$Host.UI.RawUI.WindowTitle='SingletonJob worker #$i'; dotnet run --project `"$proj`" -c Release"
    )
    Start-Sleep -Milliseconds 400
}

Write-Host "Three workers launched. Watch the windows for which one becomes leader."
Write-Host "Close one window to verify another takes over."
