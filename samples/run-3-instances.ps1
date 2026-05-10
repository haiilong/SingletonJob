#!/usr/bin/env powershell
# Launches 3 instances of the SingletonJob.Sample worker against a local Redis at localhost:6379.
# Each instance opens in its own PowerShell window so leadership transitions are visible.
# Stop one window to observe failover within ~3 seconds (HeartbeatInterval).

$ErrorActionPreference = 'Stop'
$proj = Join-Path $PSScriptRoot 'SingletonJob.Sample\SingletonJob.Sample.csproj'

if (-not (Test-Path $proj)) {
    Write-Error "Cannot find $proj"
    exit 1
}

# Prefer PowerShell 7+ (pwsh) if installed; fall back to Windows PowerShell 5.1 (powershell), which ships with Windows.
$shell = if (Get-Command pwsh -ErrorAction SilentlyContinue) { 'pwsh' } else { 'powershell' }

for ($i = 1; $i -le 3; $i++) {
    Start-Process $shell -ArgumentList @(
        '-NoExit',
        '-Command',
        "`$Host.UI.RawUI.WindowTitle='SingletonJob worker #$i'; dotnet run --project `"$proj`" -c Release"
    )
    Start-Sleep -Milliseconds 400
}

Write-Host "Three workers launched ($shell). Watch the windows for which one becomes leader."
Write-Host "Close one window to verify another takes over."
