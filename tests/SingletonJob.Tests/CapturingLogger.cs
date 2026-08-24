using Microsoft.Extensions.Logging;

namespace SingletonJob.Tests;

/// <summary>
/// Records what a job logged, so tests can assert on log output as behaviour. Used where the library
/// has no other observable side effect — the long-execution warning is advice to an operator, and the
/// log line is the whole feature.
/// </summary>
internal sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly Lock _sync = new();
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (_sync)
                return [.. _entries];
        }
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        lock (_sync)
            _entries.Add((logLevel, formatter(state, exception)));
    }

    public bool HasWarningContaining(string fragment)
        => Entries.Any(entry =>
            entry.Level == LogLevel.Warning
            && entry.Message.Contains(fragment, StringComparison.Ordinal));
}
