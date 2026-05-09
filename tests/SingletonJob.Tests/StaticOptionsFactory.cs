using Microsoft.Extensions.Options;

namespace SingletonJob.Tests;

internal sealed class StaticOptionsFactory<T>(T value) : IOptionsFactory<T>
    where T : class
{
    public T Create(string name) => value;
}
