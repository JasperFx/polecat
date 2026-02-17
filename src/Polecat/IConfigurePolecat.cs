namespace Polecat;

/// <summary>
///     Implement this interface and register in DI to apply additional
///     configuration to StoreOptions after construction.
/// </summary>
public interface IConfigurePolecat
{
    void Configure(IServiceProvider serviceProvider, StoreOptions options);
}
