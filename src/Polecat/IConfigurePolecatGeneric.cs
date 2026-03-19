namespace Polecat;

/// <summary>
///     Implement this interface and register in DI to apply additional
///     configuration to StoreOptions for a specific document store type
///     after construction. Used with AddPolecatStore&lt;T&gt;().
/// </summary>
/// <typeparam name="T">The marker type for the document store</typeparam>
public interface IConfigurePolecat<T> where T : IDocumentStore
{
    void Configure(IServiceProvider serviceProvider, StoreOptions options);
}
