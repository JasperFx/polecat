namespace Polecat;

/// <summary>
///     Interface for seeding initial data into the document store on startup.
///     Implement this interface and register via StoreOptions.InitialData.
/// </summary>
public interface IInitialData
{
    Task Populate(IDocumentStore store, CancellationToken cancellation);
}

/// <summary>
///     Collection of IInitialData instances to be executed on startup.
/// </summary>
public class InitialDataCollection : List<IInitialData>
{
    /// <summary>
    ///     Add a simple lambda-based initial data populator.
    /// </summary>
    public void Add(Func<IDocumentStore, CancellationToken, Task> populate)
    {
        Add(new LambdaInitialData(populate));
    }

    private class LambdaInitialData : IInitialData
    {
        private readonly Func<IDocumentStore, CancellationToken, Task> _populate;

        public LambdaInitialData(Func<IDocumentStore, CancellationToken, Task> populate)
        {
            _populate = populate;
        }

        public Task Populate(IDocumentStore store, CancellationToken cancellation)
        {
            return _populate(store, cancellation);
        }
    }
}
