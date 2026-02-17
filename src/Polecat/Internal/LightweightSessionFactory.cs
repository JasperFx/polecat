namespace Polecat.Internal;

/// <summary>
///     Session factory that creates lightweight sessions (no identity map).
/// </summary>
internal class LightweightSessionFactory : ISessionFactory
{
    private readonly IDocumentStore _store;

    public LightweightSessionFactory(IDocumentStore store)
    {
        _store = store;
    }

    public IDocumentSession OpenSession()
    {
        return _store.LightweightSession();
    }

    public IQuerySession QuerySession()
    {
        return _store.QuerySession();
    }
}
