namespace Polecat.Internal;

/// <summary>
///     Default session factory that creates lightweight sessions.
/// </summary>
internal class DefaultSessionFactory : ISessionFactory
{
    private readonly IDocumentStore _store;

    public DefaultSessionFactory(IDocumentStore store)
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
