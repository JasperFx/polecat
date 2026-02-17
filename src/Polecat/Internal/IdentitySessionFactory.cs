namespace Polecat.Internal;

/// <summary>
///     Session factory that creates identity map sessions.
/// </summary>
internal class IdentitySessionFactory : ISessionFactory
{
    private readonly IDocumentStore _store;

    public IdentitySessionFactory(IDocumentStore store)
    {
        _store = store;
    }

    public IDocumentSession OpenSession()
    {
        return _store.IdentitySession();
    }

    public IQuerySession QuerySession()
    {
        return _store.QuerySession();
    }
}
