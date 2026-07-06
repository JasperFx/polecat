using Weasel.Storage;

namespace Polecat.Internal;

/// <summary>
///     Session-scoped document version/revision bookkeeping behind the shared
///     <see cref="IVersionTracker" /> seam of the closed-shape storage runtime (#273).
///     Today Polecat carries versions on the documents themselves (<c>IVersioned</c> /
///     <c>IRevisioned</c>); the shared closed-shape selectors and storages record and consult
///     versions here instead, so this becomes the authoritative session store once Polecat's
///     document storage retargets onto the shared bases (phase E).
/// </summary>
internal class PolecatVersionTracker : IVersionTracker
{
    private readonly Dictionary<Type, object> _versions = new();
    private readonly Dictionary<Type, object> _revisions = new();

    public Dictionary<TId, Guid> ForType<TDoc, TId>() where TId : notnull
    {
        if (_versions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, Guid> existing)
        {
            return existing;
        }

        var fresh = new Dictionary<TId, Guid>();
        _versions[typeof(TDoc)] = fresh;
        return fresh;
    }

    public Dictionary<TId, long> RevisionsFor<TDoc, TId>() where TId : notnull
    {
        if (_revisions.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, long> existing)
        {
            return existing;
        }

        var fresh = new Dictionary<TId, long>();
        _revisions[typeof(TDoc)] = fresh;
        return fresh;
    }

    public Guid? VersionFor<TDoc, TId>(TId id) where TId : notnull
    {
        return ForType<TDoc, TId>().TryGetValue(id, out var version) ? version : null;
    }

    public long? RevisionFor<TDoc, TId>(TId id) where TId : notnull
    {
        return RevisionsFor<TDoc, TId>().TryGetValue(id, out var revision) ? revision : null;
    }

    public void StoreVersion<TDoc, TId>(TId id, Guid guid) where TId : notnull
    {
        ForType<TDoc, TId>()[id] = guid;
    }

    public void StoreRevision<TDoc, TId>(TId id, long revision) where TId : notnull
    {
        RevisionsFor<TDoc, TId>()[id] = revision;
    }

    public void ClearVersion<TDoc, TId>(TId id) where TId : notnull
    {
        ForType<TDoc, TId>().Remove(id);
    }

    public void ClearRevision<TDoc, TId>(TId id) where TId : notnull
    {
        RevisionsFor<TDoc, TId>().Remove(id);
    }
}
