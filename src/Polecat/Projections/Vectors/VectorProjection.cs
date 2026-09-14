using System.Security.Cryptography;
using System.Text;
using JasperFx.Events;
using JasperFx.Events.Vectors;
using Polecat.Linq;

namespace Polecat.Projections.Vectors;

/// <summary>
///     Produces an embedding from an event stream and keeps it in step, so a vector search has
///     something to search (gh-628).
/// </summary>
/// <remarks>
///     <para>
///         Polecat could already SEARCH vectors and fuse that with keyword search; nothing produced the
///         embeddings. A subclass declares which events carry text through
///         <see cref="Configure" />; this assembles the content, calls the
///         <see cref="IEmbeddingProvider" /> once per page, and writes an ordinary document.
///     </para>
///     <para>
///         <b>It writes an ORDINARY DOCUMENT, not a table of its own</b>, which is what makes the result
///         searchable with nothing added: declare
///         <c>Schema.For&lt;TDoc&gt;().VectorIndex(x =&gt; x.Embedding, dimensions)</c> and the persisted
///         computed column follows. Conjoined tenancy, soft delete and the migration all apply without
///         this class knowing about them — which is also how gh-628's "conjoined tenant correlation"
///         is answered: the write goes through the session, so it carries the session's tenant.
///     </para>
///     <para>
///         <b>Ported in shape from Fisher's, and Fisher's own notes record four defects in Marten's
///         template that are NOT reproduced here.</b> Marten has open issues for all four
///         (marten#5421, #5424, #5422, #5420):
///     </para>
///     <list type="bullet">
///         <item>
///             It QUEUES ONTO THE SESSION rather than opening its own connection, so the embedding and
///             the shard's progression row commit in one transaction. Marten's writes on a separate
///             connection, so an embedding commits even when the events that caused it roll back.
///         </item>
///         <item>
///             <typeparamref name="TId" /> is open, not Guid. Marten's hardcodes Guid, so a
///             string-identified store cannot use it at all.
///         </item>
///         <item>
///             A delete addresses the row the MAP wrote — see
///             <see cref="VectorProjectionMap{TDoc,TId}.Delete{TEvent}" />.
///         </item>
///         <item>
///             A selector that throws is not swallowed — see
///             <c>VectorProjectionMap.TryContent</c>.
///         </item>
///     </list>
///     <para>
///         ⚠️ <b>Asynchronous only, and refused by name at store construction otherwise.</b> Embedding is
///         a metered network round trip, and an Inline projection runs inside the caller's
///         <c>SaveChangesAsync</c> — so an outage or a slow provider would stall every writer's
///         transaction. Fisher documents async-only and does not enforce it (fisher#287); this does.
///     </para>
/// </remarks>
public abstract class VectorProjection<TDoc, TId>: IProjection, IVectorProjection
    where TDoc : class, IVectorized<TId>, new()
    where TId : notnull
{
    private readonly VectorProjectionMap<TDoc, TId> _map = new();
    private readonly IEmbeddingProvider _provider;

    protected VectorProjection(IEmbeddingProvider provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));

        Configure(_map);

        if (_map.IsEmpty)
        {
            throw new InvalidOperationException(
                $"'{GetType().Name}' configured no event mappings, so it would read every page and "
                + "write nothing. Call map.Map<TEvent>(content, id) in Configure for at least one "
                + "event type.");
        }
    }

    string IVectorProjection.DescribeSelf() => GetType().Name;

    /// <summary>Declare the events that carry embedding content, and those that retract it.</summary>
    protected abstract void Configure(VectorProjectionMap<TDoc, TId> map);

    public async Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0) return;

        // Last write wins within a page, which is what makes a stream that edits the same content twice
        // cost one embedding rather than two. Deletes are kept in ORDER against the writes, so a
        // create-then-delete inside one page ends deleted -- the ordering defect marten#5422 reports,
        // where all deletes run before all upserts and the row survives.
        var pending = new Dictionary<TId, string?>();

        foreach (var @event in events)
        {
            if (_map.TryDelete(@event, out var deletedId))
            {
                pending[deletedId] = null;
                continue;
            }

            if (_map.TryContent(@event, out var id, out var content))
            {
                // A selector returning null means "this event carries no content", which is a real
                // answer and distinct from a selector that FAILED.
                if (content is null) continue;

                pending[id] = content;
            }
        }

        if (pending.Count == 0) return;

        foreach (var (id, _) in pending.Where(x => x.Value is null))
        {
            operations.Delete(new TDoc { Id = id });
        }

        var writes = pending.Where(x => x.Value is not null)
            .Select(x => (Id: x.Key, Content: x.Value!, Hash: Sha256(x.Value!)))
            .ToList();

        if (writes.Count == 0) return;

        var unchanged = await UnchangedAsync(operations, writes, cancellation).ConfigureAwait(false);
        var stale = writes.Where(x => !unchanged.Contains(x.Id)).ToList();

        if (stale.Count == 0) return;

        // One provider call for the batch, not one per row. Embedding is the metered part, and a page
        // of a hundred documents is a hundred round trips the other way.
        var vectors = await _provider
            .GenerateEmbeddingsAsync(stale.Select(x => x.Content).ToArray(), cancellation)
            .ConfigureAwait(false);

        if (vectors.Length != stale.Count)
        {
            throw new InvalidOperationException(
                $"{_provider.GetType().FullName} returned {vectors.Length} embeddings for "
                + $"{stale.Count} texts. The contract is one vector per input, in order — a provider "
                + "that drops or reorders them would pair every embedding with the wrong document from "
                + "that point on, silently.");
        }

        for (var i = 0; i < stale.Count; i++)
        {
            var (id, content, hash) = stale[i];

            operations.Store(new TDoc
            {
                Id = id, Content = content, ContentHash = hash, Embedding = vectors[i].ToArray()
            });
        }
    }

    /// <summary>
    ///     The ids whose stored hash already matches what this page would write — the ones that cost no
    ///     provider call.
    /// </summary>
    private static async Task<HashSet<TId>> UnchangedAsync(IDocumentSession operations,
        IReadOnlyList<(TId Id, string Content, string Hash)> writes, CancellationToken cancellation)
    {
        var byId = writes.ToDictionary(x => x.Id, x => x.Hash);
        var ids = byId.Keys.ToList();

        // Read through the SESSION, so the tenant filter and the soft-delete filter are the ones every
        // other query gets rather than a set this class composes for itself.
        var existing = await operations.Query<TDoc>()
            .Where(x => x.Id.IsOneOf(ids))
            .ToListAsync(cancellation)
            .ConfigureAwait(false);

        var unchanged = new HashSet<TId>();

        foreach (var document in existing)
        {
            if (document.ContentHash is { } hash
                && byId.TryGetValue(document.Id, out var incoming)
                && string.Equals(hash, incoming, StringComparison.Ordinal))
            {
                unchanged.Add(document.Id);
            }
        }

        return unchanged;
    }

    private static string Sha256(string content)
        => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(content)));
}
