using System.Reflection;
using JasperFx.Events;
using JasperFx.Events.Projections;
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
///         <b>The fold, the hashing, the unchanged-content skip and the batched model call are
///         <see cref="VectorEmbeddingPlan{TId}" />'s since #633</b>, not Polecat's. Every store wrote
///         that same body, and what is genuinely per-store is the two storage calls this class still
///         owns: read the stored hashes, and write the documents. The
///         <see cref="VectorProjectionMap{TId}" /> a subclass configures is likewise the shared one.
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
///         <b>Fisher's notes record four defects in Marten's template that are NOT reproduced here.</b>
///         Marten has open issues for all four (marten#5421, #5424, #5422, #5420):
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
///             <see cref="VectorProjectionMap{TId}.Delete{TEvent}" />.
///         </item>
///         <item>
///             A selector that throws is not swallowed — see <c>VectorProjectionMap.TryContent</c>.
///         </item>
///     </list>
///     <para>
///         ⚠️ <b>Asynchronous only, and refused by name at store construction otherwise.</b> Embedding is
///         a metered network round trip, and an Inline projection runs inside the caller's
///         <c>SaveChangesAsync</c> — so an outage or a slow provider would stall every writer's
///         transaction. Fisher documents async-only and does not enforce it (fisher#287); this does,
///         through <see cref="IValidatedProjection{T}" /> since #633 — jasperfx#845 made
///         <c>ProjectionGraph.AssertValidity</c> unwrap the <c>ProjectionWrapper</c> a bare
///         <see cref="IProjection" /> is registered through, so the interface is finally asked and
///         Polecat's private validation pass could go.
///     </para>
/// </remarks>
public abstract class VectorProjection<TDoc, TId>: IProjection, IValidatedProjection<StoreOptions>
    where TDoc : class, IVectorized<TId>, new()
    where TId : notnull
{
    private readonly VectorProjectionMap<TId> _map = new();
    private readonly IEmbeddingProvider _provider;
    private readonly MethodInfo? _aggregateStream;

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

        if (_map.AggregateType is not null)
        {
            _aggregateStream = ResolveAggregateStreamMethod(_map.AggregateType);
        }
    }

    /// <summary>Declare the events that carry embedding content, and those that retract it.</summary>
    protected abstract void Configure(VectorProjectionMap<TId> map);

    /// <summary>
    ///     The configuration rules this projection needs, asked by
    ///     <c>ProjectionGraph.AssertValidity</c> when the store is built.
    /// </summary>
    /// <remarks>
    ///     ⚠️ A bare <see cref="IProjection" /> is registered through a <c>ProjectionWrapper</c>, so
    ///     the lifecycle is not a member of this object at all — it is found by looking this instance
    ///     up among the registered sources. Before jasperfx#845 the wrapper was not unwrapped here and
    ///     a projection implementing this interface was never asked, which is why Polecat carried a
    ///     private <c>IVectorProjection</c> marker and a validation pass of its own until #633.
    /// </remarks>
    public IEnumerable<string> ValidateConfiguration(StoreOptions options)
    {
        var lifecycle = options.Projections.All
            .FirstOrDefault(x => x is IProjectionWrapper wrapper && ReferenceEquals(wrapper.InnerProjection, this))
            ?.Lifecycle;

        if (lifecycle is not null && lifecycle != ProjectionLifecycle.Async)
        {
            yield return
                $"'{GetType().Name}' is registered {lifecycle}, but a vector projection is "
                + "asynchronous only. Embedding is a metered network call, and Inline would put it "
                + "inside every caller's SaveChangesAsync where a slow or unavailable provider stalls "
                + "the transaction. Register it with ProjectionLifecycle.Async.";
        }

        if (_map.AggregateType is null) yield break;

        // MapFromAggregate is served by LIVE AGGREGATION of the stream the trigger names, so the
        // document id has to BE the stream id. Checked against the store's declared stream identity
        // rather than only against Guid/string, because a mismatch there aggregates nothing and the
        // embedding would silently never be written.
        var expected = options.Events.StreamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);

        if (typeof(TId) != expected)
        {
            yield return
                $"'{GetType().Name}' builds its content from '{_map.AggregateType.Name}', which Polecat "
                + $"serves by live-aggregating the stream the trigger names — so TId has to be the "
                + $"store's stream identity type, '{expected.Name}', and it is '{typeof(TId).Name}'. "
                + $"Either key the projection on the stream id, or map content from the events "
                + "themselves with map.Map<TEvent>(content, id).";
        }
    }

    public async Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0) return;

        // Last write per id wins within the page and a delete wins over everything before it, so a
        // stream that edits the same content twice costs one embedding rather than two and a
        // create-then-delete inside one page ends deleted -- the ordering defect marten#5422 reports,
        // where all deletes run before all upserts and the row survives.
        var plan = VectorEmbeddingPlan<TId>.Build(_map, events);

        if (plan.AggregateIds.Count > 0)
        {
            await ApplyAggregatesAsync(operations, events, plan, cancellation).ConfigureAwait(false);
        }

        foreach (var id in plan.Deletions)
        {
            operations.Delete(new TDoc { Id = id });
        }

        var writes = await plan
            .ResolveAsync(_provider, (ids, token) => StoredHashesAsync(operations, ids, token), cancellation)
            .ConfigureAwait(false);

        foreach (var write in writes)
        {
            operations.Store(new TDoc
            {
                Id = write.Id,
                Content = write.Content,
                ContentHash = write.ContentHash,
                Embedding = write.Embedding.ToArray()
            });
        }
    }

    /// <summary>
    ///     Build the content of every <see cref="VectorEmbeddingPlan{TId}.AggregateIds" /> entry from
    ///     the aggregate as it stands after this page's events.
    /// </summary>
    /// <remarks>
    ///     ⚠️ <b>Live aggregation up to the page's last event for that stream, NOT a snapshot read.</b>
    ///     The daemon does not order shards against each other, so reading an async snapshot could see
    ///     state another shard has not caught up to and build the embedding from the wrong text with
    ///     nothing reported. Bounding the aggregation at the version this page ends on also makes a
    ///     rebuild reproduce exactly what the original run wrote, which reading "current state" would
    ///     not.
    /// </remarks>
    private async Task ApplyAggregatesAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        VectorEmbeddingPlan<TId> plan, CancellationToken cancellation)
    {
        var versions = new Dictionary<TId, long>();

        foreach (var @event in events)
        {
            if (_map.TryAggregateTrigger(@event, out var id))
            {
                versions[id] = Math.Max(versions.TryGetValue(id, out var seen) ? seen : 0, @event.Version);
            }
        }

        foreach (var id in plan.AggregateIds)
        {
            var version = versions.TryGetValue(id, out var v) ? v : 0;

            var task = (Task)_aggregateStream!.Invoke(operations.Events,
                [id, version, null, null, 0L, cancellation])!;
            await task.ConfigureAwait(false);

            var aggregate = task.GetType().GetProperty("Result")!.GetValue(task);

            // A stream that aggregates to null is "nothing to index", the same answer a content
            // selector returning null gives.
            plan.ApplyAggregate(id, _map, aggregate);
        }
    }

    /// <summary>
    ///     The content hash currently stored for each of these ids — the store's half of
    ///     <see cref="VectorEmbeddingPlan{TId}.ResolveAsync" />, and the read that makes an unchanged
    ///     document cost no model call.
    /// </summary>
    /// <remarks>
    ///     Read through the SESSION, so the tenant filter and the soft-delete filter are the ones every
    ///     other query gets rather than a set this class composes for itself.
    /// </remarks>
    private static async Task<IReadOnlyDictionary<TId, string>> StoredHashesAsync(
        IDocumentSession operations, IReadOnlyList<TId> ids, CancellationToken cancellation)
    {
        var idList = ids.ToList();

        var existing = await operations.Query<TDoc>()
            .Where(x => x.Id.IsOneOf(idList))
            .ToListAsync(cancellation)
            .ConfigureAwait(false);

        return existing
            .Where(x => x.ContentHash is not null)
            .ToDictionary(x => x.Id, x => x.ContentHash!);
    }

    /// <summary>
    ///     The <c>AggregateStreamAsync&lt;TAggregate&gt;</c> overload whose stream-id parameter is
    ///     <typeparamref name="TId" />, bound once at construction rather than per page.
    /// </summary>
    private static MethodInfo ResolveAggregateStreamMethod(Type aggregateType)
    {
        if (typeof(TId) != typeof(Guid) && typeof(TId) != typeof(string))
        {
            throw new InvalidOperationException(
                $"A vector projection that maps content from '{aggregateType.Name}' is served by live "
                + $"aggregation of the stream the trigger names, so TId has to be Guid or string and "
                + $"it is '{typeof(TId).Name}'.");
        }

        if (!aggregateType.IsClass)
        {
            throw new InvalidOperationException(
                $"'{aggregateType.Name}' cannot be live-aggregated because it is not a reference type.");
        }

        return typeof(IQueryEventStore)
            .GetMethods()
            .Single(m => m.Name == nameof(IQueryEventStore.AggregateStreamAsync)
                         && m.IsGenericMethodDefinition
                         && m.GetParameters()[0].ParameterType == typeof(TId))
            .MakeGenericMethod(aggregateType);
    }
}
