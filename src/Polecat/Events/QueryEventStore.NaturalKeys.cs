using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Polecat.Events.Fetching;
using Polecat.Internal.Batching;
using Polecat.Projections;
using Weasel.Storage;

namespace Polecat.Events;

/// <summary>
///     #1131 / #1050 (marten#6150): reading an aggregate by one of its natural keys, owned or mapped, and the
///     routing that decides whether a <c>TId</c> is a natural key, a strong-typed stream id or the stream id
///     itself. On the read side so that a query session, and a batched query over one, can read by key too.
/// </summary>
internal partial class QueryEventStore
{
    // ---- routing: natural key, strong-typed stream id, or the stream id itself ----

    /// <summary>
    ///     The fetch plan for <typeparamref name="T" /> by <typeparamref name="TId" />, when
    ///     <typeparamref name="TId" /> is one of <typeparamref name="T" />'s natural keys. Null when it is the
    ///     stream identity, or something that wraps one.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The store's own stream identity type always means the stream: <c>Guid</c> on a Guid store,
    ///         <c>string</c> on a string store. A natural key of that type is refused when the store is built
    ///         (<see cref="Projections.PolecatProjectionOptions.AssertNaturalKeysAreNotStreamIdentities" />), so the
    ///         two can't be confused. A <c>string</c> key on a Guid store is a natural key.
    ///     </para>
    ///     <para>
    ///         <c>TKey?</c> is refused rather than quietly treated as <c>TKey</c>, as Marten does: a null key can
    ///         never match a stream, and the caller already knows when it has one. The <c>notnull</c> constraint
    ///         only warns for <c>Nullable&lt;T&gt;</c>.
    ///     </para>
    /// </remarks>
    internal NaturalKeyFetchPlan<T>? NaturalKeyPlanFor<T, TId>() where T : class where TId : notnull
    {
        if (Nullable.GetUnderlyingType(typeof(TId)) is { } underlying)
        {
            throw new ArgumentException(
                $"{typeof(T).FullNameInCode()} cannot be fetched by {typeof(TId).FullNameInCode()}. Fetch by {underlying.FullNameInCode()} instead, and handle a null value before fetching.");
        }

        if (typeof(TId) == streamIdentityType) return null;

        return _events.NaturalKeyPlans.GetOrAdd((typeof(T), typeof(TId)), _ => buildNaturalKeyPlan<T, TId>())
            as NaturalKeyFetchPlan<T>;
    }

    protected Type streamIdentityType => _events.StreamIdentity == StreamIdentity.AsGuid ? typeof(Guid) : typeof(string);

    private object? buildNaturalKeyPlan<T, TId>() where T : class where TId : notnull
    {
        var projections = _options.Projections;

        if (!projections.TryFindAggregate(typeof(T), out var projection))
        {
            if (!tryAutoRegisterNaturalKeyProjection<T, TId>(out projection)) return null;
        }

        var key = FindNaturalKey(projection, typeof(TId));
        return key == null ? null : new NaturalKeyFetchPlan<T>(_events, key);
    }

    /// <summary>
    ///     The aggregate's owned or mapped natural key of the given type. Keys on one aggregate have distinct
    ///     types, so there is at most one.
    /// </summary>
    internal static NaturalKeyDefinition? FindNaturalKey(IAggregateProjection projection, Type keyType)
    {
        // A definition's OuterType is never Nullable<T>, and fetches refuse TKey? before getting here
        if (projection.NaturalKeyDefinition?.OuterType == keyType) return projection.NaturalKeyDefinition;

        return projection.MappedNaturalKeyDefinitions.FirstOrDefault(x => x.OuterType == keyType);
    }

    /// <summary>
    ///     An aggregate fetched by the type of a natural key it owns, with no projection registered for it, gets an
    ///     Inline one, so that the lookup is kept. Only an OWNED key does this (#1131): a <c>[MappedNaturalKey]</c>
    ///     reads its source's lookup, and an aggregate with only mapped keys has no lookup of its own to keep.
    ///     Before, any <c>[NaturalKey]</c> did, including a mapped one, which then registered an Inline projection
    ///     that owned no key, beside the one already registered.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2090",
        Justification = "Aggregate types reaching the fetch API are preserved at the projection-registration boundary.")]
    private bool tryAutoRegisterNaturalKeyProjection<T, TId>([NotNullWhen(true)] out IAggregateProjection? projection)
        where T : class where TId : notnull
    {
        projection = null;

        var property = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(p => p.GetCustomAttribute<NaturalKeyAttribute>() is { SourceProjection: null });

        // A TKey? property is the TKey key
        if (property == null || NaturalKeyDefinition.KeyTypeOf(property.PropertyType) != typeof(TId)) return false;

        lock (_events.NaturalKeyPlans)
        {
            var projections = _options.Projections;
            if (projections.TryFindAggregate(typeof(T), out projection)) return true;

            var single = new SingleStreamProjection<T, Guid>();
            single.Lifecycle = ProjectionLifecycle.Inline;
            single.AssembleAndAssertValidity();

            foreach (var eventType in single.IncludedEventTypes)
            {
                _events.AddEventType(eventType);
            }

            projections.AddInlineProjectionAfterTheStoreWasBuilt(
                (IProjectionSource<IDocumentSession, IQuerySession>)single);
            _session.ForgetEventStoreSchema();
            projection = single;
            return true;
        }
    }

    /// <summary>
    ///     #417 / marten#5144: a strong-typed identifier such as <c>PaymentId(Guid)</c> IS the stream identity, just
    ///     wrapped. Null when <typeparamref name="TId" /> neither is nor wraps the type that identifies this store's
    ///     streams: a wrapper around a string is not a stream key on a Guid store, so an aggregate fetched by it,
    ///     which has no natural key of that type, is refused rather than looked up by the wrong column.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "A strong-typed stream id flows in from the caller's fetch and is preserved by the aggregate's registration, as on the stream-id paths.")]
    [UnconditionalSuppressMessage("Trimming", "IL2087",
        Justification = "A strong-typed stream id flows in from the caller's fetch and is preserved by the aggregate's registration, as on the stream-id paths.")]
    internal object? UnwrapStreamIdentity<TId>(TId id) where TId : notnull
    {
        // The store's by-id fetch says so when a bare Guid or string is the wrong one
        if (id is Guid or string) return id;

        ValueTypeInfo info;
        try
        {
            info = ValueTypeInfo.ForType(typeof(TId));
        }
        catch (InvalidValueTypeException)
        {
            return null;
        }

        if (info.SimpleType != streamIdentityType) return null;

        return info.SimpleType == typeof(Guid)
            ? info.UnWrapper<TId, Guid>()(id)
            : info.UnWrapper<TId, string>()(id);
    }

    internal InvalidOperationException NoNaturalKey<T, TId>() where T : class
        // #369 / jasperfx#571: NaturalKeyFor() is the real escape hatch, see NaturalKeyBuilder<TDoc>
        => new($"No natural key of type {typeof(TId).FullNameInCode()} found for aggregate type '{typeof(T).Name}', and {typeof(TId).Name} does not identify a stream either. " +
               $"Mark the key property on {typeof(T).Name} with [NaturalKey] and annotate the event handler(s) " +
               "that set it with [NaturalKeySource], or register the mapping explicitly from the projection with " +
               "NaturalKeyFor(x => x.SetBy<TEvent>(e => ...)). A key whose lookup another aggregate keeps is declared " +
               "with [MappedNaturalKey<TSource>].");

    internal async Task<NaturalKeyFetchResult<T>> runNaturalKeyFetchAsync<T>(NaturalKeyFetchPlan<T> plan, object id,
        object keyValue, NaturalKeyFetchShape shape, CancellationToken cancellation) where T : class
    {
        await using var batch = new Microsoft.Data.SqlClient.SqlBatch();
        var builder = new Weasel.SqlServer.BatchBuilder(batch);
        plan.WriteFetch(builder, keyValue, TenantId, shape);
        builder.Compile();

        try
        {
            await using var reader = await _session.ExecuteReaderAsync(batch, cancellation).ConfigureAwait(false);
            var storageSession = _session;
            return await plan.ReadFetchAsync(reader, _session.Serializer, storageSession, TenantId, shape,
                cancellation).ConfigureAwait(false);
        }
        // #652, as FetchForExclusiveWriting by stream id: only the locked read can be a lock failure
        catch (Exception e) when (shape.ForUpdate && EventOperations.IsLockFailure(e))
        {
            throw new StreamLockedException(id, EventOperations.FindLockFailure(e));
        }
    }

    // ---- reading ----

    /// <summary>The tenant this store reads as, for a batched read composed outside it.</summary>
    internal string ReadTenantId => TenantId;

    /// <summary>
    ///     A batched <c>FetchLatest</c> by stream id answers from the session's aggregate identity map, as the
    ///     standalone one does.
    /// </summary>
    internal bool TryFindStreamInAggregateIdentityMap<T>(object streamId, [NotNullWhen(true)] out T? aggregate)
        where T : class
    {
        aggregate = null;
        if (!UsesAggregateIdentityMap) return false;

        return streamId is Guid id
            ? _session.TryGetAggregateFromIdentityMap(id, out aggregate) && aggregate != null
            : _session.TryGetAggregateFromIdentityMap((string)streamId, out aggregate) && aggregate != null;
    }

    public async ValueTask<T?> FetchLatest<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class where TId : notnull
    {
        if (NaturalKeyPlanFor<T, TId>() is { } plan)
        {
            return await fetchLatestByNaturalKeyAsync(plan, id, cancellation).ConfigureAwait(false);
        }

        return UnwrapStreamIdentity(id) switch
        {
            Guid streamId => await FetchLatest<T>(streamId, cancellation).ConfigureAwait(false),
            string streamKey => await FetchLatest<T>(streamKey, cancellation).ConfigureAwait(false),
            _ => throw NoNaturalKey<T, TId>()
        };
    }

    /// <summary>
    ///     A null key, or a strong-typed key whose value is null, finds nothing without a query. With
    ///     <c>UseIdentityMapForAggregates</c>, an aggregate this session already holds with the key is the answer,
    ///     as <c>FetchLatest</c> by stream id answers from the session.
    /// </summary>
    internal async ValueTask<T?> fetchLatestByNaturalKeyAsync<T>(NaturalKeyFetchPlan<T> plan, object id,
        CancellationToken cancellation) where T : class
    {
        var keyValue = plan.KeyValueFor(id);
        if (keyValue == null) return null;

        if (TryFindInAggregateIdentityMap(plan, keyValue, out var inMap)) return inMap;

        await _session.EnsureEventStoreSchemaAsync(cancellation).ConfigureAwait(false);

        var shape = NaturalKeyReadShape<T>();
        if (shape.Document != null)
        {
            await _session.EnsureDocumentTableAsync(typeof(T), cancellation).ConfigureAwait(false);
        }

        var result = await runNaturalKeyFetchAsync(plan, id, keyValue, shape, cancellation).ConfigureAwait(false);
        return await CompleteFetchLatestAsync(result, shape, cancellation).ConfigureAwait(false);
    }

    /// <summary>
    ///     #463, as <c>FetchLatest</c> by stream id: an Inline aggregate whose document can be loaded by the stream
    ///     id is read from that document, and any other is aggregated from its events.
    /// </summary>
    internal NaturalKeyFetchShape NaturalKeyReadShape<T>() where T : class
        => CanReadInlineDocument<T>(streamIdentityType)
            ? new NaturalKeyFetchShape(false, false, _session.BatchLoadStorageFor<T>())
            : new NaturalKeyFetchShape(false, true, null);

    internal async Task<T?> CompleteFetchLatestAsync<T>(NaturalKeyFetchResult<T> result, NaturalKeyFetchShape shape,
        CancellationToken cancellation) where T : class
    {
        if (!result.Found) return null;
        if (shape.Document != null) return result.Document;
        if (result.Events.Count == 0) return null;

        var aggregator = _options.Projections.AggregatorFor<T>();
        var aggregate = await aggregator.BuildAsync(result.Events, _session, null, cancellation).ConfigureAwait(false);
        if (aggregate != null) TrySetIdentity(aggregate, result.StreamId);

        return aggregate;
    }

    /// <summary>
    ///     The aggregate for this key in the session's aggregate identity map, matched by its CURRENT key value
    ///     rather than registered under the key, so that whatever removes the entry by stream id also removes it
    ///     here.
    /// </summary>
    internal bool TryFindInAggregateIdentityMap<T>(NaturalKeyFetchPlan<T> plan, object keyValue,
        [NotNullWhen(true)] out T? aggregate) where T : class
    {
        aggregate = null;
        if (!_events.UseIdentityMapForAggregates || !UsesAggregateIdentityMap) return false;

        if (!_session.AggregateIdentityMap.TryGetValue(typeof(T), out var raw)
            || raw is not System.Collections.IDictionary map)
        {
            return false;
        }

        foreach (var candidate in map.Values)
        {
            if (candidate is T typed && plan.HasKey(typed, keyValue))
            {
                aggregate = typed;
                return true;
            }
        }

        return false;
    }

}
