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
///     #1131 / #1050 (marten#6150): fetching an aggregate by one of its natural keys, owned or mapped, and the
///     generic <c>&lt;T, TId&gt;</c> overloads that decide whether a <typeparamref name="TId" /> is a natural key,
///     a strong-typed stream id or the stream id itself.
/// </summary>
internal partial class EventOperations
{
    // ---- writing ----

    public async Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class where TId : notnull
    {
        if (NaturalKeyPlanFor<T, TId>() is { } plan)
        {
            return await fetchForWritingByNaturalKeyAsync(plan, id, false, null, cancellation).ConfigureAwait(false);
        }

        return UnwrapStreamIdentity(id) switch
        {
            Guid streamId => await Writes.FetchForWriting<T>(streamId, cancellation).ConfigureAwait(false),
            string streamKey => await Writes.FetchForWriting<T>(streamKey, cancellation).ConfigureAwait(false),
            _ => throw NoNaturalKey<T, TId>()
        };
    }

    /// <summary>
    ///     jasperfx#1281: by natural key, strong-typed stream id or stream id. A stale version fails before the
    ///     session is touched: no stream action, no write-cache claim and no identity map entry is left behind.
    /// </summary>
    public async Task<IEventStream<T>> FetchForWriting<T, TId>(TId id, long expectedVersion,
        CancellationToken cancellation = default) where T : class where TId : notnull
    {
        if (NaturalKeyPlanFor<T, TId>() is { } plan)
        {
            return await fetchForWritingByNaturalKeyAsync(plan, id, false, expectedVersion, cancellation)
                .ConfigureAwait(false);
        }

        return UnwrapStreamIdentity(id) switch
        {
            Guid streamId => await Writes.FetchForWriting<T>(streamId, expectedVersion, cancellation).ConfigureAwait(false),
            string streamKey => await Writes.FetchForWriting<T>(streamKey, expectedVersion, cancellation).ConfigureAwait(false),
            _ => throw NoNaturalKey<T, TId>()
        };
    }

    public async Task<IEventStream<T>> FetchForExclusiveWriting<T, TId>(TId id, CancellationToken cancellation = default)
        where T : class where TId : notnull
    {
        if (NaturalKeyPlanFor<T, TId>() is { } plan)
        {
            return await fetchForWritingByNaturalKeyAsync(plan, id, true, null, cancellation).ConfigureAwait(false);
        }

        return UnwrapStreamIdentity(id) switch
        {
            Guid streamId => await Writes.FetchForExclusiveWriting<T>(streamId, cancellation).ConfigureAwait(false),
            string streamKey => await Writes.FetchForExclusiveWriting<T>(streamKey, cancellation).ConfigureAwait(false),
            _ => throw NoNaturalKey<T, TId>()
        };
    }

    /// <summary>
    ///     A null key is refused the way an empty Guid or a null string stream id is: there is no stream to load,
    ///     and no key for a new stream to claim. That includes a strong-typed key whose value is null, which is
    ///     what <c>default</c> of a struct wrapping a string is.
    /// </summary>
    internal static object KeyForWriting<T>(NaturalKeyFetchPlan<T> plan, object? id) where T : class
        => plan.KeyValueFor(id) ?? throw new ArgumentOutOfRangeException(nameof(id),
            $"Cannot use a null natural key of type {plan.Key.OuterType.FullNameInCode()} to fetch {typeof(T).FullNameInCode()} for writing. A null key can't match a stream, so check for a missing key before fetching");

    private async Task<IEventStream<T>> fetchForWritingByNaturalKeyAsync<T>(NaturalKeyFetchPlan<T> plan, object id,
        bool exclusive, long? expectedVersion, CancellationToken cancellation) where T : class
    {
        var keyValue = KeyForWriting(plan, id);

        // #1139: the lookup table is part of the event store schema, which is created on first use (#219)
        await _sessionBase.EnsureEventStoreSchemaAsync(cancellation).ConfigureAwait(false);
        await _sessionBase.ProvisionEventTenantBeforeHoldingLocksAsync(_tenantId, exclusive, cancellation).ConfigureAwait(false);

        if (exclusive)
        {
            await _sessionBase.BeginTransactionAsync(cancellation).ConfigureAwait(false);
        }

        // An aggregate in the write cache reads its events once its baseline is known, as FetchForWriting by
        // stream id does, so its events are a second round trip. Any other reads them in the same command.
        var cached = _events.AggregateWriteCaching.IsEnabled(typeof(T));
        var shape = new NaturalKeyFetchShape(exclusive, !cached, null);

        for (var attempt = 1; ; attempt++)
        {
            var result = await runNaturalKeyFetchAsync(plan, id, keyValue, shape, cancellation).ConfigureAwait(false);

            if (result.Found && !result.StillNamed)
            {
                // The key changed hands between being resolved and the stream being locked. What was locked is
                // not the stream the key names now, so the fetch is redone, still inside the transaction.
                if (attempt < MaxFetchAttempts) continue;
                throw NaturalKeyMoved<T>(keyValue);
            }

            return await completeFetchForWritingAsync(plan, id, result, cached, expectedVersion, cancellation)
                .ConfigureAwait(false);
        }
    }

    internal static ConcurrencyException NaturalKeyMoved<T>(object keyValue) where T : class
        => new($"The natural key '{keyValue}' of {typeof(T).FullNameInCode()} moved to another stream while it was being fetched for writing. Fetch it again.",
            typeof(T), keyValue);

    /// <summary>
    ///     Turns what one fetch by natural key read into the stream handle, the way <c>FetchForWriting</c> by
    ///     stream id does. Shared by the standalone fetch and its batched form.
    /// </summary>
    /// <param name="readFromCache">
    ///     The events have not been read yet, because they are read from the write cache's baseline, in a second
    ///     round trip. Only the standalone fetch does this.
    /// </param>
    internal async Task<IEventStream<T>> completeFetchForWritingAsync<T>(NaturalKeyFetchPlan<T> plan, object id,
        NaturalKeyFetchResult<T> result, bool readFromCache, long? expectedVersion, CancellationToken cancellation)
        where T : class
    {
        if (!result.Found)
        {
            // No live stream has this key, so this is a new stream (marten#6150). Its key is claimed when the
            // projection that owns the key sees the first event that sets it. A stream id that names no stream is
            // a new stream with that id.
            if (expectedVersion is { } expected && expected != 0)
            {
                throw new EventStreamUnexpectedMaxEventIdException(id, typeof(T), expected, 0);
            }

            // A fetch by stream id starts the stream it was asked for, as FetchForWriting by stream id does
            object streamId = plan.IsByStreamId ? id
                : _events.StreamIdentity == StreamIdentity.AsGuid
                    ? _events.StreamIdGenerator.NewGuid()
                    : _events.StreamIdGenerator.NewGuid().ToString();

            return trackForWriting<T>(streamId, 0, false, null, cancellation);
        }

        var version = result.Version;

        // Checked before anything below touches the session, so a stale version leaves it as it was
        assertExpectedNaturalKeyVersion<T>(id, expectedVersion, reachedVersion(version, result.Events));

        T? aggregate;
        if (readFromCache)
        {
            (aggregate, version) = await buildForWritingAsync<T>(result.StreamId, version, false, cancellation)
                .ConfigureAwait(false);
            assertExpectedNaturalKeyVersion<T>(id, expectedVersion, version);
        }
        else
        {
            // The write-cache claim is still made, which clears an entry the stream has moved past
            var baseline = takeWriteBaseline<T>(result.StreamId, version, false);
            (aggregate, version) = await foldForWritingAsync(result.StreamId, version, result.Events,
                baseline.WithoutAggregate(), cancellation).ConfigureAwait(false);
        }

        cacheAggregateInIdentityMap<T>(result.StreamId, aggregate);

        return trackForWriting<T>(result.StreamId, version, true, aggregate, cancellation);
    }

    /// <summary>
    ///     polecat#1049: the events read is a separate statement from the version read, so an append committed
    ///     between them is in it, and the fetch answers at the version it actually reached.
    /// </summary>
    private static long reachedVersion(long version, IReadOnlyList<IEvent> events)
    {
        foreach (var @event in events)
        {
            if (@event.Version > version) version = @event.Version;
        }

        return version;
    }

    private static void assertExpectedNaturalKeyVersion<T>(object id, long? expectedVersion, long version)
    {
        if (expectedVersion.HasValue && version != expectedVersion.Value)
        {
            throw new EventStreamUnexpectedMaxEventIdException(id, typeof(T), expectedVersion.Value, version);
        }
    }

    // ---- many streams ----

    /// <summary>
    ///     jasperfx#1281: the many-stream form of <c>FetchForWriting&lt;T, TId&gt;</c>. By natural key, every key is
    ///     resolved in one round trip, and the streams it finds are then fetched as
    ///     <see cref="FetchManyForWriting{T}(IReadOnlyList{Guid}, CancellationToken)" /> fetches them, write cache
    ///     and all. An unknown key gets a new-stream handle in its place, as a single fetch by it would.
    /// </summary>
    public async Task<IReadOnlyList<IEventStream<T>>> FetchManyForWriting<T, TId>(IReadOnlyList<TId> ids,
        CancellationToken cancellation = default) where T : class where TId : notnull
    {
        JasperFx.Events.IEventStoreOperations.AssertFetchableIds(ids, nameof(ids));
        if (ids.Count == 0) return [];

        if (NaturalKeyPlanFor<T, TId>() is not { } plan)
        {
            var streamIds = new object[ids.Count];
            for (var i = 0; i < ids.Count; i++)
            {
                streamIds[i] = UnwrapStreamIdentity(ids[i]) ?? throw NoNaturalKey<T, TId>();
            }

            return await fetchManyForWritingAsync<T>(assertDistinctStreams(streamIds, nameof(ids)), cancellation)
                .ConfigureAwait(false);
        }

        var keyValues = new object[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            keyValues[i] = KeyForWriting(plan, ids[i]);
        }

        await _sessionBase.EnsureEventStoreSchemaAsync(cancellation).ConfigureAwait(false);

        // ---- round trip 1: every key's stream ----
        var batch = (BatchedQuery)_sessionBase.CreateBatchQuery();
        var resolutions = new Task<NaturalKeyFetchResult<T>>[ids.Count];
        var shape = new NaturalKeyFetchShape(false, false, null);
        for (var i = 0; i < ids.Count; i++)
        {
            var item = new NaturalKeyFetchBatchItem<T>(plan, keyValues[i], _tenantId, shape, _sessionBase.Serializer,
                _sessionBase);
            batch.RequireEventStore();
            batch.AddItem(item);
            resolutions[i] = item.Result;
        }

        await batch.Execute(cancellation).ConfigureAwait(false);

        var found = new List<(int Index, object StreamId)>();
        var streams = new IEventStream<T>[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            var result = await resolutions[i].ConfigureAwait(false);
            if (result.Found)
            {
                found.Add((i, result.StreamId));
            }
            else
            {
                streams[i] = await completeFetchForWritingAsync(plan, ids[i], result, false, null, cancellation)
                    .ConfigureAwait(false);
            }
        }

        if (found.Count == 0) return streams;

        // Two keys can't name one stream: an aggregate owns one key, and a lookup row names one stream
        // ---- round trips 2 and 3, as for stream ids ----
        var fetched = await fetchManyForWritingAsync<T>(found.Select(x => x.StreamId).ToArray(), cancellation)
            .ConfigureAwait(false);
        for (var n = 0; n < found.Count; n++)
        {
            streams[found[n].Index] = fetched[n];
        }

        return streams;
    }
}

/// <summary>
///     One fetch by natural key as an item of a <see cref="BatchedQuery" />, read into a
///     <see cref="NaturalKeyFetchResult{T}" />. What the caller makes of the result is its own business.
/// </summary>
internal sealed class NaturalKeyFetchBatchItem<T> : IBatchQueryItem where T : class
{
    private readonly TaskCompletionSource<NaturalKeyFetchResult<T>> _tcs = new();
    private readonly NaturalKeyFetchPlan<T> _plan;
    private readonly object _keyValue;
    private readonly string _tenantId;
    private readonly NaturalKeyFetchShape _shape;
    private readonly Polecat.Serialization.ISerializer _serializer;
    private readonly IStorageSession _session;

    public NaturalKeyFetchBatchItem(NaturalKeyFetchPlan<T> plan, object keyValue, string tenantId,
        NaturalKeyFetchShape shape, Polecat.Serialization.ISerializer serializer, IStorageSession session)
    {
        _plan = plan;
        _keyValue = keyValue;
        _tenantId = tenantId;
        _shape = shape;
        _serializer = serializer;
        _session = session;
    }

    public Task<NaturalKeyFetchResult<T>> Result => _tcs.Task;

    public void Fail(Exception failure) => _tcs.FailWith(failure);

    public void WriteSql(Weasel.SqlServer.ICommandBuilder builder)
        => _plan.WriteFetch(builder, _keyValue, _tenantId, _shape);

    public async Task ReadResultSetAsync(System.Data.Common.DbDataReader reader, CancellationToken token)
    {
        _tcs.SetResult(await _plan.ReadFetchAsync(reader, _serializer, _session, _tenantId, _shape, token)
            .ConfigureAwait(false));
    }
}
