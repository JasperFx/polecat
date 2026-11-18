using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1049 (port of marten#5799). Polecat folds an Async aggregate from its events rather than
///     from its snapshot, so a fetch for writing reads the stream version and then the events, as separate
///     statements, and a <c>FetchLatest</c> reads the events. These tests run a change to the database
///     between those reads, and check that every kind of fetch still answers rightly: at the newest
///     version it saw, aggregate and version together, and never with a snapshot that outlived its stream.
///     Each subclass runs the same scenarios against a different store setup.
/// </summary>
public abstract class async_fetch_reads_in_a_safe_order : OneOffConfigurationsContext
{
    public enum Fetch
    {
        ForWriting,
        ForWritingAtVersion,
        ForExclusiveWriting,
        Latest,
        ManyForWriting
    }

    public static IEnumerable<object[]> AllFetches() => Enum.GetValues<Fetch>().Select(x => new object[] { x });

    // An exclusive fetch holds UPDLOCK, HOLDLOCK on the stream row, or a range lock where a missing stream
    // would go, until the session commits. A change that appends to or starts the stream waits for that
    // lock while the fetch waits for the change, so the two cannot race.
    public static IEnumerable<object[]> FetchesAWriteCanRace() =>
        Enum.GetValues<Fetch>().Where(x => x != Fetch.ForExclusiveWriting).Select(x => new object[] { x });

    /// <summary>What a fetch found, reduced to what every aggregate in these tests can report.</summary>
    public record struct Found(int A, int B, int C, int D, long? Version);

    private readonly StoreShape _shape;

    protected async_fetch_reads_in_a_safe_order(StoreShape shape)
    {
        _shape = shape;
        ConfigureStore(opts =>
        {
            opts.Projections.UseIdentityMapForAggregates = false;
            shape.Configure(opts);
        });
    }

    private IDocumentSession sessionFor(string? tenant) =>
        tenant == null ? theStore.LightweightSession() : theStore.LightweightSession(tenant);

    private async Task catchTheDaemonUpAsync()
    {
        using var daemon = (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.CatchUpAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
        await daemon.StopAllAsync();
    }

    private async Task appendAsync(string? tenant, object id, bool start, params char[] letters)
    {
        await using var s = sessionFor(tenant);
        var events = letters.Select(_shape.Subject.Event).ToArray();
        if (start) _shape.Subject.Start(s, id, events);
        else _shape.Subject.Append(s, id, events);
        await s.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     A stream of A, B, C. Where the shape has a second tenant, the same id there holds A and four B's
    ///     and gets a snapshot of its own, so anything read from the wrong tenant shows in the counts.
    /// </summary>
    private async Task<object> aStreamWithNoSnapshotYetAsync()
    {
        var id = _shape.Subject.NewId();
        if (_shape.OtherTenant != null)
        {
            await appendAsync(_shape.OtherTenant, id, true, 'A', 'B', 'B', 'B', 'B');
            await catchTheDaemonUpAsync();
        }

        await appendAsync(_shape.Tenant, id, true, 'A', 'B', 'C');
        return id;
    }

    private async Task<object> aStreamWithAStaleSnapshotAsync()
    {
        var id = _shape.Subject.NewId();
        if (_shape.OtherTenant != null)
        {
            await appendAsync(_shape.OtherTenant, id, true, 'A', 'B', 'B', 'B', 'B');
        }

        await appendAsync(_shape.Tenant, id, true, 'A');
        await catchTheDaemonUpAsync();
        await appendAsync(_shape.Tenant, id, false, 'B', 'C');
        return id;
    }

    private async Task<object> aStreamWithACurrentSnapshotAsync()
    {
        var id = await aStreamWithNoSnapshotYetAsync();
        await catchTheDaemonUpAsync();
        return id;
    }

    private async Task executeAsync(string sql, object id)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tenant", _shape.Tenant ?? StorageConstants.DefaultTenantId);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task deleteTheSnapshotAsync(object id)
    {
        var table = theStore.GetProvider(_shape.Subject.AggregateType).Mapping.QualifiedTableName;
        return executeAsync(
            $"DELETE FROM {table} WHERE id = @id" + (_shape.Tenant == null ? "" : " AND tenant_id = @tenant"), id);
    }

    private Task deleteTheStreamAsync(object id)
    {
        var events = theStore.Options.EventGraph;
        return executeAsync($"DELETE FROM {events.EventsTableName} WHERE stream_id = @id AND tenant_id = @tenant; " +
                            $"DELETE FROM {events.StreamsTableName} WHERE id = @id AND tenant_id = @tenant;", id);
    }

    /// <summary>
    ///     Runs the fetch with <paramref name="interfere" /> committed between its reads, and returns what it
    ///     found. The change runs before the first read of the events or of a missing stream's snapshot,
    ///     which is after a fetch for writing has read the stream version.
    /// </summary>
    private async Task<Found?> fetchWhileAsync(Fetch fetch, object id, Func<Task> interfere)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var s = sessionFor(_shape.Tenant);
        var pause = PauseBeforeTheEventsRead.Attach(s, interfere);

        var found = await _shape.Subject.FetchAsync(s, fetch, id, 3, ct);
        pause.Paused.ShouldBe(1, "the change never ran between the fetch's reads");
        return found;
    }

    private static void shouldBe(Found? found, int a, int b, int c, int d, long version)
    {
        found.ShouldNotBeNull();
        found.Value.A.ShouldBe(a);
        found.Value.B.ShouldBe(b);
        found.Value.C.ShouldBe(c);
        found.Value.D.ShouldBe(d);
        if (found.Value.Version.HasValue) found.Value.Version.Value.ShouldBe(version);
    }

    private static void shouldBeTheWholeStream(Found? found) => shouldBe(found, 1, 1, 1, 0, 3);

    [Theory]
    [MemberData(nameof(AllFetches))]
    public async Task the_daemon_writes_the_first_snapshot_mid_fetch(Fetch fetch)
    {
        var id = await aStreamWithNoSnapshotYetAsync();

        shouldBeTheWholeStream(await fetchWhileAsync(fetch, id, catchTheDaemonUpAsync));
    }

    [Theory]
    [MemberData(nameof(AllFetches))]
    public async Task the_daemon_advances_a_stale_snapshot_mid_fetch(Fetch fetch)
    {
        var id = await aStreamWithAStaleSnapshotAsync();

        shouldBeTheWholeStream(await fetchWhileAsync(fetch, id, catchTheDaemonUpAsync));
    }

    [Theory]
    [MemberData(nameof(AllFetches))]
    public async Task a_current_snapshot_is_deleted_mid_fetch(Fetch fetch)
    {
        var id = await aStreamWithACurrentSnapshotAsync();

        shouldBeTheWholeStream(await fetchWhileAsync(fetch, id, () => deleteTheSnapshotAsync(id)));
    }

    [Theory]
    [MemberData(nameof(AllFetches))]
    public async Task a_stale_snapshot_is_deleted_mid_fetch(Fetch fetch)
    {
        var id = await aStreamWithAStaleSnapshotAsync();

        shouldBeTheWholeStream(await fetchWhileAsync(fetch, id, () => deleteTheSnapshotAsync(id)));
    }

    // An event appended and projected mid-fetch: the stream version was read at 3 and the events read finds
    // 4. The fetch answers at 4, aggregate and version both, because that is the newest state it saw.

    private async Task appendAnEventAndCatchTheDaemonUpAsync(object id)
    {
        await appendAsync(_shape.Tenant, id, false, 'D');
        await catchTheDaemonUpAsync();
    }

    [Theory]
    [MemberData(nameof(FetchesAWriteCanRace))]
    public async Task an_event_is_appended_and_projected_mid_fetch(Fetch fetch)
    {
        var id = await aStreamWithACurrentSnapshotAsync();

        if (fetch == Fetch.ForWritingAtVersion)
        {
            // Expected 3, and the stream reached 4 while the fetch was reading it
            var ex = await Should.ThrowAsync<ConcurrencyException>(() =>
                fetchWhileAsync(fetch, id, () => appendAnEventAndCatchTheDaemonUpAsync(id)));
            ex.Message.ShouldContain("but was 4");
            return;
        }

        shouldBe(await fetchWhileAsync(fetch, id, () => appendAnEventAndCatchTheDaemonUpAsync(id)), 1, 1, 1, 1, 4);
    }

    // A stream that does not exist yet when the fetch reads its version, and is started and projected before
    // the next read. The fetch reads it again rather than answer with a new stream, or refuse a snapshot
    // that has a stream after all.

    [Theory]
    [MemberData(nameof(FetchesAWriteCanRace))]
    public async Task a_stream_is_started_and_projected_mid_fetch(Fetch fetch)
    {
        var id = _shape.Subject.NewId();
        if (_shape.OtherTenant != null)
        {
            await appendAsync(_shape.OtherTenant, id, true, 'A', 'B', 'B', 'B', 'B');
            await catchTheDaemonUpAsync();
        }

        shouldBeTheWholeStream(await fetchWhileAsync(fetch, id, async () =>
        {
            await appendAsync(_shape.Tenant, id, true, 'A', 'B', 'C');
            await catchTheDaemonUpAsync();
        }));
    }

    // A snapshot whose stream is gone, as DeleteAllEventDataAsync leaves it: every fetch refuses it, rather
    // than answer with a new, empty aggregate that the daemon would then fold onto the old snapshot.

    [Theory]
    [MemberData(nameof(AllFetches))]
    public async Task a_snapshot_that_outlived_its_stream_is_refused(Fetch fetch)
    {
        var id = await aStreamWithACurrentSnapshotAsync();
        await deleteTheStreamAsync(id);

        var ex = await Should.ThrowAsync<SnapshotWithoutStreamException>(() =>
            fetchWhileAsync(fetch, id, () => Task.CompletedTask));
        ex.Id.ShouldBe(id);
        ex.AggregateType.ShouldBe(_shape.Subject.AggregateType);
    }

    // The check for a snapshot without its stream leaves a stream that is simply new alone
    [Fact]
    public async Task a_new_stream_with_no_snapshot_is_still_a_new_stream()
    {
        var id = _shape.Subject.NewId();

        await using var s = sessionFor(_shape.Tenant);
        foreach (var fetch in Enum.GetValues<Fetch>())
        {
            (await _shape.Subject.FetchAsync(s, fetch, id, 0, TestContext.Current.CancellationToken)).ShouldBeNull();
        }
    }
}

/// <summary>
///     Runs a change to the database before the first read of a fetch's events or of a missing stream's
///     snapshot, through the session's test seam. A fetch for writing has read the stream version by then,
///     so the change lands between its two reads. The fetching session holds no locks at that point,
///     except an exclusive fetch's lock on the stream.
/// </summary>
public sealed class PauseBeforeTheEventsRead
{
    private readonly Func<Task> _change;
    private readonly int _fetchIndex;

    private PauseBeforeTheEventsRead(Func<Task> change, int fetchIndex)
    {
        _change = change;
        _fetchIndex = fetchIndex;
    }

    /// <summary>How many times the change ran.</summary>
    public int Paused { get; private set; }

    /// <summary>How many reads of events or snapshots the session has sent.</summary>
    public int Reads { get; private set; }

    /// <param name="session">The session that fetches</param>
    /// <param name="change">The change to make</param>
    /// <param name="fetchIndex">Which of the session's reads of events or snapshots to pause, from 0</param>
    public static PauseBeforeTheEventsRead Attach(IQuerySession session, Func<Task> change, int fetchIndex = 0)
    {
        var pause = new PauseBeforeTheEventsRead(change, fetchIndex);
        ((QuerySession)session).BeforeExecuteAsync = pause.beforeExecuteAsync;
        return pause;
    }

    private async Task beforeExecuteAsync(string sql, CancellationToken token)
    {
        if (!sql.Contains("pc_events") && !sql.Contains("pc_doc_")) return;

        if (Reads++ != _fetchIndex) return;

        Paused++;
        await _change();
    }
}

/// <summary>
///     One store setup for <see cref="async_fetch_reads_in_a_safe_order" />: how the store is configured, the
///     aggregate it fetches, and the tenant the fetch runs in.
/// </summary>
public sealed record StoreShape(
    Action<StoreOptions> Configure,
    FetchSubject Subject,
    string? Tenant = null,
    string? OtherTenant = null);

/// <summary>Drives one aggregate type through every kind of fetch, by Guid or string stream identity.</summary>
public abstract class FetchSubject
{
    public abstract Type AggregateType { get; }
    public abstract object NewId();
    public abstract object Event(char letter);
    public abstract void Start(IDocumentSession session, object id, object[] events);
    public abstract void Append(IDocumentSession session, object id, object[] events);

    public abstract Task<async_fetch_reads_in_a_safe_order.Found?> FetchAsync(IDocumentSession session,
        async_fetch_reads_in_a_safe_order.Fetch fetch, object id, long expectedVersion, CancellationToken ct);
}

public sealed class FetchSubject<T>(
    Func<object> newId,
    Func<T, async_fetch_reads_in_a_safe_order.Found> countsOf) : FetchSubject where T : class
{
    public override Type AggregateType => typeof(T);
    public override object NewId() => newId();

    public override object Event(char letter) => letter switch
    {
        'A' => new FetchedA(),
        'B' => new FetchedB(),
        'C' => new FetchedC(),
        'D' => new FetchedD(),
        _ => throw new ArgumentOutOfRangeException(nameof(letter))
    };

    public override void Start(IDocumentSession session, object id, object[] events)
    {
        if (id is Guid guid) session.Events.StartStream<T>(guid, events);
        else session.Events.StartStream<T>((string)id, events);
    }

    public override void Append(IDocumentSession session, object id, object[] events)
    {
        if (id is Guid guid) session.Events.Append(guid, events);
        else session.Events.Append((string)id, events);
    }

    private async_fetch_reads_in_a_safe_order.Found? found(T? aggregate, long? version) =>
        aggregate == null ? null : countsOf(aggregate) with { Version = version };

    private async_fetch_reads_in_a_safe_order.Found? found(IEventStream<T> stream) =>
        found(stream.Aggregate, stream.CurrentVersion);

    public override async Task<async_fetch_reads_in_a_safe_order.Found?> FetchAsync(IDocumentSession session,
        async_fetch_reads_in_a_safe_order.Fetch fetch, object id, long expectedVersion, CancellationToken ct)
    {
        var events = session.Events;
        var guid = id as Guid?;
        var key = id as string;

        switch (fetch)
        {
            case async_fetch_reads_in_a_safe_order.Fetch.ForWriting:
                return found(guid.HasValue
                    ? await events.FetchForWriting<T>(guid.Value, ct)
                    : await events.FetchForWriting<T>(key!, ct));

            case async_fetch_reads_in_a_safe_order.Fetch.ForWritingAtVersion:
                return found(guid.HasValue
                    ? await events.FetchForWriting<T>(guid.Value, expectedVersion, ct)
                    : await events.FetchForWriting<T>(key!, expectedVersion, ct));

            case async_fetch_reads_in_a_safe_order.Fetch.ForExclusiveWriting:
                return found(guid.HasValue
                    ? await events.FetchForExclusiveWriting<T>(guid.Value, ct)
                    : await events.FetchForExclusiveWriting<T>(key!, ct));

            case async_fetch_reads_in_a_safe_order.Fetch.Latest:
                return found(guid.HasValue
                    ? await events.FetchLatest<T>(guid.Value, ct)
                    : await events.FetchLatest<T>(key!, ct), null);

            case async_fetch_reads_in_a_safe_order.Fetch.ManyForWriting:
                // Behind another stream that does not exist, so the fetched one is not first
                var streams = guid.HasValue
                    ? await events.FetchManyForWriting<T>([Guid.NewGuid(), guid.Value], ct)
                    : await events.FetchManyForWriting<T>([Guid.NewGuid().ToString(), key!], ct);
                streams[0].Aggregate.ShouldBeNull();
                return found(streams[1]);

            default:
                throw new ArgumentOutOfRangeException(nameof(fetch));
        }
    }
}

public record FetchedA;

public record FetchedB;

public record FetchedC;

public record FetchedD;

public partial class FetchedAggregate
{
    public Guid Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }
    public int CCount { get; set; }
    public int DCount { get; set; }

    public void Apply(FetchedA _) => ACount++;
    public void Apply(FetchedB _) => BCount++;
    public void Apply(FetchedC _) => CCount++;
    public void Apply(FetchedD _) => DCount++;
}

public partial class FetchedAggregateAsString
{
    public string Id { get; set; } = null!;
    public int ACount { get; set; }
    public int BCount { get; set; }
    public int CCount { get; set; }
    public int DCount { get; set; }

    public void Apply(FetchedA _) => ACount++;
    public void Apply(FetchedB _) => BCount++;
    public void Apply(FetchedC _) => CCount++;
    public void Apply(FetchedD _) => DCount++;
}

public readonly record struct FetchedAggregateId(Guid Value);

/// <summary><see cref="FetchedAggregate" /> with a strong-typed id, fetched by its Guid stream id.</summary>
public partial class FetchedAggregateWithAStrongTypedId
{
    public FetchedAggregateId Id { get; set; }
    public int ACount { get; set; }
    public int BCount { get; set; }
    public int CCount { get; set; }
    public int DCount { get; set; }

    public void Apply(FetchedA _) => ACount++;
    public void Apply(FetchedB _) => BCount++;
    public void Apply(FetchedC _) => CCount++;
    public void Apply(FetchedD _) => DCount++;
}

internal static class FetchSubjects
{
    public static readonly FetchSubject Guids = new FetchSubject<FetchedAggregate>(
        () => Guid.NewGuid(), x => new(x.ACount, x.BCount, x.CCount, x.DCount, null));

    public static readonly FetchSubject Strings = new FetchSubject<FetchedAggregateAsString>(
        () => Guid.NewGuid().ToString(), x => new(x.ACount, x.BCount, x.CCount, x.DCount, null));

    public static readonly FetchSubject StrongTypedIds = new FetchSubject<FetchedAggregateWithAStrongTypedId>(
        () => Guid.NewGuid(), x => new(x.ACount, x.BCount, x.CCount, x.DCount, null));
}

public class async_fetch_reads_in_a_safe_order_on_guid_streams() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts => opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async),
    FetchSubjects.Guids));

public class async_fetch_reads_in_a_safe_order_on_string_streams() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts =>
    {
        opts.Events.StreamIdentity = StreamIdentity.AsString;
        opts.Projections.Snapshot<FetchedAggregateAsString>(SnapshotLifecycle.Async);
    },
    FetchSubjects.Strings));

public class async_fetch_reads_in_a_safe_order_with_conjoined_tenancy() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts =>
    {
        opts.Policies.AllDocumentsAreMultiTenanted();
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async);
    },
    FetchSubjects.Guids, Tenant: "tenant-a", OtherTenant: "tenant-b"));

public class async_fetch_reads_in_a_safe_order_with_the_identity_map() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts =>
    {
        opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async);
        opts.Projections.UseIdentityMapForAggregates = true;
    },
    FetchSubjects.Guids));

public class async_fetch_reads_in_a_safe_order_with_the_write_cache() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts =>
    {
        opts.Projections.Snapshot<FetchedAggregate>(SnapshotLifecycle.Async);
        opts.Events.CacheAggregatesForWriting<FetchedAggregate>();
    },
    FetchSubjects.Guids));

public class async_fetch_reads_in_a_safe_order_with_a_strong_typed_id() : async_fetch_reads_in_a_safe_order(new StoreShape(
    opts =>
    {
        opts.RegisterValueType<FetchedAggregateId>();
        opts.Projections.Snapshot<FetchedAggregateWithAStrongTypedId>(SnapshotLifecycle.Async);
    },
    FetchSubjects.StrongTypedIds));
