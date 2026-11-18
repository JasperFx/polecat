using System.Collections.Concurrent;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Events;

/// <summary>
///     polecat#1049 (port of marten#5799's stress test). Several writers append to the same Async-snapshotted
///     streams through every kind of fetch for writing, while readers fetch the same streams and the snapshot
///     moves underneath all of them. Every event records its own version in the aggregate, so a fetch that
///     applies an event twice or misses one shows up as a repeat or a gap, a fetch whose version and
///     aggregate disagree shows up as a mismatch, and a fetch that comes back behind an append that had
///     already committed shows up as too short.
/// </summary>
/// <remarks>
///     The snapshot moves two ways. With the daemon running, it only moves forward, as in production. With the
///     daemon off, a task deletes it or rewrites it at an earlier version, which is what a rebuild does to it.
///     The rewritten snapshot is always correct for the version it claims, folded live from the events, so any
///     wrong answer is the fetch's.
/// </remarks>
public abstract class async_fetch_under_concurrent_writes : OneOffConfigurationsContext
{
    private const int Writers = 4;
    private const int AppendsPerWriter = 10;
    private const int Readers = 2;

    public enum SnapshotMovement
    {
        DaemonRunning,
        DaemonRunningWithTheWriteCache,
        RewoundAndDeleted
    }

    public enum WriteFetch
    {
        ForWriting,
        ForExclusiveWriting,
        ManyForWriting
    }

    /// <summary>One stream as a fetch found it: the event versions folded into the aggregate, and its version.</summary>
    public readonly record struct Fetched(object Id, IReadOnlyList<long>? Seen, long? Version);

    private readonly TallySubject _subject;

    protected async_fetch_under_concurrent_writes(TallySubject subject)
    {
        _subject = subject;
    }

    [Theory]
    [InlineData(SnapshotMovement.DaemonRunning)]
    [InlineData(SnapshotMovement.DaemonRunningWithTheWriteCache)]
    [InlineData(SnapshotMovement.RewoundAndDeleted)]
    public async Task no_event_is_lost_or_applied_twice(SnapshotMovement movement)
    {
        ConfigureStore(opts =>
        {
            opts.Projections.UseIdentityMapForAggregates = false;
            _subject.Configure(opts, movement == SnapshotMovement.DaemonRunningWithTheWriteCache);
        });

        var ct = TestContext.Current.CancellationToken;

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);
        var ids = new[] { _subject.NewId(), _subject.NewId() };
        var committed = new long[ids.Length];
        for (var i = 0; i < ids.Length; i++)
        {
            await using var s = theStore.LightweightSession();
            _subject.Start(s, ids[i], new Tick(-1));
            await s.SaveChangesAsync(ct);
            committed[i] = 1;
        }

        var failures = new ConcurrentQueue<string>();
        var appends = 0;
        var rewinds = 0;

        var daemon = movement == SnapshotMovement.RewoundAndDeleted
            ? null
            : (IProjectionDaemon)await theStore.BuildProjectionDaemonAsync();
        if (daemon != null) await daemon.StartAllAsync();

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        void check(string what, Fetched fetched, long atLeast)
        {
            var problem = describe(fetched, atLeast);
            if (problem != null) failures.Enqueue($"{what} of {fetched.Id}: {problem}");
        }

        async Task writeAsync(int writer)
        {
            var done = 0;
            for (var attempt = 0; done < AppendsPerWriter && attempt < AppendsPerWriter * 50; attempt++)
            {
                var target = (writer + done) % ids.Length;
                var kind = (WriteFetch)((writer + attempt) % Enum.GetValues<WriteFetch>().Length);
                var floors = new long[ids.Length];
                for (var i = 0; i < ids.Length; i++) floors[i] = Interlocked.Read(ref committed[i]);

                try
                {
                    await using var s = theStore.LightweightSession();
                    var found = await _subject.FetchForWritingAsync(s, kind, ids[target], ids[1 - target], ct);
                    foreach (var stream in found)
                    {
                        check($"{kind} by writer {writer}", stream, floors[Array.IndexOf(ids, stream.Id)]);
                    }

                    _subject.Append(s, ids[target], new Tick(writer));
                    await s.SaveChangesAsync(ct);

                    var written = found[0].Version!.Value + 1;
                    long seen;
                    while ((seen = Interlocked.Read(ref committed[target])) < written &&
                           Interlocked.CompareExchange(ref committed[target], written, seen) != seen)
                    {
                    }

                    Interlocked.Increment(ref appends);
                    done++;
                }
                catch (Exception e) when (e is ConcurrencyException or StreamLockedException)
                {
                    // Another writer got there first, or holds the stream. Fetch again.
                }
                catch (Exception e)
                {
                    failures.Enqueue($"{kind} by writer {writer}: {e}");
                    return;
                }
            }

            if (done < AppendsPerWriter) failures.Enqueue($"Writer {writer} only appended {done} times");
        }

        async Task readAsync(int reader)
        {
            for (var attempt = 0; !stop.IsCancellationRequested; attempt++)
            {
                var target = (reader + attempt) % ids.Length;
                var floor = Interlocked.Read(ref committed[target]);

                try
                {
                    await using var s = theStore.LightweightSession();
                    var found = await _subject.FetchLatestAsync(s, ids[target], ct);
                    check($"FetchLatest by reader {reader}", found, floor);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    failures.Enqueue($"FetchLatest by reader {reader}: {e}");
                    return;
                }
            }
        }

        async Task rewindAsync()
        {
            var random = new Random(17);
            while (!stop.IsCancellationRequested)
            {
                var target = random.Next(ids.Length);
                var current = Interlocked.Read(ref committed[target]);

                try
                {
                    if (random.Next(3) == 0)
                    {
                        await _subject.DeleteSnapshotAsync(theStore, ids[target], ct);
                    }
                    else
                    {
                        await _subject.WriteSnapshotAsync(theStore, ids[target], random.NextInt64(1, current + 1), ct);
                    }

                    Interlocked.Increment(ref rewinds);
                    await Task.Delay(random.Next(2, 8), stop.Token);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    failures.Enqueue($"Moving the snapshot: {e}");
                    return;
                }
            }
        }

        var writers = Enumerable.Range(0, Writers).Select(w => Task.Run(() => writeAsync(w), ct)).ToArray();
        var others = Enumerable.Range(0, Readers).Select(r => Task.Run(() => readAsync(r), ct)).ToList();
        if (movement == SnapshotMovement.RewoundAndDeleted) others.Add(Task.Run(rewindAsync, ct));

        try
        {
            await Task.WhenAll(writers);
        }
        finally
        {
            await stop.CancelAsync();
            await Task.WhenAll(others);
            if (daemon != null)
            {
                await daemon.StopAllAsync();
                daemon.Dispose();
            }
        }

        failures.ShouldBeEmpty(string.Join(Environment.NewLine, failures));
        appends.ShouldBe(Writers * AppendsPerWriter);
        if (movement == SnapshotMovement.RewoundAndDeleted) rewinds.ShouldBeGreaterThan(0);

        // Settled: every append is in its stream exactly once, through every kind of fetch
        committed.Sum().ShouldBe(ids.Length + Writers * AppendsPerWriter);
        for (var i = 0; i < ids.Length; i++)
        {
            await using var s = theStore.LightweightSession();
            foreach (var kind in Enum.GetValues<WriteFetch>())
            {
                var found = await _subject.FetchForWritingAsync(s, kind, ids[i], ids[1 - i], ct);
                describe(found[0], committed[i]).ShouldBeNull();
                found[0].Version.ShouldBe(committed[i]);
            }

            var latest = await _subject.FetchLatestAsync(s, ids[i], ct);
            describe(latest, committed[i]).ShouldBeNull();
            latest.Seen!.Count.ShouldBe((int)committed[i]);
        }
    }

    /// <summary>What is wrong with a fetch's answer, or null when the events folded are exactly 1..version.</summary>
    private static string? describe(Fetched fetched, long atLeast)
    {
        var seen = fetched.Seen;
        if (seen == null) return "no aggregate for a stream that has events";

        for (var i = 0; i < seen.Count; i++)
        {
            if (seen[i] != i + 1)
            {
                return $"the aggregate folded versions [{string.Join(", ", seen)}], not 1..{seen.Count}";
            }
        }

        if (fetched.Version.HasValue && fetched.Version.Value != seen.Count)
        {
            return $"the stream is at version {fetched.Version} but the aggregate folded {seen.Count} events";
        }

        if (seen.Count < atLeast)
        {
            return $"the aggregate folded {seen.Count} events, but {atLeast} had already been committed";
        }

        return null;
    }
}

public record Tick(int Writer);

public partial class TickTally
{
    public Guid Id { get; set; }
    public List<long> Seen { get; set; } = new();

    public void Apply(IEvent<Tick> e) => Seen.Add(e.Version);
}

public partial class TickTallyAsString
{
    public string Id { get; set; } = null!;
    public List<long> Seen { get; set; } = new();

    public void Apply(IEvent<Tick> e) => Seen.Add(e.Version);
}

public readonly record struct TickTallyId(Guid Value);

public partial class TickTallyWithAStrongTypedId
{
    public TickTallyId Id { get; set; }
    public List<long> Seen { get; set; } = new();

    public void Apply(IEvent<Tick> e) => Seen.Add(e.Version);
}

/// <summary>Drives one tally aggregate through the fetches, by Guid or string stream identity.</summary>
public abstract class TallySubject
{
    public abstract void Configure(StoreOptions opts, bool writeCache);
    public abstract object NewId();
    public abstract void Start(IDocumentSession session, object id, Tick tick);
    public abstract void Append(IDocumentSession session, object id, Tick tick);

    /// <summary>The fetched stream first, then any other stream the same fetch read.</summary>
    public abstract Task<IReadOnlyList<async_fetch_under_concurrent_writes.Fetched>> FetchForWritingAsync(
        IDocumentSession session, async_fetch_under_concurrent_writes.WriteFetch fetch, object id, object otherId,
        CancellationToken ct);

    public abstract Task<async_fetch_under_concurrent_writes.Fetched> FetchLatestAsync(IDocumentSession session,
        object id, CancellationToken ct);

    public abstract Task DeleteSnapshotAsync(IDocumentStore store, object id, CancellationToken ct);

    /// <summary>Replaces the snapshot with the stream folded live up to <paramref name="version" />.</summary>
    public abstract Task WriteSnapshotAsync(IDocumentStore store, object id, long version, CancellationToken ct);
}

public sealed class TallySubject<T>(
    Action<StoreOptions> configure,
    Func<object> newId,
    Func<T, IReadOnlyList<long>> seenOf) : TallySubject where T : class
{
    public override void Configure(StoreOptions opts, bool writeCache)
    {
        configure(opts);
        opts.Projections.Snapshot<T>(SnapshotLifecycle.Async);
        if (writeCache) opts.Events.CacheAggregatesForWriting<T>();
    }

    public override object NewId() => newId();

    public override void Start(IDocumentSession session, object id, Tick tick)
    {
        if (id is Guid guid) session.Events.StartStream<T>(guid, tick);
        else session.Events.StartStream<T>((string)id, tick);
    }

    public override void Append(IDocumentSession session, object id, Tick tick)
    {
        if (id is Guid guid) session.Events.Append(guid, tick);
        else session.Events.Append((string)id, tick);
    }

    private async_fetch_under_concurrent_writes.Fetched found(object id, T? aggregate, long? version) =>
        new(id, aggregate == null ? null : seenOf(aggregate).ToList(), version);

    private async_fetch_under_concurrent_writes.Fetched found(object id, IEventStream<T> stream) =>
        found(id, stream.Aggregate, stream.CurrentVersion);

    public override async Task<IReadOnlyList<async_fetch_under_concurrent_writes.Fetched>> FetchForWritingAsync(
        IDocumentSession session, async_fetch_under_concurrent_writes.WriteFetch fetch, object id, object otherId,
        CancellationToken ct)
    {
        var events = session.Events;
        switch (fetch)
        {
            case async_fetch_under_concurrent_writes.WriteFetch.ForWriting:
                return [found(id, id is Guid g1 ? await events.FetchForWriting<T>(g1, ct) : await events.FetchForWriting<T>((string)id, ct))];

            case async_fetch_under_concurrent_writes.WriteFetch.ForExclusiveWriting:
                return [found(id, id is Guid g2
                    ? await events.FetchForExclusiveWriting<T>(g2, ct)
                    : await events.FetchForExclusiveWriting<T>((string)id, ct))];

            case async_fetch_under_concurrent_writes.WriteFetch.ManyForWriting:
            {
                var streams = id is Guid g3
                    ? await events.FetchManyForWriting<T>([g3, (Guid)otherId], ct)
                    : await events.FetchManyForWriting<T>([(string)id, (string)otherId], ct);
                return [found(id, streams[0]), found(otherId, streams[1])];
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(fetch));
        }
    }

    public override async Task<async_fetch_under_concurrent_writes.Fetched> FetchLatestAsync(
        IDocumentSession session, object id, CancellationToken ct)
        => found(id, id is Guid guid
            ? await session.Events.FetchLatest<T>(guid, ct)
            : await session.Events.FetchLatest<T>((string)id, ct), null);

    public override async Task DeleteSnapshotAsync(IDocumentStore store, object id, CancellationToken ct)
    {
        var table = ((DocumentStore)store).GetProvider(typeof(T)).Mapping.QualifiedTableName;
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DELETE FROM {table} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override async Task WriteSnapshotAsync(IDocumentStore store, object id, long version, CancellationToken ct)
    {
        T? folded;
        await using (var query = store.QuerySession())
        {
            folded = id is Guid guid
                ? await query.Events.AggregateStreamAsync<T>(guid, version, token: ct)
                : await query.Events.AggregateStreamAsync<T>((string)id, version, token: ct);
        }

        if (folded == null) return;

        await using var session = store.LightweightSession();
        session.Store(folded);
        await session.SaveChangesAsync(ct);
    }
}

public class async_fetch_under_concurrent_writes_on_guid_streams() : async_fetch_under_concurrent_writes(
    new TallySubject<TickTally>(_ => { }, () => Guid.NewGuid(), x => x.Seen));

public class async_fetch_under_concurrent_writes_on_string_streams() : async_fetch_under_concurrent_writes(
    new TallySubject<TickTallyAsString>(opts => opts.Events.StreamIdentity = StreamIdentity.AsString,
        () => Guid.NewGuid().ToString(), x => x.Seen));

public class async_fetch_under_concurrent_writes_with_a_strong_typed_id() : async_fetch_under_concurrent_writes(
    new TallySubject<TickTallyWithAStrongTypedId>(opts => opts.RegisterValueType<TickTallyId>(),
        () => Guid.NewGuid(), x => x.Seen));
