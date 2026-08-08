using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #420 — an async projection's raised events were silently dropped.
///     <para>
///     <c>PolecatProjectionBatch</c> implemented the three members JasperFx drives event-raising
///     through (<c>QuickAppendEventWithVersion</c>, <c>UpdateStreamVersion</c>,
///     <c>QuickAppendEvents</c>) as empty methods. <c>EventSlice.BuildOperations</c> calls them
///     whenever a projection raises an event, so an async projection that appends events lost them
///     with no exception, no shard failure, no dead letter and no log line: the projection's
///     documents committed, the progression row advanced, and the raised events simply never
///     existed. The only way to notice was to go looking for events that should be there.
///     </para>
///     <para>
///     Inline projections were never affected — this is the async daemon batch only.
///     </para>
/// </summary>
public partial class async_projection_raised_events_tests : IAsyncLifetime
{
    private const string Schema = "async_raised_events";

    public async ValueTask InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.Projections.Add(new RaisingProjection(), ProjectionLifecycle.Async);
        });
    }

    /// <summary>
    ///     The headline case: a projection raises an event onto a different stream. Before the fix
    ///     the audit stream did not exist at all.
    /// </summary>
    [Fact]
    public async Task raised_events_are_appended_to_their_stream()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new RaiseStarted("alpha"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var audit = await query.Events.FetchStreamAsync(AuditStreamFor(streamId),
            token: TestContext.Current.CancellationToken);

        audit.Count.ShouldBe(1);
        var raised = audit[0].Data.ShouldBeOfType<RaiseNoticed>();
        raised.Label.ShouldBe("alpha");

        // Versions come from the stream row this batch read under UPDLOCK/HOLDLOCK, not from the
        // slice's client-side count.
        audit[0].Version.ShouldBe(1);
        audit[0].StreamId.ShouldBe(AuditStreamFor(streamId));
    }

    /// <summary>
    ///     The stream row has to be written too, not just the event rows — otherwise the raised
    ///     events are invisible to every version-aware read path.
    /// </summary>
    [Fact]
    public async Task the_raised_event_stream_row_is_created_with_the_right_version()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new RaiseStarted("beta"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(AuditStreamFor(streamId),
            TestContext.Current.CancellationToken);

        state.ShouldNotBeNull();
        state!.Version.ShouldBe(1);
    }

    /// <summary>
    ///     Two source streams raise onto two distinct audit streams, and appending again to a
    ///     stream that already exists takes the UPDATE branch rather than trying to insert a second
    ///     stream row — the version has to continue from what the earlier batch left behind.
    /// </summary>
    [Fact]
    public async Task appends_accumulate_across_batches_on_an_existing_stream()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new RaiseStarted("one"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        // A second event on the same source stream produces a second raised event on the same
        // audit stream, in a later batch.
        await using (var session = store.LightweightSession())
        {
            session.Events.Append(streamId, new RaiseStarted("two"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var audit = await query.Events.FetchStreamAsync(AuditStreamFor(streamId),
            token: TestContext.Current.CancellationToken);

        audit.Count.ShouldBe(2);
        audit.Select(x => x.Version).ShouldBe(new long[] { 1, 2 });
        audit.Select(x => ((RaiseNoticed)x.Data).Label).ShouldBe(new[] { "one", "two" });
    }

    /// <summary>
    ///     A projection that raises nothing must not pay for the feature — no stray stream rows,
    ///     and the ordinary projected document still lands.
    /// </summary>
    [Fact]
    public async Task a_batch_with_no_raised_events_is_unaffected()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Projections.Add(new SingleStreamProjection<RaiseSnap, Guid>(), ProjectionLifecycle.Async);
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new RaiseStarted("quiet"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var snap = await query.LoadAsync<RaiseSnap>(streamId, TestContext.Current.CancellationToken);
        snap.ShouldNotBeNull();
        snap.Label.ShouldBe("quiet");

        var audit = await query.Events.FetchStreamAsync(AuditStreamFor(streamId),
            token: TestContext.Current.CancellationToken);
        audit.Count.ShouldBe(0);
    }

    // A deterministic audit stream id per source stream, so the assertions can find it without
    // the projection having to report anything back.
    private static Guid AuditStreamFor(Guid streamId)
    {
        var bytes = streamId.ToByteArray();
        bytes[0] ^= 0xFF;
        return new Guid(bytes);
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    public record RaiseStarted(string Label);

    public record RaiseNoticed(string Label);

    public partial class RaiseSnap
    {
        public Guid Id { get; set; }
        public string Label { get; set; } = "";

        public void Apply(RaiseStarted e) => Label = e.Label;
    }

    public partial class RaisingProjection : SingleStreamProjection<RaiseSnap, Guid>
    {
        public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<RaiseSnap> slice)
        {
            // Raise one event per source event onto a separate audit stream — the ordinary
            // "projection notices something and records it" shape. IEventSlice<T> exposes no id of
            // its own, so the source stream comes off the events.
            foreach (var e in slice.Events().Where(x => x.Data is RaiseStarted))
            {
                var started = (RaiseStarted)e.Data;
                slice.AppendEvent(AuditStreamFor(e.StreamId), new RaiseNoticed(started.Label));
            }

            return ValueTask.CompletedTask;
        }
    }
}
