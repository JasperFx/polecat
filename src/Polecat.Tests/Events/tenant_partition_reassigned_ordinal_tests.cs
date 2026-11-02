using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Polecat.Events.Storage;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer.Tables.Partitioning;
using Weasel.Storage;

namespace Polecat.Tests.Events;

public record ReassignedOrdinalHappened(int Number);

/// <summary>
///     A stream row keeps the ordinal it was written with, and a tenant removed with <c>RetainData</c> and
///     added back can come back on a different one. The version update named the tenant's current
///     ordinal, so it matched nothing, the append's events went in anyway, and the row stayed behind them.
///     The next append read that old version, numbered its event the same as the last one and failed on the
///     unique index.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partition_reassigned_ordinal_tests : IAsyncLifetime
{
    private const string Schema = "pt_reassigned";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> CreateStoreAsync(StreamIdentity identity = StreamIdentity.AsGuid)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.StreamIdentity = identity;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<ReassignedOrdinalHappened>();
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static async Task MoveTenantToANewOrdinalAsync(DocumentStore store, string tenant)
    {
        await store.Advanced.RemovePolecatManagedTenantsAsync([tenant], TenantDropBehavior.RetainData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(new Dictionary<string, int> { [tenant] = 9 }, Ct);
    }

    private static async Task<long> StreamRowVersionAsync(string id)
    {
        var rows = await TestSchema.QueryAsync($"SELECT version FROM [{Schema}].[pc_streams] WHERE id = '{id}'");
        return (long)rows.Single()[0];
    }

    private static async Task<IReadOnlyList<long>> EventVersionsAsync(string id)
    {
        var rows = await TestSchema.QueryAsync(
            $"SELECT version FROM [{Schema}].[pc_events] WHERE stream_id = '{id}' ORDER BY version");
        return rows.Select(x => (long)x[0]).ToList();
    }

    [Fact]
    public async Task an_append_moves_the_version_of_a_row_left_on_an_old_ordinal()
    {
        using var store = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1), new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.Append(id, new ReassignedOrdinalHappened(3));
            await session.SaveChangesAsync(Ct);
        }

        (await StreamRowVersionAsync(id.ToString())).ShouldBe(3);
        (await EventVersionsAsync(id.ToString())).ShouldBe([1, 2, 3]);

        await using (var query = store.QuerySession("red"))
        {
            (await query.Events.FetchStreamStateAsync(id, Ct))!.Version.ShouldBe(3);
        }

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.Append(id, new ReassignedOrdinalHappened(4));
            await session.SaveChangesAsync(Ct);
        }

        (await StreamRowVersionAsync(id.ToString())).ShouldBe(4);
        (await EventVersionsAsync(id.ToString())).ShouldBe([1, 2, 3, 4]);
    }

    [Fact]
    public async Task a_string_stream_on_an_old_ordinal_moves_too()
    {
        using var store = await CreateStoreAsync(StreamIdentity.AsString);
        var key = Guid.NewGuid().ToString();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(key, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.Append(key, new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        (await StreamRowVersionAsync(key)).ShouldBe(2);
    }

    [Fact]
    public async Task a_stream_on_the_current_ordinal_is_updated_once()
    {
        using var store = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.Append(id, new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        (await StreamRowVersionAsync(id.ToString())).ShouldBe(2);
    }

    [Fact]
    public async Task an_update_that_finds_no_row_at_any_ordinal_fails_the_append()
    {
        using var store = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            () => AppendExpectingAsync(store, "red", id, expectedVersion: 5));

        (await StreamRowVersionAsync(id.ToString())).ShouldBe(1);
        (await EventVersionsAsync(id.ToString())).ShouldBe([1]);
    }

    [Fact]
    public async Task an_update_that_finds_no_row_fails_the_append_without_partitioning()
    {
        const string schema = "pt_reassigned_plain";
        await TestSchema.DropSchemaTablesAsync(schema);

        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.AddEventType<ReassignedOrdinalHappened>();
        });

        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await Should.ThrowAsync<EventStreamUnexpectedMaxEventIdException>(
            () => AppendExpectingAsync(store, StorageConstants.DefaultTenantId, id, expectedVersion: 5));

        var events = await TestSchema.QueryAsync($"SELECT version FROM [{schema}].[pc_events] WHERE stream_id = '{id}'");
        events.Count.ShouldBe(1);
    }

    /// <summary>
    ///     The planner reads the version under a lock, so it never gets this wrong by itself. This hands the
    ///     append operation what a stale read would have, which is the only way to reach the guard behind it.
    /// </summary>
    private static async Task AppendExpectingAsync(DocumentStore store, string tenant, Guid id, long expectedVersion)
    {
        await using var session = (DocumentSessionBase)store.LightweightSession(tenant);
        session.Events.Append(id, new ReassignedOrdinalHappened(99));

        var stream = session.WorkTracker.Streams.Single();
        stream.ExpectedVersionOnServer = expectedVersion;
        stream.Version = expectedVersion + 1;

        var @event = stream.Events.Single();
        @event.Id = Guid.NewGuid();
        @event.Version = stream.Version;
        @event.TenantId = tenant;
        @event.StreamId = id;
        @event.Timestamp = DateTimeOffset.UtcNow;

        var graph = store.Events;
        var operation = (PolecatQuickAppendEventsOperation)((EventStorage<Guid>)graph.ClosedShapeEventStorage)
            .QuickAppendEvents(stream);
        operation.Mode = StreamWriteMode.Update;

        if (graph.UseTenantPartitionedEvents)
        {
            var partition = await graph.TenantSequences.ResolveAsync(tenant, insideTransaction: false, Ct);
            operation.PartitionOrdinal = partition.Ordinal;
            operation.PartitionSequenceName = partition.SequenceName;
        }

        // The guard runs after the inserts, so like a save this leans on the transaction to take them back
        await session.BeginTransactionAsync(Ct);
        await session.ExecuteClosedShapeEventOperationsAsync([operation], Ct);
    }
}
