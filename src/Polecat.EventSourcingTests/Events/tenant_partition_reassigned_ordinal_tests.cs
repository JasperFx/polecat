using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Polecat.Events.Storage;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer.Tables.Partitioning;
using Weasel.Storage;
using ExistingStreamIdCollisionException = JasperFx.Events.ExistingStreamIdCollisionException;

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

    [Fact]
    public async Task starting_a_stream_that_is_on_an_old_ordinal_collides()
    {
        using var store = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(2));
            var collision = await Should.ThrowAsync<ExistingStreamIdCollisionException>(
                () => session.SaveChangesAsync(Ct));
            collision.Id.ShouldBe(id);
        }

        (await StreamRowVersionAsync(id.ToString())).ShouldBe(1);
        (await EventVersionsAsync(id.ToString())).ShouldBe([1]);
    }

    [Fact]
    public async Task starting_a_string_stream_that_is_on_an_old_ordinal_collides()
    {
        using var store = await CreateStoreAsync(StreamIdentity.AsString);
        var key = Guid.NewGuid().ToString();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(key, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using var second = store.LightweightSession("red");
        second.Events.StartStream(key, new ReassignedOrdinalHappened(2));
        await Should.ThrowAsync<ExistingStreamIdCollisionException>(() => second.SaveChangesAsync(Ct));

        (await EventVersionsAsync(key)).ShouldBe([1]);
    }

    [Fact]
    public async Task a_collision_on_an_old_ordinal_names_the_stream_that_collided()
    {
        using var store = await CreateStoreAsync();
        var existing = Guid.NewGuid();
        var fresh = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(existing, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using var second = store.LightweightSession("red");
        second.Events.StartStream(fresh, new ReassignedOrdinalHappened(2));
        second.Events.StartStream(existing, new ReassignedOrdinalHappened(3));

        var collision = await Should.ThrowAsync<ExistingStreamIdCollisionException>(
            () => second.SaveChangesAsync(Ct));

        collision.Id.ShouldBe(existing);
        (await EventVersionsAsync(fresh.ToString())).ShouldBeEmpty();
    }

    [Fact]
    public async Task starting_a_new_stream_after_the_move_still_works()
    {
        using var store = await CreateStoreAsync();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(Guid.NewGuid(), new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        (await EventVersionsAsync(id.ToString())).ShouldBe([1]);
    }

    private static async Task<(string Id, DocumentStore Store)> StreamAcrossTwoOrdinalsAsync(
        StreamIdentity identity = StreamIdentity.AsGuid)
    {
        var store = await CreateStoreAsync(identity);
        var id = Guid.NewGuid();
        var key = id.ToString();
        var isGuid = identity == StreamIdentity.AsGuid;

        await using (var session = store.LightweightSession("red"))
        {
            if (isGuid) session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            else session.Events.StartStream(key, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await MoveTenantToANewOrdinalAsync(store, "red");

        await using (var session = store.LightweightSession("red"))
        {
            if (isGuid) session.Events.Append(id, new ReassignedOrdinalHappened(2));
            else session.Events.Append(key, new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        var ordinals = await TestSchema.QueryAsync(
            $"SELECT DISTINCT tenant_ordinal FROM [{Schema}].[pc_events] WHERE stream_id = '{key}'");
        ordinals.Count.ShouldBe(2, "the stream has to span two ordinals for these facts to say anything");

        return (key, store);
    }

    [Fact]
    public async Task fetching_a_stream_reads_every_ordinal_it_is_on()
    {
        var (key, store) = await StreamAcrossTwoOrdinalsAsync();
        using var _ = store;

        await using var query = store.QuerySession("red");
        var events = await query.Events.FetchStreamAsync(Guid.Parse(key), token: Ct);

        events.Select(x => x.Version).ShouldBe([1, 2]);
    }

    [Fact]
    public async Task fetching_a_string_stream_reads_every_ordinal_it_is_on()
    {
        var (key, store) = await StreamAcrossTwoOrdinalsAsync(StreamIdentity.AsString);
        using var _ = store;

        await using var query = store.QuerySession("red");
        var events = await query.Events.FetchStreamAsync(key, token: Ct);

        events.Select(x => x.Version).ShouldBe([1, 2]);
    }

    [Fact]
    public async Task a_batched_fetch_reads_every_ordinal_a_stream_is_on()
    {
        var (key, store) = await StreamAcrossTwoOrdinalsAsync();
        using var _ = store;

        await using var query = store.QuerySession("red");
        var batch = query.CreateBatchQuery();
        var fetched = batch.Events.FetchStream(Guid.Parse(key));
        var filtered = batch.Events.FetchStream(Guid.Parse(key), fromVersion: 2);
        await batch.Execute(Ct);

        (await fetched).Select(x => x.Version).ShouldBe([1, 2]);
        (await filtered).Select(x => x.Version).ShouldBe([2]);
    }

    [Fact]
    public async Task a_batched_fetch_of_a_string_stream_binds_it_as_one()
    {
        var (key, store) = await StreamAcrossTwoOrdinalsAsync(StreamIdentity.AsString);
        using var _ = store;

        await using var query = store.QuerySession("red");
        var batch = query.CreateBatchQuery();
        var fetched = batch.Events.FetchStream(key);
        await batch.Execute(Ct);

        (await fetched).Select(x => x.Version).ShouldBe([1, 2]);
    }

    /// <summary>
    ///     The row is on the tenant's current ordinal again, and the newest event isn't. Reading only the
    ///     ordinal the row is on would lose it without a word.
    /// </summary>
    [Fact]
    public async Task a_tenant_back_on_an_old_ordinal_still_reads_the_events_from_in_between()
    {
        var (key, store) = await StreamAcrossTwoOrdinalsAsync();
        using var _ = store;

        var first = await TestSchema.QueryAsync(
            $"SELECT tenant_ordinal FROM [{Schema}].[pc_events] WHERE stream_id = '{key}' AND version = 1");

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.RetainData, Ct);
        await store.Advanced.AddPolecatManagedTenantsAsync(
            new Dictionary<string, int> { ["red"] = (int)first.Single()[0] }, Ct);

        await using var query = store.QuerySession("red");
        (await query.Events.FetchStreamAsync(Guid.Parse(key), token: Ct))
            .Select(x => x.Version).ShouldBe([1, 2]);
    }

    [Fact]
    public async Task a_process_that_has_never_written_the_tenant_reads_its_stream()
    {
        using var writer = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = writer.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1), new ReassignedOrdinalHappened(2));
            await session.SaveChangesAsync(Ct);
        }

        using var reader = await CreateStoreAsync();
        reader.Events.TenantOrdinals.TryGetOrdinal("red", out _).ShouldBeFalse();

        await using var query = reader.QuerySession("red");
        (await query.Events.FetchStreamAsync(id, token: Ct)).Select(x => x.Version).ShouldBe([1, 2]);

        reader.Events.TenantOrdinals.TryGetOrdinal("red", out _).ShouldBeFalse("a read must not register a tenant");
    }

    [Fact]
    public async Task a_tenant_removed_with_its_data_retained_can_still_read_it()
    {
        using var store = await CreateStoreAsync();
        var id = Guid.NewGuid();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(id, new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await store.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.RetainData, Ct);

        await using var query = store.QuerySession("red");
        (await query.Events.FetchStreamAsync(id, token: Ct)).Select(x => x.Version).ShouldBe([1]);
    }

    [Fact]
    public async Task fetching_a_stream_that_is_not_there_is_empty()
    {
        using var store = await CreateStoreAsync();

        await using (var session = store.LightweightSession("red"))
        {
            session.Events.StartStream(Guid.NewGuid(), new ReassignedOrdinalHappened(1));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = store.QuerySession("red");
        (await query.Events.FetchStreamAsync(Guid.NewGuid(), token: Ct)).ShouldBeEmpty();

        await using var unknown = store.QuerySession("nobody");
        (await unknown.Events.FetchStreamAsync(Guid.NewGuid(), token: Ct)).ShouldBeEmpty();
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
