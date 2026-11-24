using JasperFx;
using JasperFx.Events;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.SqlServer.Tables.Partitioning;
using Weasel.Storage;

namespace Polecat.Tests.Events;

public record PerSchemaPartitionHappened(int Number);

/// <summary>
///     #1140 (weasel#920): a partition function and scheme belong to the DATABASE, and before Weasel 10.0 they
///     were named after the table alone (<c>pf_pc_events_tenant_ordinal</c>), so two stores in different schemas
///     of one database put their tenant-partitioned <c>pc_events</c> on the very same function. Each store keeps
///     its own tenant registry and ordinals, so one store's <c>RemovePolecatManagedTenantsAsync</c> MERGE took a
///     boundary out from under the other store's tables. Weasel 10.0 puts the schema in the name, so each store's
///     tables get their own function. The assertions read the function each table is actually bound to in the
///     catalog rather than a name, so they hold the behavior, not the naming.
/// </summary>
[Collection("tenant-partitioning")]
public class partition_objects_per_schema_tests : IAsyncLifetime
{
    private const string SchemaA = "pt_per_schema_a";
    private const string SchemaB = "pt_per_schema_b";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(SchemaA);
        await TestSchema.DropSchemaTablesAsync(SchemaB);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(SchemaA);
        await TestSchema.DropSequencesAsync(SchemaB);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static async Task<DocumentStore> CreateStoreAsync(string schema)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
            opts.Events.AddEventType<PerSchemaPartitionHappened>();
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        return store;
    }

    private static async Task StartAsync(DocumentStore store, string tenant, Guid id)
    {
        await using var session = store.LightweightSession(tenant);
        session.Events.StartStream(id, new PerSchemaPartitionHappened(1), new PerSchemaPartitionHappened(2));
        await session.SaveChangesAsync(Ct);
    }

    /// <summary>The partition function the schema's <c>pc_events</c> clustered index is on, by the catalog.</summary>
    private static async Task<string> BoundFunctionAsync(string schema)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT pf.name
            FROM sys.tables t
            JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
            JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
            JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
            WHERE t.name = 'pc_events' AND SCHEMA_NAME(t.schema_id) = '{schema}'
            """);
        return (string)rows.Single()[0];
    }

    private static async Task<IReadOnlyList<int>> BoundariesAsync(string function)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM sys.partition_range_values v
            JOIN sys.partition_functions f ON f.function_id = v.function_id
            WHERE f.name = '{function}'
            ORDER BY v.boundary_id
            """);
        return rows.Select(x => (int)x[0]).ToList();
    }

    [Fact]
    public async Task each_schema_gets_its_own_partition_function()
    {
        using var a = await CreateStoreAsync(SchemaA);
        using var b = await CreateStoreAsync(SchemaB);

        var functionA = await BoundFunctionAsync(SchemaA);
        var functionB = await BoundFunctionAsync(SchemaB);

        functionA.ShouldNotBe(functionB);
        functionA.ShouldContain(SchemaA);
        functionB.ShouldContain(SchemaB);
    }

    [Fact]
    public async Task removing_a_tenant_in_one_store_leaves_the_other_stores_partitions_alone()
    {
        using var a = await CreateStoreAsync(SchemaA);
        using var b = await CreateStoreAsync(SchemaB);

        // Both stores' registries hand "red" the same ordinal, so on a shared function they'd share its boundary
        await StartAsync(a, "red", Guid.NewGuid());
        var kept = Guid.NewGuid();
        await StartAsync(b, "red", kept);

        var functionB = await BoundFunctionAsync(SchemaB);
        var before = await BoundariesAsync(functionB);
        before.ShouldNotBeEmpty();

        await a.Advanced.RemovePolecatManagedTenantsAsync(["red"], TenantDropBehavior.DeleteData, Ct);

        (await BoundariesAsync(functionB)).ShouldBe(before);

        // And store B's tenant still works on its own partition
        await using (var session = b.LightweightSession("red"))
        {
            session.Events.Append(kept, new PerSchemaPartitionHappened(3));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = b.QuerySession("red");
        (await query.Events.FetchStreamAsync(kept, token: Ct)).Count.ShouldBe(3);
    }
}
