using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Events;

public record StaleOrdinalHappened(int Number);

public class StaleOrdinalDocument
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
///     #906: Weasel allocates a tenant's ordinal from the map it cached when it first loaded the registry,
///     and it never loads it again. So once another process has registered a tenant, the next new tenant
///     here asks for an ordinal that is taken, the unique index on the registry refuses it, and so does every
///     retry, because the map that produced the number has not changed. Each store is a process here: two
///     instances over one schema, with nothing shared but the database.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partition_stale_ordinal_tests : IAsyncLifetime
{
    private const string Schema = "pt_stale_ordinal";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(Action<StoreOptions>? configure = null) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionString;
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        opts.EventGraph.UseTenantPartitionedEvents = true;
        opts.Events.AddEventType<StaleOrdinalHappened>();
        configure?.Invoke(opts);
    });

    private static async Task<(DocumentStore a, DocumentStore b)> TwoLoadedStoresAsync()
    {
        var a = CreateStore();
        var b = CreateStore();

        await a.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        // Both load the registry here, and neither ever does again
        await AppendAsync(a, "incumbent");
        await AppendAsync(b, "incumbent");

        a.Events.TenantPartitionManager.Ordinals.ShouldContainKey("incumbent");
        b.Events.TenantPartitionManager.Ordinals.ShouldContainKey("incumbent");

        return (a, b);
    }

    private static async Task<Guid> AppendAsync(DocumentStore store, string tenant)
    {
        var streamId = Guid.NewGuid();

        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        session.Events.StartStream(streamId, new StaleOrdinalHappened(1));
        await session.SaveChangesAsync(Ct);

        return streamId;
    }

    private static async Task<IReadOnlyDictionary<string, int>> RegistryAsync()
    {
        var rows = await TestSchema.QueryAsync($"SELECT tenant_id, ordinal FROM [{Schema}].[pc_tenant_partitions]");
        return rows.ToDictionary(x => (string)x[0], x => (int)x[1]);
    }

    private static async Task<IReadOnlyList<int>> BoundariesOfAsync(string table)
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT CAST(v.value AS int)
            FROM sys.partition_range_values v
            JOIN sys.partition_functions f ON f.function_id = v.function_id
            WHERE f.name = 'pf_{table}_tenant_ordinal'
            """);
        return rows.Select(x => (int)x[0]).ToList();
    }

    private static async Task ShouldEachHaveTheirOwnPartitionAsync(params string[] tenants)
    {
        var registry = await RegistryAsync();

        var ordinals = tenants.Select(x => registry[x]).ToArray();
        ordinals.ShouldBeUnique();

        ordinals.ShouldBeSubsetOf(await BoundariesOfAsync("pc_events"));
        ordinals.ShouldBeSubsetOf(await BoundariesOfAsync("pc_streams"));
    }

    /// <summary>
    ///     #916. The removal asked the map it had, found no such tenant, and said so in a warning nobody
    ///     could read, because its logger was the null one. The tenant stayed registered and split.
    /// </summary>
    [Fact]
    public async Task a_store_can_remove_a_tenant_another_store_registered()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(b, "tenant_b");
            var ordinal = (await RegistryAsync())["tenant_b"];

            await a.Advanced.RemovePolecatManagedTenantsAsync(["tenant_b"], Ct);

            (await RegistryAsync()).ShouldNotContainKey("tenant_b");
            (await BoundariesOfAsync("pc_events")).ShouldNotContain(ordinal);
            (await BoundariesOfAsync("pc_streams")).ShouldNotContain(ordinal);
        }
    }

    [Fact]
    public async Task a_store_can_remove_a_tenant_with_its_data_when_another_store_registered_it()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            var streamId = await AppendAsync(b, "tenant_b");

            await a.Advanced.RemovePolecatManagedTenantsAsync(["tenant_b"],
                Weasel.SqlServer.Tables.Partitioning.TenantDropBehavior.DeleteData, Ct);

            var events = await TestSchema.QueryAsync(
                $"SELECT seq_id FROM [{Schema}].[pc_events] WHERE stream_id = '{streamId}'");
            events.ShouldBeEmpty();
        }
    }

    [Fact]
    public async Task a_store_can_add_a_tenant_after_another_store_took_the_next_ordinal()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(a, "tenant_a");

            var streamId = await AppendAsync(b, "tenant_b");

            await ShouldEachHaveTheirOwnPartitionAsync("incumbent", "tenant_a", "tenant_b");

            await using var query = b.QuerySession(new SessionOptions { TenantId = "tenant_b" });
            (await query.Events.FetchStreamAsync(streamId, token: Ct)).Count.ShouldBe(1);
        }
    }

    [Fact]
    public async Task the_loser_is_not_left_broken_for_later_tenants()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(a, "tenant_a");
            await AppendAsync(b, "tenant_b");
            await AppendAsync(a, "tenant_c");
            await AppendAsync(b, "tenant_d");
            await AppendAsync(a, "tenant_e");

            await ShouldEachHaveTheirOwnPartitionAsync("incumbent", "tenant_a", "tenant_b", "tenant_c", "tenant_d",
                "tenant_e");
        }
    }

    [Fact]
    public async Task a_tenant_the_other_store_registered_gets_the_ordinal_it_already_has()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(a, "tenant_a");
            await AppendAsync(b, "tenant_a");

            (await RegistryAsync()).Keys.ShouldBe(["incumbent", "tenant_a"], ignoreOrder: true);
        }
    }

    [Fact]
    public async Task two_stores_seeing_different_new_tenants_at_the_same_time()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            var tenants = Enumerable.Range(0, 12).Select(x => $"racing_{x}").ToArray();

            await Task.WhenAll(tenants.Select((tenant, i) => AppendAsync(i % 2 == 0 ? a : b, tenant)));

            await ShouldEachHaveTheirOwnPartitionAsync(tenants);
        }
    }

    [Fact]
    public async Task one_store_seeing_new_tenants_at_the_same_time()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var tenants = Enumerable.Range(0, 8).Select(x => $"racing_{x}").ToArray();

        await Task.WhenAll(tenants.Select(tenant => AppendAsync(store, tenant)));

        await ShouldEachHaveTheirOwnPartitionAsync(tenants);
    }

    [Fact]
    public async Task onboarding_a_tenant_explicitly_from_the_stale_store()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(a, "tenant_a");

            await b.Advanced.AddPolecatManagedTenantsAsync(Ct, "tenant_b");

            await ShouldEachHaveTheirOwnPartitionAsync("incumbent", "tenant_a", "tenant_b");
        }
    }

    [Fact]
    public async Task asking_for_a_taken_ordinal_is_refused_by_name_rather_than_retried_for_ever()
    {
        var (a, b) = await TwoLoadedStoresAsync();
        using (a)
        using (b)
        {
            await AppendAsync(a, "tenant_a");
            var taken = (await RegistryAsync())["tenant_a"];

            // Asked for explicitly, the number is not one a fresher map can improve on
            await Should.ThrowAsync<InvalidOperationException>(() => b.Advanced.AddPolecatManagedTenantsAsync(
                new Dictionary<string, int> { ["tenant_b"] = taken }, Ct));

            (await RegistryAsync()).ContainsKey("tenant_b").ShouldBeFalse();
        }
    }

    [Fact]
    public async Task a_document_write_that_provisions_from_a_stale_map()
    {
        static void Documents(StoreOptions opts)
        {
            opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
            opts.Schema.For<StaleOrdinalDocument>().MultiTenanted();
        }

        using var a = CreateStore(Documents);
        using var b = CreateStore(Documents);
        await a.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await AppendAsync(a, "incumbent");
        await AppendAsync(b, "incumbent");

        await AppendAsync(a, "tenant_a");

        var id = Guid.NewGuid();
        await using (var session = b.LightweightSession(new SessionOptions { TenantId = "tenant_b" }))
        {
            session.Store(new StaleOrdinalDocument { Id = id, Name = "stored" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = b.QuerySession(new SessionOptions { TenantId = "tenant_b" });
        (await query.LoadAsync<StaleOrdinalDocument>(id, Ct))!.Name.ShouldBe("stored");

        await ShouldEachHaveTheirOwnPartitionAsync("incumbent", "tenant_a", "tenant_b");
    }

    /// <summary>
    ///     What a duplicate on any other index looks like must keep propagating. A retry here would hide it.
    /// </summary>
    [Fact]
    public async Task the_index_being_retried_on_is_named_the_way_weasel_names_it()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var manager = store.Events.TenantPartitionManager;

        var rows = await TestSchema.QueryAsync($"""
            SELECT i.name FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID('[{Schema}].[pc_tenant_partitions]') AND i.is_unique = 1
              AND i.is_primary_key = 0
            """);

        rows.Single()[0].ShouldBe($"uniq_{manager.RegistryTableIdentifier.Name}_ordinal");
    }
}
