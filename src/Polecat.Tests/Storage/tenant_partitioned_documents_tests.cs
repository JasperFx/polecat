using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Tests.Storage;

public class PartitionedTenantDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class OptedOutTenantDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
///     Covers #335 scopes 1 + 3 + 4 — managed per-tenant partitioning of conjoined document tables:
///     the AllDocumentsAreMultiTenantedWithPartitioning / PartitionMultiTenantedDocumentsUsingPolecatManagement
///     policies, the tenant_ordinal schema shape + server-side ordinal resolution on every write path
///     (upsert, insert, update, bulk), per-type opt-out, config guards, and the
///     AddPolecatManagedTenantsAsync / RemovePolecatManagedTenantsAsync runtime onboarding APIs with
///     TenantDropBehavior semantics.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partitioned_documents_tests : IAsyncLifetime
{
    private const string Schema = "pt_docs";

    public async Task InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore(Action<StoreOptions>? configure = null)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Policies.AllDocumentsAreMultiTenantedWithPartitioning();
            configure?.Invoke(opts);
        });
    }

    [Fact]
    public void all_documents_policy_forces_conjoined_tenancy()
    {
        using var store = CreateStore();
        store.Options.Events.TenancyStyle.ShouldBe(TenancyStyle.Conjoined);
    }

    [Fact]
    public void management_only_policy_requires_conjoined_tenancy()
    {
        var ex = Should.Throw<InvalidOperationException>(() => DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            // TenancyStyle left at the default (Single)
            opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
        }));

        ex.Message.ShouldContain("Conjoined");
    }

    [Fact]
    public async Task document_table_is_partitioned_and_writes_carry_the_tenant_ordinal()
    {
        using var store = CreateStore();

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "red-1" });
            red.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "red-2" });
            await red.SaveChangesAsync();
        }

        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "blue-1" });
            await blue.SaveChangesAsync();
        }

        // The table carries tenant_ordinal, and every row's ordinal matches its tenant's
        // registry assignment — SQL Server's $PARTITION proves physical placement (RANGE RIGHT:
        // ordinal N lands in partition N + 1).
        (await TestSchema.ColumnExistsAsync(Schema, "pc_doc_partitionedtenantdoc", "tenant_ordinal"))
            .ShouldBeTrue();

        var rows = await TestSchema.QueryAsync($"""
            SELECT d.tenant_id, d.tenant_ordinal, tp.ordinal,
                   $PARTITION.pf_pc_doc_partitionedtenantdoc_tenant_ordinal(d.tenant_ordinal) AS p,
                   COUNT(*) AS c
            FROM [{Schema}].[pc_doc_partitionedtenantdoc] d
            JOIN [{Schema}].[pc_tenant_partitions] tp ON tp.tenant_id = d.tenant_id
            GROUP BY d.tenant_id, d.tenant_ordinal, tp.ordinal,
                     $PARTITION.pf_pc_doc_partitionedtenantdoc_tenant_ordinal(d.tenant_ordinal)
            ORDER BY d.tenant_id
            """);

        rows.Count.ShouldBe(2);
        rows.Select(r => r[3]).Distinct().Count().ShouldBe(2); // distinct physical partitions
        foreach (var row in rows)
        {
            ((int)row[1]).ShouldBe((int)row[2]); // row ordinal == registry ordinal
        }

        rows.Single(r => (string)r[0] == "Red")[4].ShouldBe(2);
        rows.Single(r => (string)r[0] == "Blue")[4].ShouldBe(1);
    }

    [Fact]
    public async Task load_update_and_delete_round_trip_per_tenant()
    {
        using var store = CreateStore();
        var redId = Guid.NewGuid();
        var blueId = Guid.NewGuid();

        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Store(new PartitionedTenantDoc { Id = redId, Name = "red" });
            await red.SaveChangesAsync();
        }

        await using (var blue = store.LightweightSession(new SessionOptions { TenantId = "Blue" }))
        {
            blue.Store(new PartitionedTenantDoc { Id = blueId, Name = "blue" });
            await blue.SaveChangesAsync();
        }

        // Cross-tenant isolation on loads.
        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await redQuery.LoadAsync<PartitionedTenantDoc>(redId))!.Name.ShouldBe("red");
            (await redQuery.LoadAsync<PartitionedTenantDoc>(blueId)).ShouldBeNull();
        }

        // Update through the MERGE matched branch (partition-eliminated by tenant_ordinal).
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Store(new PartitionedTenantDoc { Id = redId, Name = "red-updated" });
            await red.SaveChangesAsync();
        }

        // session.Update (the update-only SQL path).
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Update(new PartitionedTenantDoc { Id = redId, Name = "red-updated-again" });
            await red.SaveChangesAsync();
        }

        // session.Insert (the insert-only MERGE path).
        var redSecondId = Guid.NewGuid();
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Insert(new PartitionedTenantDoc { Id = redSecondId, Name = "red-inserted" });
            await red.SaveChangesAsync();
        }

        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await redQuery.LoadAsync<PartitionedTenantDoc>(redId))!.Name.ShouldBe("red-updated-again");
            (await redQuery.LoadAsync<PartitionedTenantDoc>(redSecondId))!.Name.ShouldBe("red-inserted");
        }

        // Delete only touches the owning tenant.
        await using (var red = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            red.Delete<PartitionedTenantDoc>(redId);
            await red.SaveChangesAsync();
        }

        await using (var redQuery = store.QuerySession(new SessionOptions { TenantId = "Red" }))
        {
            (await redQuery.LoadAsync<PartitionedTenantDoc>(redId)).ShouldBeNull();
        }

        await using (var blueQuery = store.QuerySession(new SessionOptions { TenantId = "Blue" }))
        {
            (await blueQuery.LoadAsync<PartitionedTenantDoc>(blueId))!.Name.ShouldBe("blue");
        }
    }

    [Fact]
    public async Task for_tenant_writes_resolve_the_override_tenants_ordinal()
    {
        using var store = CreateStore();
        var greenId = Guid.NewGuid();

        // A session for one tenant writing via ForTenant for another must provision + stamp the
        // OVERRIDE tenant's ordinal.
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            session.ForTenant("Green").Store(new PartitionedTenantDoc { Id = greenId, Name = "green" });
            await session.SaveChangesAsync();
        }

        var rows = await TestSchema.QueryAsync($"""
            SELECT d.tenant_ordinal, tp.ordinal
            FROM [{Schema}].[pc_doc_partitionedtenantdoc] d
            JOIN [{Schema}].[pc_tenant_partitions] tp ON tp.tenant_id = 'Green'
            WHERE d.tenant_id = 'Green'
            """);
        rows.Count.ShouldBe(1);
        ((int)rows[0][0]).ShouldBe((int)rows[0][1]);

        await using var query = store.QuerySession(new SessionOptions { TenantId = "Green" });
        (await query.LoadAsync<PartitionedTenantDoc>(greenId))!.Name.ShouldBe("green");
    }

    [Fact]
    public async Task bulk_insert_provisions_and_stamps_the_tenant_ordinal()
    {
        using var store = CreateStore();

        var docs = Enumerable.Range(1, 5)
            .Select(i => new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = $"bulk-{i}" })
            .ToArray();
        await store.Advanced.BulkInsertAsync(docs, BulkInsertMode.InsertsOnly, 2, "Bulky");

        var rows = await TestSchema.QueryAsync($"""
            SELECT COUNT(*)
            FROM [{Schema}].[pc_doc_partitionedtenantdoc] d
            JOIN [{Schema}].[pc_tenant_partitions] tp ON tp.tenant_id = d.tenant_id
            WHERE d.tenant_id = 'Bulky' AND d.tenant_ordinal = tp.ordinal
            """);
        ((int)rows[0][0]).ShouldBe(5);
    }

    [Fact]
    public async Task per_type_opt_out_keeps_a_plain_conjoined_table()
    {
        using var store = CreateStore(opts =>
            opts.Policies.ForDocument<OptedOutTenantDoc>(p => p.DisableTenantPartitioning = true));

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Red" }))
        {
            session.Store(new OptedOutTenantDoc { Id = Guid.NewGuid(), Name = "plain" });
            await session.SaveChangesAsync();
        }

        (await TestSchema.ColumnExistsAsync(Schema, "pc_doc_optedouttenantdoc", "tenant_ordinal"))
            .ShouldBeFalse();
    }

    [Fact]
    public void combining_with_range_partitioning_throws()
    {
        var ex = Should.Throw<NotSupportedException>(() => CreateStore(opts =>
            opts.Schema.For<PartitionedTenantDoc>()
                .PartitionByRange(x => x.Id, Guid.NewGuid())));

        ex.Message.ShouldContain("one partition scheme");
    }

    [Fact]
    public async Task add_managed_tenants_reports_per_table_statuses_and_splits_registered_tables()
    {
        using var store = CreateStore(opts => opts.Schema.For<PartitionedTenantDoc>());

        // Materialize the registered document table (plus the shared registry) up front.
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        var statuses = await store.Advanced.AddPolecatManagedTenantsAsync(
            CancellationToken.None, "t1", "t2");

        statuses.ShouldNotBeEmpty();
        statuses.ShouldContain(s =>
            s.Identifier.QualifiedName == $"{Schema}.pc_doc_partitionedtenantdoc"
            && s.Status == PartitionMigrationStatus.Complete);

        // Idempotent — a second add of the same tenants emits no failures and keeps ordinals.
        var again = await store.Advanced.AddPolecatManagedTenantsAsync(CancellationToken.None, "t1");
        again.ShouldAllBe(s => s.Status == PartitionMigrationStatus.Complete);

        var registry = await TestSchema.QueryAsync(
            $"SELECT tenant_id, ordinal FROM [{Schema}].[pc_tenant_partitions] ORDER BY ordinal");
        registry.Count.ShouldBe(2);

        // Writes for an onboarded tenant flow straight through (cached or registry-resolved).
        await using var session = store.LightweightSession(new SessionOptions { TenantId = "t1" });
        session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "onboarded" });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task remove_managed_tenants_with_delete_data_purges_the_tenants_rows()
    {
        using var store = CreateStore();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Doomed" }))
        {
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "doomed-1" });
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "doomed-2" });
            await session.SaveChangesAsync();
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Kept" }))
        {
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "kept" });
            await session.SaveChangesAsync();
        }

        await store.Advanced.RemovePolecatManagedTenantsAsync(
            ["Doomed"], TenantDropBehavior.DeleteData);

        // Doomed's rows are physically gone, Kept's remain; the registry row is dropped.
        var rows = await TestSchema.QueryAsync(
            $"SELECT tenant_id, COUNT(*) FROM [{Schema}].[pc_doc_partitionedtenantdoc] GROUP BY tenant_id");
        rows.Count.ShouldBe(1);
        ((string)rows[0][0]).ShouldBe("Kept");

        var registry = await TestSchema.QueryAsync(
            $"SELECT tenant_id FROM [{Schema}].[pc_tenant_partitions]");
        registry.Count.ShouldBe(1);
        ((string)registry[0][0]).ShouldBe("Kept");

        // A removed tenant can be re-onboarded (fresh ordinal) and written again — the
        // in-process caches were evicted.
        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Doomed" }))
        {
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "reborn" });
            await session.SaveChangesAsync();
        }

        await using var query = store.QuerySession(new SessionOptions { TenantId = "Doomed" });
        var reborn = await query.Query<PartitionedTenantDoc>().ToListAsync();
        reborn.Count.ShouldBe(1);
        reborn[0].Name.ShouldBe("reborn");
    }

    [Fact]
    public async Task remove_managed_tenants_with_retain_data_keeps_the_rows()
    {
        using var store = CreateStore();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Merged" }))
        {
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "merged" });
            await session.SaveChangesAsync();
        }

        await store.Advanced.RemovePolecatManagedTenantsAsync(["Merged"]);

        // Historical merge-only semantics: the boundary is gone but the rows survive.
        var rows = await TestSchema.QueryAsync(
            $"SELECT COUNT(*) FROM [{Schema}].[pc_doc_partitionedtenantdoc] WHERE tenant_id = 'Merged'");
        ((int)rows[0][0]).ShouldBe(1);

        var registry = await TestSchema.QueryAsync(
            $"SELECT COUNT(*) FROM [{Schema}].[pc_tenant_partitions] WHERE tenant_id = 'Merged'");
        ((int)registry[0][0]).ShouldBe(0);
    }

    [Fact]
    public async Task events_streams_and_documents_share_one_registry_and_ordinal()
    {
        using var store = CreateStore(opts =>
        {
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.EventGraph.UseTenantPartitionedEvents = true;
        });

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = "Shared" }))
        {
            session.Events.StartStream(Guid.NewGuid(), new QuestStarted("Shared Quest"));
            session.Store(new PartitionedTenantDoc { Id = Guid.NewGuid(), Name = "shared" });
            await session.SaveChangesAsync();
        }

        // One registry row; pc_events, pc_streams, and the document table all stamp the SAME ordinal.
        var rows = await TestSchema.QueryAsync($"""
            SELECT tp.ordinal,
                   (SELECT DISTINCT tenant_ordinal FROM [{Schema}].[pc_events] WHERE tenant_id = 'Shared'),
                   (SELECT DISTINCT tenant_ordinal FROM [{Schema}].[pc_streams] WHERE tenant_id = 'Shared'),
                   (SELECT DISTINCT tenant_ordinal FROM [{Schema}].[pc_doc_partitionedtenantdoc] WHERE tenant_id = 'Shared')
            FROM [{Schema}].[pc_tenant_partitions] tp WHERE tp.tenant_id = 'Shared'
            """);

        rows.Count.ShouldBe(1);
        var ordinal = (int)rows[0][0];
        ((int)rows[0][1]).ShouldBe(ordinal);
        ((int)rows[0][2]).ShouldBe(ordinal);
        ((int)rows[0][3]).ShouldBe(ordinal);
    }
}
