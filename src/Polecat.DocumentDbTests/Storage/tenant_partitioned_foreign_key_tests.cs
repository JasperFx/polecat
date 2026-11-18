using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.Core;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Tests.Storage;

public class PartitionedFkCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class PartitionedFkOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
}

/// <summary>
///     #1075: a foreign key between two conjoined document types under
///     <c>PartitionMultiTenantedDocumentsUsingPolecatManagement()</c>. The partitioned table's primary key is
///     <c>(tenant_id, id, tenant_ordinal)</c>, so the foreign key references all three.
/// </summary>
[Collection("tenant-partitioning")]
public class tenant_partitioned_foreign_key_tests : IAsyncLifetime
{
    private const string Schema = "pt_fk";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await TestSchema.DropSchemaTablesAsync(Schema);
        await PartitionTestCleanup.DropEventsPartitionObjectsAsync();
        await TestSchema.DropSequencesAsync(Schema);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore(bool partitioned, Action<StoreOptions>? configure = null)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Schema.For<PartitionedFkCustomer>().MultiTenanted();
            opts.Schema.For<PartitionedFkOrder>().MultiTenanted().ForeignKey<PartitionedFkCustomer>(x => x.CustomerId);

            if (partitioned)
            {
                opts.Policies.PartitionMultiTenantedDocumentsUsingPolecatManagement();
            }

            configure?.Invoke(opts);
        });
    }

    private static async Task<PartitionedFkCustomer> storeCustomerAsync(DocumentStore store, string tenantId)
    {
        var customer = new PartitionedFkCustomer { Id = Guid.NewGuid(), Name = tenantId };
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        session.Store(customer);
        await session.SaveChangesAsync(Ct);
        return customer;
    }

    private static async Task storeOrderAsync(DocumentStore store, string tenantId, Guid customerId)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenantId });
        session.Store(new PartitionedFkOrder { Id = Guid.NewGuid(), CustomerId = customerId });
        await session.SaveChangesAsync(Ct);
    }

    private static async Task<(string[] Columns, string[] Referenced)> foreignKeyColumnsAsync()
    {
        var rows = await TestSchema.QueryAsync($"""
            SELECT pc.name, rc.name
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
            JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
            WHERE fk.parent_object_id = OBJECT_ID('[{Schema}].[pc_doc_partitionedfkorder]')
            ORDER BY fkc.constraint_column_id
            """);

        return (rows.Select(x => (string)x[0]).ToArray(), rows.Select(x => (string)x[1]).ToArray());
    }

    private static async Task<int> countAsync(string table, string tenantId)
    {
        var rows = await TestSchema.QueryAsync(
            $"SELECT COUNT(*) FROM [{Schema}].[{table}] WHERE tenant_id = '{tenantId}'");
        return (int)rows[0][0];
    }

    [Fact]
    public async Task foreign_key_between_two_partitioned_tables_references_the_tenant_ordinal_too()
    {
        using var store = CreateStore(partitioned: true);

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await store.Database.AssertDatabaseMatchesConfigurationAsync(Ct);

        var (columns, referenced) = await foreignKeyColumnsAsync();
        columns.ShouldBe(["tenant_id", "cc_customerid", "tenant_ordinal"]);
        referenced.ShouldBe(["tenant_id", "id", "tenant_ordinal"]);
    }

    [Fact]
    public async Task a_same_tenant_reference_saves_and_a_dangling_one_is_rejected()
    {
        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var customer = await storeCustomerAsync(store, "Red");
        await storeOrderAsync(store, "Red", customer.Id);
        (await countAsync("pc_doc_partitionedfkorder", "Red")).ShouldBe(1);

        var dangling = await Should.ThrowAsync<SqlException>(() => storeOrderAsync(store, "Red", Guid.NewGuid()));
        dangling.Number.ShouldBe(547);

        // The customer exists, but in another tenant.
        var crossTenant = await Should.ThrowAsync<SqlException>(() => storeOrderAsync(store, "Blue", customer.Id));
        crossTenant.Number.ShouldBe(547);

        (await countAsync("pc_doc_partitionedfkorder", "Red")).ShouldBe(1);
        (await countAsync("pc_doc_partitionedfkorder", "Blue")).ShouldBe(0);
    }

    [Fact]
    public async Task a_partitioned_table_may_reference_one_that_opted_out()
    {
        using var store = CreateStore(partitioned: true,
            opts => opts.Policies.ForDocument<PartitionedFkCustomer>(p => p.DisableTenantPartitioning = true));

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await store.Database.AssertDatabaseMatchesConfigurationAsync(Ct);

        var (columns, referenced) = await foreignKeyColumnsAsync();
        columns.ShouldBe(["tenant_id", "cc_customerid"]);
        referenced.ShouldBe(["tenant_id", "id"]);

        var customer = await storeCustomerAsync(store, "Red");
        await storeOrderAsync(store, "Red", customer.Id);

        var dangling = await Should.ThrowAsync<SqlException>(() => storeOrderAsync(store, "Red", Guid.NewGuid()));
        dangling.Number.ShouldBe(547);
    }

    [Fact]
    public void an_opted_out_table_referencing_a_partitioned_one_is_refused()
    {
        var ex = Should.Throw<NotSupportedException>(() => CreateStore(partitioned: true,
            opts => opts.Policies.ForDocument<PartitionedFkOrder>(p => p.DisableTenantPartitioning = true)));

        ex.Message.ShouldContain("PartitionMultiTenantedDocumentsUsingPolecatManagement()");
        ex.Message.ShouldContain("Policies.ForDocument<PartitionedFkOrder>(p => p.DisableTenantPartitioning = true)");
    }

    [Fact]
    public async Task removing_a_tenant_with_delete_data_purges_both_tables()
    {
        using var store = CreateStore(partitioned: true);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var doomed = await storeCustomerAsync(store, "Doomed");
        await storeOrderAsync(store, "Doomed", doomed.Id);

        var kept = await storeCustomerAsync(store, "Kept");
        await storeOrderAsync(store, "Kept", kept.Id);

        await store.Advanced.RemovePolecatManagedTenantsAsync(["Doomed"], TenantDropBehavior.DeleteData, Ct);

        (await countAsync("pc_doc_partitionedfkcustomer", "Doomed")).ShouldBe(0);
        (await countAsync("pc_doc_partitionedfkorder", "Doomed")).ShouldBe(0);
        (await countAsync("pc_doc_partitionedfkcustomer", "Kept")).ShouldBe(1);
        (await countAsync("pc_doc_partitionedfkorder", "Kept")).ShouldBe(1);

        // Every boundary of the released ordinal is gone, so the tenant comes back cleanly.
        var customer = await storeCustomerAsync(store, "Doomed");
        await storeOrderAsync(store, "Doomed", customer.Id);
    }

    [Fact]
    public async Task without_partitioning_the_foreign_key_is_unchanged()
    {
        using var store = CreateStore(partitioned: false);

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await store.Database.AssertDatabaseMatchesConfigurationAsync(Ct);

        var (columns, referenced) = await foreignKeyColumnsAsync();
        columns.ShouldBe(["tenant_id", "cc_customerid"]);
        referenced.ShouldBe(["tenant_id", "id"]);

        var customer = await storeCustomerAsync(store, "Red");
        await storeOrderAsync(store, "Red", customer.Id);

        var dangling = await Should.ThrowAsync<SqlException>(() => storeOrderAsync(store, "Red", Guid.NewGuid()));
        dangling.Number.ShouldBe(547);
    }
}
