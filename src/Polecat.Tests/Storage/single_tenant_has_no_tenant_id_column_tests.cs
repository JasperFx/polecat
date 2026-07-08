using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Storage;

// #234: single-tenant (non-conjoined) document tables do NOT carry a tenant_id column, matching
// Marten — every document lives under the default tenant implicitly and no per-query tenant filter
// is applied. Conjoined tenancy still adds tenant_id (as part of the composite primary key).
[Collection("integration")]
public class single_tenant_has_no_tenant_id_column_tests : IntegrationContext
{
    public single_tenant_has_no_tenant_id_column_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public class Widget
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task single_tenant_table_has_no_tenant_id_column()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "no_tenant_col"; });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Widget { Id = Guid.NewGuid(), Name = "A" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_widget", "no_tenant_col");
        var names = columns.Select(c => c.Name).ToList();

        names.ShouldNotContain("tenant_id");
        // The standard columns are still there, including the always-present version column.
        names.ShouldContain("id");
        names.ShouldContain("data");
        names.ShouldContain("version");
        names.ShouldContain("last_modified");
        names.ShouldContain("created_at");
        names.ShouldContain("dotnet_type");
    }

    [Fact]
    public async Task conjoined_table_still_has_tenant_id_column()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "with_tenant_col";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Widget { Id = Guid.NewGuid(), Name = "A" });
            await session.SaveChangesAsync();
        }

        var columns = await SchemaInspector.GetColumnInfoAsync("pc_doc_widget", "with_tenant_col");
        columns.Select(c => c.Name).ShouldContain("tenant_id");
    }

    // The full document lifecycle must work on a single-tenant table with no tenant_id column:
    // store, load (Lightweight writeable selector + QueryOnly), LINQ query, metadata, and delete.
    [Fact]
    public async Task full_lifecycle_works_without_tenant_id_column()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "no_tenant_lifecycle"; });

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Widget { Id = id, Name = "Original" });
            await session.SaveChangesAsync();
        }

        // Lightweight database read (writeable selector).
        await using (var lightweight = theStore.LightweightSession())
        {
            var loaded = await lightweight.LoadAsync<Widget>(id);
            loaded.ShouldNotBeNull();
            loaded!.Name.ShouldBe("Original");
        }

        // QueryOnly + LINQ (no implicit tenant filter).
        await using (var query = theStore.QuerySession())
        {
            var byLinq = await query.Query<Widget>().Where(w => w.Name == "Original").ToListAsync();
            byLinq.Count.ShouldBe(1);

            var meta = await query.MetadataForAsync(new Widget { Id = id });
            meta.ShouldNotBeNull();
            meta!.TenantId.ShouldBe(StorageConstants.DefaultTenantId);
        }

        // Delete.
        await using (var session = theStore.LightweightSession())
        {
            session.Delete<Widget>(id);
            await session.SaveChangesAsync();
        }

        await using (var query = theStore.QuerySession())
        {
            (await query.LoadAsync<Widget>(id)).ShouldBeNull();
        }
    }
}
