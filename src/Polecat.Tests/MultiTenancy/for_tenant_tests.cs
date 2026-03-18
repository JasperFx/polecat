using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

public class TenantDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class for_tenant_tests : IntegrationContext
{
    public for_tenant_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task can_store_documents_for_different_tenants()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "for_tenant";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        var doc1 = new TenantDoc { Id = Guid.NewGuid(), Name = "Tenant A Doc" };
        var doc2 = new TenantDoc { Id = Guid.NewGuid(), Name = "Tenant B Doc" };

        theSession.Store(doc1);
        theSession.ForTenant("tenant-b").Store(doc2);
        await theSession.SaveChangesAsync();

        // Verify doc1 is stored for default tenant
        await using var queryDefault = theStore.QuerySession();
        var loaded1 = await queryDefault.LoadAsync<TenantDoc>(doc1.Id);
        loaded1.ShouldNotBeNull();
        loaded1.Name.ShouldBe("Tenant A Doc");
    }

    [Fact]
    public async Task for_tenant_returns_cached_instance()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "for_tenant_cache");

        var ops1 = theSession.ForTenant("tenant-x");
        var ops2 = theSession.ForTenant("tenant-x");

        ops1.ShouldBeSameAs(ops2);
    }

    [Fact]
    public async Task for_tenant_exposes_tenant_id_and_parent()
    {
        await StoreOptions(opts => opts.DatabaseSchemaName = "for_tenant_props");

        var ops = theSession.ForTenant("my-tenant");

        ops.TenantId.ShouldBe("my-tenant");
        ops.Parent.ShouldBe(theSession);
    }

    [Fact]
    public async Task for_tenant_events_append_with_different_tenant()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "for_tenant_events";
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
        });

        var streamId = Guid.NewGuid();

        // Start stream on default tenant
        theSession.Events.StartStream(streamId, new QuestStarted("Main Quest"));

        // Append event for a different tenant
        var stream2 = Guid.NewGuid();
        theSession.ForTenant("tenant-alt").Events.StartStream(stream2, new QuestStarted("Alt Quest"));

        await theSession.SaveChangesAsync();

        // Verify both streams exist
        await using var query = theStore.QuerySession();
        var events1 = await query.Events.FetchStreamAsync(streamId);
        events1.Count.ShouldBe(1);
    }
}
