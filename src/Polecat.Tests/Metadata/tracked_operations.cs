using Polecat.Metadata;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Metadata;

[Collection("integration")]
public class tracked_operations : IntegrationContext
{
    public tracked_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task tracked_properties_are_synced_on_store()
    {
        theSession.CorrelationId = "corr-123";
        theSession.CausationId = "cause-456";
        theSession.LastModifiedBy = "user-789";

        var doc = new TrackedDoc { Id = Guid.NewGuid(), Name = "Tracked" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // In-memory properties should be set
        doc.CorrelationId.ShouldBe("corr-123");
        doc.CausationId.ShouldBe("cause-456");
        doc.LastModifiedBy.ShouldBe("user-789");

        // Reload from DB and verify
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<TrackedDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.CorrelationId.ShouldBe("corr-123");
        loaded.CausationId.ShouldBe("cause-456");
        loaded.LastModifiedBy.ShouldBe("user-789");
    }

    [Fact]
    public async Task tracked_properties_are_synced_on_insert()
    {
        theSession.CorrelationId = "insert-corr";
        theSession.LastModifiedBy = "insert-user";

        var doc = new TrackedDoc { Id = Guid.NewGuid(), Name = "Inserted" };
        theSession.Insert(doc);
        await theSession.SaveChangesAsync();

        doc.CorrelationId.ShouldBe("insert-corr");
        doc.LastModifiedBy.ShouldBe("insert-user");

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<TrackedDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.CorrelationId.ShouldBe("insert-corr");
    }

    [Fact]
    public async Task tracked_properties_are_updated_on_subsequent_store()
    {
        theSession.CorrelationId = "v1";
        theSession.LastModifiedBy = "user-a";

        var doc = new TrackedDoc { Id = Guid.NewGuid(), Name = "Original" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        // Update with different session metadata
        await using var session2 = theStore.LightweightSession();
        session2.CorrelationId = "v2";
        session2.LastModifiedBy = "user-b";

        doc.Name = "Updated";
        session2.Store(doc);
        await session2.SaveChangesAsync();

        doc.CorrelationId.ShouldBe("v2");
        doc.LastModifiedBy.ShouldBe("user-b");

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<TrackedDoc>(doc.Id);
        loaded!.CorrelationId.ShouldBe("v2");
        loaded.LastModifiedBy.ShouldBe("user-b");
    }

    [Fact]
    public async Task null_session_metadata_sets_null_on_document()
    {
        // Don't set any session metadata
        var doc = new TrackedDoc { Id = Guid.NewGuid(), Name = "NoMetadata" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        doc.CorrelationId.ShouldBeNull();
        doc.CausationId.ShouldBeNull();
        doc.LastModifiedBy.ShouldBeNull();
    }

    [Fact]
    public async Task tenanted_document_gets_tenant_id_on_store()
    {
        var doc = new TenantedDoc { Id = Guid.NewGuid(), Name = "TenantAware" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        doc.TenantId.ShouldBe(theSession.TenantId);
    }

    [Fact]
    public async Task tenanted_document_gets_tenant_id_on_load()
    {
        var doc = new TenantedDoc { Id = Guid.NewGuid(), Name = "TenantLoad" };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<TenantedDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.TenantId.ShouldBe(query.TenantId);
    }

    [Fact]
    public async Task combined_tracked_and_tenanted()
    {
        theSession.CorrelationId = "combo-corr";

        var doc = new FullMetadataDoc
        {
            Id = Guid.NewGuid(),
            Name = "Both"
        };
        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        doc.CorrelationId.ShouldBe("combo-corr");
        doc.TenantId.ShouldBe(theSession.TenantId);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<FullMetadataDoc>(doc.Id);
        loaded.ShouldNotBeNull();
        loaded.CorrelationId.ShouldBe("combo-corr");
        loaded.TenantId.ShouldBe(query.TenantId);
    }
}

public class TrackedDoc : ITracked
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }
}

public class TenantedDoc : ITenanted
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
}

public class FullMetadataDoc : ITracked, ITenanted
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? CorrelationId { get; set; }
    public string? CausationId { get; set; }
    public string? LastModifiedBy { get; set; }
    public string TenantId { get; set; } = string.Empty;
}
