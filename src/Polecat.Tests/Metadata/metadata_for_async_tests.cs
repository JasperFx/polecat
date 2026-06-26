using Polecat.Attributes;
using Polecat.Metadata;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Metadata;

/// <summary>
/// #242: MetadataForAsync&lt;T&gt; returns a DocumentMetadata snapshot (version, timestamps, tenant,
/// dotnet type, soft-delete state, and the opt-in correlation/causation/last-modified-by/headers)
/// without loading the document body.
/// </summary>
public class metadata_for_async_tests : OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [SoftDeleted]
    public class SoftDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task returns_core_metadata_for_a_stored_document()
    {
        ConfigureStore(_ => { });

        var doc = new Doc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var metadata = await query.MetadataForAsync(doc);

        metadata.ShouldNotBeNull();
        metadata.Id.ShouldBe(doc.Id);
        metadata.Version.ShouldBe(1);
        metadata.TenantId.ShouldBe("*DEFAULT*");
        metadata.DotNetType.ShouldNotBeNull();
        metadata.CreatedAt.ShouldBeGreaterThan(default);
        metadata.LastModified.ShouldBeGreaterThan(default);
    }

    [Fact]
    public async Task version_increments_are_reflected()
    {
        ConfigureStore(_ => { });

        var doc = new Doc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            await session.SaveChangesAsync();
            doc.Name = "B";
            session.Store(doc);
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var metadata = await query.MetadataForAsync<Doc>(doc.Id);
        metadata!.Version.ShouldBe(2);
    }

    [Fact]
    public async Task returns_optin_metadata_columns()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>().Metadata(m =>
        {
            m.CorrelationId.Enabled = true;
            m.CausationId.Enabled = true;
            m.LastModifiedBy.Enabled = true;
            m.Headers.Enabled = true;
        }));

        var doc = new Doc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.CorrelationId = "corr-9";
            session.CausationId = "cause-9";
            session.LastModifiedBy = "user-9";
            session.SetHeader("region", "us");
            session.Store(doc);
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var metadata = await query.MetadataForAsync(doc);

        metadata!.CorrelationId.ShouldBe("corr-9");
        metadata.CausationId.ShouldBe("cause-9");
        metadata.LastModifiedBy.ShouldBe("user-9");
        metadata.Headers.ShouldNotBeNull();
        metadata.Headers!["region"].ToString().ShouldBe("us");
    }

    [Fact]
    public async Task returns_null_for_missing_document()
    {
        ConfigureStore(_ => { });
        // Ensure the table exists.
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Doc { Id = Guid.NewGuid(), Name = "seed" });
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        (await query.MetadataForAsync<Doc>(Guid.NewGuid())).ShouldBeNull();
    }

    [Fact]
    public async Task surfaces_soft_delete_state()
    {
        ConfigureStore(_ => { });

        var doc = new SoftDoc { Id = Guid.NewGuid(), Name = "A" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            await session.SaveChangesAsync();
            session.Delete(doc); // soft delete
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var metadata = await query.MetadataForAsync<SoftDoc>(doc.Id);

        metadata.ShouldNotBeNull();
        metadata.Deleted.ShouldBeTrue();
        metadata.DeletedAt.ShouldNotBeNull();
    }
}
