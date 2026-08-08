using Polecat.Attributes;
using Polecat.Linq;
using Polecat.Linq.SoftDeletes;
using Polecat.Tests.Harness;

namespace Polecat.Tests.SoftDeletes;

/// <summary>
///     A soft-deleted type carrying the deletion timestamp on a member of its own rather than
///     through <see cref="ISoftDeleted" />, so the attribute path is covered independently of the
///     interface path. #421.
/// </summary>
[SoftDeleted]
public class AttributeMappedDeletedAtDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    [IsSoftDeletedMetadata] public bool IsGone { get; set; }

    [SoftDeletedAtMetadata] public DateTimeOffset? GoneAt { get; set; }
}

/// <summary>
///     #421: <c>JasperFx.Metadata.ISoftDeleted</c> declares two members — <c>Deleted</c> and
///     <c>DeletedAt</c> — and Polecat populated neither on read. <c>deleted_at</c> had no
///     member-mapping config at all (only <c>is_deleted</c> did) and its binder was built
///     write-only, so a document implementing the interface loaded back with <c>DeletedAt</c> at
///     <c>default</c> — null where the column holds a real timestamp. The value is in the database
///     and reachable through <c>DeletedSince</c> / <c>DeletedBefore</c>; it just never reached the
///     member the interface says holds it.
/// </summary>
[Collection("integration")]
public class soft_delete_metadata_on_read : IntegrationContext
{
    public soft_delete_metadata_on_read(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "soft_delete_read_meta";
            opts.Schema.For<AttributeMappedDeletedAtDoc>();
        });
    }

    [Fact]
    public async Task deleted_at_reaches_the_isoftdeleted_member_on_read()
    {
        var doc = new SoftDeletedWithInterface { Id = Guid.NewGuid(), Name = "read-deleted-at" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var before = DateTimeOffset.UtcNow.AddMinutes(-1);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(doc);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var after = DateTimeOffset.UtcNow.AddMinutes(1);

        // A soft-deleted document is hidden from ordinary queries, so MaybeDeleted is the read
        // path where these members are observable at all.
        await using var query = theStore.QuerySession();
        var loaded = (await query.Query<SoftDeletedWithInterface>()
            .MaybeDeleted()
            .Where(x => x.Id == doc.Id)
            .ToListAsync(TestContext.Current.CancellationToken)).ShouldHaveSingleItem();

        loaded.Deleted.ShouldBeTrue();
        loaded.DeletedAt.ShouldNotBeNull();
        loaded.DeletedAt!.Value.ShouldBeInRange(before, after);
    }

    /// <summary>
    ///     Half a populated interface is arguably worse than none, because <c>Deleted</c> being
    ///     right is what suggests <c>DeletedAt</c> is too. A live document has to report both
    ///     members honestly as well.
    /// </summary>
    [Fact]
    public async Task a_live_document_reports_not_deleted_with_no_timestamp()
    {
        var doc = new SoftDeletedWithInterface { Id = Guid.NewGuid(), Name = "still-here" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedWithInterface>(doc.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Deleted.ShouldBeFalse();
        loaded.DeletedAt.ShouldBeNull();
    }

    /// <summary>
    ///     Undelete sets <c>deleted_at = NULL</c>, so the member must come back to null rather than
    ///     keeping the stale instant the document was loaded with earlier.
    /// </summary>
    [Fact]
    public async Task undeleting_clears_the_timestamp_member_on_read()
    {
        var doc = new SoftDeletedWithInterface { Id = Guid.NewGuid(), Name = "undelete-read" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(doc);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session3 = theStore.LightweightSession())
        {
            session3.UndoDeleteWhere<SoftDeletedWithInterface>(x => x.Name == "undelete-read");
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedWithInterface>(doc.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Deleted.ShouldBeFalse();
        loaded.DeletedAt.ShouldBeNull();
    }

    /// <summary>
    ///     The same mapping via <c>[SoftDeletedAtMetadata]</c> on an arbitrarily-named member, so
    ///     the feature is not tied to implementing <see cref="ISoftDeleted" />. Mirrors Marten's
    ///     <c>SoftDeletedAtMetadataAttribute</c>.
    /// </summary>
    [Fact]
    public async Task deleted_at_reaches_a_member_mapped_by_attribute()
    {
        var doc = new AttributeMappedDeletedAtDoc { Id = Guid.NewGuid(), Name = "attr-mapped" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(doc);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var loaded = (await query.Query<AttributeMappedDeletedAtDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc.Id)
            .ToListAsync(TestContext.Current.CancellationToken)).ShouldHaveSingleItem();

        loaded.IsGone.ShouldBeTrue();
        loaded.GoneAt.ShouldNotBeNull();
    }
}
