using Polecat.Linq;
using Polecat.Linq.SoftDeletes;
using Polecat.Tests.Harness;

namespace Polecat.Tests.SoftDeletes;

[Collection("integration")]
public class soft_delete_operations : IntegrationContext
{
    public soft_delete_operations(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "soft_delete_ops";
        });
    }

    [Fact]
    public async Task soft_delete_by_document_marks_as_deleted()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "to-delete" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync();

        // Should not be found by normal Load
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task soft_delete_by_id_marks_as_deleted()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "to-delete-by-id" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<SoftDeletedDoc>(doc.Id);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task hard_delete_removes_physically()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hard-delete" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.HardDelete(doc);
        await session2.SaveChangesAsync();

        // Not found by any query — verify row physically removed
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", doc.Id);
        var count = (int)await cmd.ExecuteScalarAsync()!;
        count.ShouldBe(0);
    }

    [Fact]
    public async Task hard_delete_by_id_removes_physically()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hard-delete-by-id" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.HardDelete<SoftDeletedDoc>(doc.Id);
        await session2.SaveChangesAsync();

        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", doc.Id);
        var count = (int)await cmd.ExecuteScalarAsync()!;
        count.ShouldBe(0);
    }

    [Fact]
    public async Task isoft_deleted_interface_sets_properties_on_delete()
    {
        var doc = new SoftDeletedWithInterface { Id = Guid.NewGuid(), Name = "interface-doc" };

        doc.Deleted.ShouldBeFalse();
        doc.DeletedAt.ShouldBeNull();

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync();

        // In-memory properties should be set
        doc.Deleted.ShouldBeTrue();
        doc.DeletedAt.ShouldNotBeNull();
    }

    [Fact]
    public async Task undo_delete_where_restores_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "restore-me", Number = 42 };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "stay-deleted", Number = 99 };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        // Delete both
        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc1);
        session2.Delete(doc2);
        await session2.SaveChangesAsync();

        // Undo only doc1
        await using var session3 = theStore.LightweightSession();
        session3.UndoDeleteWhere<SoftDeletedDoc>(x => x.Name == "restore-me");
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var restored = await query.LoadAsync<SoftDeletedDoc>(doc1.Id);
        restored.ShouldNotBeNull();
        restored.Name.ShouldBe("restore-me");

        var stillDeleted = await query.LoadAsync<SoftDeletedDoc>(doc2.Id);
        stillDeleted.ShouldBeNull();
    }

    [Fact]
    public async Task delete_where_soft_deletes_matching_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-keep", Number = 10 };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-remove", Number = 20 };
        var doc3 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-also-remove", Number = 20 };

        theSession.Store(doc1, doc2, doc3);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.DeleteWhere<SoftDeletedDoc>(x => x.Number == 20);
        await session2.SaveChangesAsync();

        // doc1 should still be visible
        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<SoftDeletedDoc>(doc1.Id);
        kept.ShouldNotBeNull();

        // doc2 and doc3 should be soft-deleted (hidden from normal queries)
        var gone2 = await query.LoadAsync<SoftDeletedDoc>(doc2.Id);
        gone2.ShouldBeNull();
        var gone3 = await query.LoadAsync<SoftDeletedDoc>(doc3.Id);
        gone3.ShouldBeNull();

        // But still present via MaybeDeleted
        var all = await query.Query<SoftDeletedDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc2.Id || x.Id == doc3.Id)
            .ToListAsync();
        all.Count.ShouldBe(2);
    }

    [Fact]
    public async Task hard_delete_where_physically_removes_matching_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hdw-keep", Number = 30 };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hdw-remove", Number = 40 };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.HardDeleteWhere<SoftDeletedDoc>(x => x.Number == 40);
        await session2.SaveChangesAsync();

        // doc1 still present
        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<SoftDeletedDoc>(doc1.Id);
        kept.ShouldNotBeNull();

        // doc2 physically gone — not even MaybeDeleted can find it
        var all = await query.Query<SoftDeletedDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc2.Id)
            .ToListAsync();
        all.Count.ShouldBe(0);
    }

    [Fact]
    public async Task delete_where_on_non_soft_deleted_type_does_hard_delete()
    {
        // User is a normal (non-soft-deleted) type, so DeleteWhere should physically remove
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Keep", LastName = "Me", Age = 25 };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Delete", LastName = "Me", Age = 99 };

        theSession.Store(user1, user2);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.DeleteWhere<User>(x => x.Age == 99);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<User>(user1.Id);
        kept.ShouldNotBeNull();

        var gone = await query.LoadAsync<User>(user2.Id);
        gone.ShouldBeNull();
    }
}
