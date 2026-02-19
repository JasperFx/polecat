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
}
