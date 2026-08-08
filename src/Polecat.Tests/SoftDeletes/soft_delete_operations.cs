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

    public override async ValueTask InitializeAsync()
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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Should not be found by normal Load
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task soft_delete_by_id_marks_as_deleted()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "to-delete-by-id" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.Delete<SoftDeletedDoc>(doc.Id);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<SoftDeletedDoc>(doc.Id, TestContext.Current.CancellationToken);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task hard_delete_removes_physically()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hard-delete" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.HardDelete(doc);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Not found by any query — verify row physically removed
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", doc.Id);
        var count = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        count.ShouldBe(0);
    }

    [Fact]
    public async Task hard_delete_by_id_removes_physically()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hard-delete-by-id" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.HardDelete<SoftDeletedDoc>(doc.Id);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", doc.Id);
        var count = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        count.ShouldBe(0);
    }

    [Fact]
    public async Task isoft_deleted_interface_sets_properties_on_delete()
    {
        var doc = new SoftDeletedWithInterface { Id = Guid.NewGuid(), Name = "interface-doc" };

        doc.Deleted.ShouldBeFalse();
        doc.DeletedAt.ShouldBeNull();

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

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
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Delete both
        await using var session2 = theStore.LightweightSession();
        session2.Delete(doc1);
        session2.Delete(doc2);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Undo only doc1
        await using var session3 = theStore.LightweightSession();
        session3.UndoDeleteWhere<SoftDeletedDoc>(x => x.Name == "restore-me");
        await session3.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var restored = await query.LoadAsync<SoftDeletedDoc>(doc1.Id, TestContext.Current.CancellationToken);
        restored.ShouldNotBeNull();
        restored.Name.ShouldBe("restore-me");

        var stillDeleted = await query.LoadAsync<SoftDeletedDoc>(doc2.Id, TestContext.Current.CancellationToken);
        stillDeleted.ShouldBeNull();
    }

    [Fact]
    public async Task delete_where_soft_deletes_matching_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-keep", Number = 10 };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-remove", Number = 20 };
        var doc3 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "delete-where-also-remove", Number = 20 };

        theSession.Store(doc1, doc2, doc3);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.DeleteWhere<SoftDeletedDoc>(x => x.Number == 20);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // doc1 should still be visible
        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<SoftDeletedDoc>(doc1.Id, TestContext.Current.CancellationToken);
        kept.ShouldNotBeNull();

        // doc2 and doc3 should be soft-deleted (hidden from normal queries)
        var gone2 = await query.LoadAsync<SoftDeletedDoc>(doc2.Id, TestContext.Current.CancellationToken);
        gone2.ShouldBeNull();
        var gone3 = await query.LoadAsync<SoftDeletedDoc>(doc3.Id, TestContext.Current.CancellationToken);
        gone3.ShouldBeNull();

        // But still present via MaybeDeleted
        var all = await query.Query<SoftDeletedDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc2.Id || x.Id == doc3.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        all.Count.ShouldBe(2);
    }

    [Fact]
    public async Task hard_delete_where_physically_removes_matching_documents()
    {
        var doc1 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hdw-keep", Number = 30 };
        var doc2 = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "hdw-remove", Number = 40 };

        theSession.Store(doc1, doc2);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.HardDeleteWhere<SoftDeletedDoc>(x => x.Number == 40);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // doc1 still present
        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<SoftDeletedDoc>(doc1.Id, TestContext.Current.CancellationToken);
        kept.ShouldNotBeNull();

        // doc2 physically gone — not even MaybeDeleted can find it
        var all = await query.Query<SoftDeletedDoc>()
            .MaybeDeleted()
            .Where(x => x.Id == doc2.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        all.Count.ShouldBe(0);
    }

    /// <summary>
    ///     #419. <c>DeleteWhere</c> was an <c>UPDATE … SET is_deleted = 1, deleted_at =
    ///     SYSDATETIMEOFFSET()</c> with no guard on the row's current state, so it re-stamped
    ///     <c>deleted_at</c> on rows that were already deleted. Nothing errors and nothing looks
    ///     wrong — the rows were already deleted and stay deleted. What changes is the answer to
    ///     every question about *when*: <c>DeletedSince</c> / <c>DeletedBefore</c>, an audit, or a
    ///     retention sweep all silently read the later bulk-delete call instead of the deletion,
    ///     and the real value is unrecoverable.
    ///     <para>
    ///     Two deletes in the same millisecond agree either way, so calling <c>DeleteWhere</c>
    ///     twice proves nothing on its own. This plants a known <c>deleted_at</c> far in the past
    ///     on the already-deleted row and asserts the second call leaves it alone.
    ///     </para>
    /// </summary>
    [Fact]
    public async Task delete_where_does_not_restamp_deleted_at_on_already_deleted_rows()
    {
        var planted = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);

        var alreadyDeleted = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "restamp-old", Number = 50 };
        var stillLive = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "restamp-new", Number = 50 };

        theSession.Store(alreadyDeleted, stillLive);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(alreadyDeleted);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Plant a deletion instant well in the past, so a re-stamp is unmistakable.
        await PlantDeletedAtAsync(alreadyDeleted.Id, planted);

        // A later bulk delete whose predicate still matches the already-deleted row.
        await using (var session3 = theStore.LightweightSession())
        {
            session3.DeleteWhere<SoftDeletedDoc>(x => x.Number == 50);
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // The already-deleted row keeps the instant it was actually deleted.
        var untouched = await ReadDeletedAtAsync(alreadyDeleted.Id);
        untouched.ShouldNotBeNull();
        untouched.Value.ToUniversalTime().ShouldBe(planted.ToUniversalTime());

        // ... and the row that was still live is deleted now, with a real (recent) timestamp.
        var freshlyDeleted = await ReadDeletedAtAsync(stillLive.Id);
        freshlyDeleted.ShouldNotBeNull();
        freshlyDeleted.Value.ShouldBeGreaterThan(planted);

        await using var query = theStore.QuerySession();
        var visible = await query.LoadAsync<SoftDeletedDoc>(stillLive.Id, TestContext.Current.CancellationToken);
        visible.ShouldBeNull();
    }

    /// <summary>
    ///     #419, the other half: <c>HardDeleteWhere</c> must NOT get the guard. A hard delete of an
    ///     already-soft-deleted row is a real delete and has to proceed — guarding it would strand
    ///     soft-deleted rows permanently beyond the reach of the purge that is supposed to remove
    ///     them.
    /// </summary>
    [Fact]
    public async Task hard_delete_where_still_removes_already_soft_deleted_rows()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "purge-me", Number = 60 };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(doc);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session3 = theStore.LightweightSession())
        {
            session3.HardDeleteWhere<SoftDeletedDoc>(x => x.Number == 60);
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", doc.Id);
        var count = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        count.ShouldBe(0);
    }

    /// <summary>
    ///     #419, the symmetry nit: <c>UnDeleteByIdOperation</c> had no <c>is_deleted = 1</c> guard
    ///     where <c>UndoDeleteWhereOperation</c> does. It is harmless — undeleting a live row writes
    ///     exactly the values the row already holds — but the guard has to not break the case it
    ///     applies to, which is undeleting a row that really is deleted.
    /// </summary>
    [Fact]
    public async Task undelete_by_id_still_restores_a_deleted_document()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "undelete-by-id", Number = 70 };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using (var session2 = theStore.LightweightSession())
        {
            session2.Delete(doc);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var session3 = theStore.LightweightSession())
        {
            session3.UndoDeleteWhere<SoftDeletedDoc>(x => x.Number == 70);
            await session3.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        var restored = await query.LoadAsync<SoftDeletedDoc>(doc.Id, TestContext.Current.CancellationToken);
        restored.ShouldNotBeNull();
        (await ReadDeletedAtAsync(doc.Id)).ShouldBeNull();
    }

    /// <summary>
    ///     #419: a live row left alone by a soft <c>DeleteWhere</c> whose predicate excludes it must
    ///     still be deletable later — the guard scopes the UPDATE, it does not disable it.
    /// </summary>
    [Fact]
    public async Task delete_where_still_stamps_deleted_at_on_a_live_row()
    {
        var doc = new SoftDeletedDoc { Id = Guid.NewGuid(), Name = "first-delete", Number = 80 };

        theSession.Store(doc);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        (await ReadDeletedAtAsync(doc.Id)).ShouldBeNull();

        await using (var session2 = theStore.LightweightSession())
        {
            session2.DeleteWhere<SoftDeletedDoc>(x => x.Number == 80);
            await session2.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await ReadDeletedAtAsync(doc.Id)).ShouldNotBeNull();
    }

    private async Task PlantDeletedAtAsync(Guid id, DateTimeOffset value)
    {
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "UPDATE [soft_delete_ops].[pc_doc_softdeleteddoc] SET deleted_at = @at WHERE id = @id";
        cmd.Parameters.AddWithValue("@at", value);
        cmd.Parameters.AddWithValue("@id", id);
        var rows = await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        rows.ShouldBe(1);
    }

    private async Task<DateTimeOffset?> ReadDeletedAtAsync(Guid id)
    {
        var conn = await OpenConnectionAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT deleted_at FROM [soft_delete_ops].[pc_doc_softdeleteddoc] WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        var raw = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return raw is null or DBNull ? null : (DateTimeOffset)raw;
    }

    [Fact]
    public async Task delete_where_on_non_soft_deleted_type_does_hard_delete()
    {
        // User is a normal (non-soft-deleted) type, so DeleteWhere should physically remove
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Keep", LastName = "Me", Age = 25 };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Delete", LastName = "Me", Age = 99 };

        theSession.Store(user1, user2);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        session2.DeleteWhere<User>(x => x.Age == 99);
        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var kept = await query.LoadAsync<User>(user1.Id, TestContext.Current.CancellationToken);
        kept.ShouldNotBeNull();

        var gone = await query.LoadAsync<User>(user2.Id, TestContext.Current.CancellationToken);
        gone.ShouldBeNull();
    }
}
