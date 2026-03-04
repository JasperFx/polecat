using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

/// <summary>
///     Tests for complex operation ordering, conflicts, and edge cases.
///     Verifies FIFO ordering, mixed operation interactions, and session reuse.
/// </summary>
[Collection("integration")]
public class complex_operation_ordering_tests : IntegrationContext
{
    public complex_operation_ordering_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ===== Insert then update in same session =====

    [Fact]
    public async Task insert_then_store_same_document_in_same_session()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Original", LastName = "Insert", Age = 25 };
        theSession.Insert(user);

        // Now update via Store
        user.FirstName = "Updated";
        theSession.Store(user);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Updated");
    }

    // ===== Delete then re-insert =====

    [Fact]
    public async Task delete_then_store_same_id_across_sessions()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Original", LastName = "A", Age = 30 });
        await theSession.SaveChangesAsync();

        // Delete in a second session
        await using var session2 = theStore.LightweightSession();
        session2.Delete<User>(id);
        await session2.SaveChangesAsync();

        // Re-insert with same ID in a third session
        await using var session3 = theStore.LightweightSession();
        session3.Store(new User { Id = id, FirstName = "Reinserted", LastName = "B", Age = 31 });
        await session3.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Reinserted");
    }

    // ===== Multiple document types in one save =====

    [Fact]
    public async Task mixed_document_types_all_persist_in_one_save()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Mix", LastName = "Test", Age = 1 };
        var target = Target.Random();
        var stringDoc = new StringDoc { Id = $"mix-{Guid.NewGuid():N}", Name = "Mixed" };

        theSession.Store(user);
        theSession.Store(target);
        theSession.Store(stringDoc);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user.Id)).ShouldNotBeNull();
        (await query.LoadAsync<Target>(target.Id)).ShouldNotBeNull();
        (await query.LoadAsync<StringDoc>(stringDoc.Id)).ShouldNotBeNull();
    }

    // ===== DeleteWhere then Store a matching doc =====

    [Fact]
    public async Task delete_where_then_store_matching_document()
    {
        // Store initial documents
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 100 });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 200 });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 300 });
        await theSession.SaveChangesAsync();

        // In a new session: delete all Red, then store a new Red
        await using var session2 = theStore.LightweightSession();
        session2.HardDeleteWhere<Target>(t => t.Color == "Red");
        var newRed = new Target { Id = Guid.NewGuid(), Color = "Red", Number = 999 };
        session2.Store(newRed);
        await session2.SaveChangesAsync();

        // The new red should exist (store after delete executes in FIFO)
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Target>(newRed.Id);
        loaded.ShouldNotBeNull();
        loaded.Number.ShouldBe(999);
    }

    // ===== Session reuse after SaveChanges =====

    [Fact]
    public async Task session_reusable_after_save_changes()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Save1", LastName = "A", Age = 1 };
        theSession.Store(user1);
        await theSession.SaveChangesAsync();

        // Pending changes should be empty after save
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();

        // Session should still be usable
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Save2", LastName = "B", Age = 2 };
        theSession.Store(user2);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user1.Id)).ShouldNotBeNull();
        (await query.LoadAsync<User>(user2.Id)).ShouldNotBeNull();
    }

    // ===== Multiple saves accumulate documents =====

    [Fact]
    public async Task multiple_saves_in_same_session_accumulate_correctly()
    {
        var ids = new List<Guid>();

        for (var i = 0; i < 5; i++)
        {
            var user = new User { Id = Guid.NewGuid(), FirstName = $"Multi{i}", LastName = "Save", Age = i };
            theSession.Store(user);
            ids.Add(user.Id);
            await theSession.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        foreach (var id in ids)
        {
            (await query.LoadAsync<User>(id)).ShouldNotBeNull();
        }
    }

    // ===== Insert duplicate throws =====

    [Fact]
    public async Task insert_duplicate_id_throws()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "First", LastName = "A", Age = 1 });
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Insert(new User { Id = id, FirstName = "Duplicate", LastName = "B", Age = 2 });

        // Insert with existing ID should throw (PK violation)
        await Should.ThrowAsync<Exception>(session2.SaveChangesAsync());
    }

    // ===== Documents and events in same SaveChanges =====

    [Fact]
    public async Task documents_and_events_in_same_save()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "EventDoc", LastName = "Mix", Age = 42 };
        var streamId = Guid.NewGuid();

        theSession.Store(user);
        theSession.Events.StartStream(streamId, new QuestStarted("Joint Save"));

        theSession.PendingChanges.Operations.Count.ShouldBe(1);
        theSession.PendingChanges.Streams.Count.ShouldBe(1);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user.Id)).ShouldNotBeNull();
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }

    // ===== Update non-existent document (upsert behavior) =====

    [Fact]
    public async Task store_acts_as_upsert_for_new_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Upsert", LastName = "New", Age = 77 };

        // Store should work as upsert — inserting if not exists
        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Upsert");
    }
}
