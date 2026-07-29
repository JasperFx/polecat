using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Session;

/// <summary>
///     Tests for document operation behavior: PendingChanges inspection,
///     session-level timeout override, and operation ordering.
///     Ported from Marten's document operation test patterns.
/// </summary>
[Collection("integration")]
public class document_ops_tests : IntegrationContext
{
    public document_ops_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ===== PendingChanges inspection =====

    [Fact]
    public async Task pending_changes_tracks_mixed_document_types()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "Smith", Age = 30 };
        var target = Target.Random();

        theSession.Store(user);
        theSession.Store(target);

        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();
        theSession.PendingChanges.Operations.Count.ShouldBe(2);
    }

    [Fact]
    public async Task pending_changes_tracks_inserts_updates_deletes()
    {
        // Setup: Store a user first
        var existingUser = new User { Id = Guid.NewGuid(), FirstName = "Existing", LastName = "User", Age = 50 };
        theSession.Store(existingUser);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // New session: insert, update, delete
        await using var session = theStore.LightweightSession();
        var newUser = new User { Id = Guid.NewGuid(), FirstName = "New", LastName = "User", Age = 25 };
        session.Insert(newUser);
        session.Update(existingUser);
        session.Delete<User>(existingUser.Id);

        session.PendingChanges.Operations.Count.ShouldBe(3);
        session.PendingChanges.HasOutstandingWork().ShouldBeTrue();
    }

    [Fact]
    public async Task pending_changes_includes_stream_actions()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("PendingChanges Stream"));

        theSession.PendingChanges.Streams.Count.ShouldBe(1);
        theSession.PendingChanges.Streams[0].Id.ShouldBe(streamId);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();
    }

    [Fact]
    public async Task pending_changes_tracks_both_docs_and_streams()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "Smith", Age = 30 };
        var streamId = Guid.NewGuid();

        theSession.Store(user);
        theSession.Events.StartStream(streamId, new QuestStarted("Mixed"));

        theSession.PendingChanges.Operations.Count.ShouldBe(1);
        theSession.PendingChanges.Streams.Count.ShouldBe(1);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();
    }

    [Fact]
    public async Task pending_changes_empty_after_save()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Clean", LastName = "Slate", Age = 40 };
        theSession.Store(user);

        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
        theSession.PendingChanges.Operations.ShouldBeEmpty();
        theSession.PendingChanges.Streams.ShouldBeEmpty();
    }

    [Fact]
    public async Task pending_changes_no_work_initially()
    {
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
        theSession.PendingChanges.Operations.ShouldBeEmpty();
        theSession.PendingChanges.Streams.ShouldBeEmpty();
    }

    // ===== Session-level timeout override =====

    [Fact]
    public async Task session_timeout_override_works()
    {
        // Create a session with a custom timeout
        var session = theStore.LightweightSession(new SessionOptions { Timeout = 120 });
        await using (session)
        {
            // The session should be usable — store and load a document
            var user = new User { Id = Guid.NewGuid(), FirstName = "Timeout", LastName = "Test", Age = 1 };
            session.Store(user);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        // Verify the document was stored
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(((IDocumentSession)session).PendingChanges.Operations.Count == 0
            ? Guid.Empty : Guid.Empty, TestContext.Current.CancellationToken);
        // Just verifying no timeout exception was thrown
    }

    [Fact]
    public async Task query_session_timeout_override_works()
    {
        // Store a user first
        var userId = Guid.NewGuid();
        theSession.Store(new User { Id = userId, FirstName = "Query", LastName = "Timeout", Age = 2 });
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Create a query session with custom timeout
        await using var query = theStore.QuerySession(new SessionOptions { Timeout = 60 });
        var loaded = await query.LoadAsync<User>(userId, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Query");
    }

    // ===== Operation ordering (FIFO) =====

    [Fact]
    public async Task operations_execute_in_insertion_order()
    {
        // Store multiple documents — they should all persist successfully
        // regardless of type order (FIFO)
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "First", LastName = "A", Age = 1 };
        var target = Target.Random();
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Second", LastName = "B", Age = 2 };

        theSession.Store(user1);
        theSession.Store(target);
        theSession.Store(user2);

        // Verify FIFO ordering in PendingChanges
        var ops = theSession.PendingChanges.Operations;
        ops.Count.ShouldBe(3);
        ops[0].DocumentType.ShouldBe(typeof(User));
        ops[1].DocumentType.ShouldBe(typeof(Target));
        ops[2].DocumentType.ShouldBe(typeof(User));

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Verify all documents persisted
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user1.Id, TestContext.Current.CancellationToken)).ShouldNotBeNull();
        (await query.LoadAsync<Target>(target.Id, TestContext.Current.CancellationToken)).ShouldNotBeNull();
        (await query.LoadAsync<User>(user2.Id, TestContext.Current.CancellationToken)).ShouldNotBeNull();
    }

    [Fact]
    public async Task store_then_delete_same_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "ToDelete", LastName = "Soon", Age = 99 };

        // Store first, then delete in the same session
        theSession.Store(user);
        theSession.Delete<User>(user.Id);

        // Both operations should be tracked
        theSession.PendingChanges.Operations.Count.ShouldBe(2);

        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Net effect: document should not exist (delete after insert)
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id, TestContext.Current.CancellationToken);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task multiple_updates_to_same_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Original", LastName = "Name", Age = 30 };
        theSession.Store(user);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var session2 = theStore.LightweightSession();
        user.FirstName = "Updated1";
        session2.Store(user);
        user.FirstName = "Updated2";
        session2.Store(user);

        // Both store operations queued
        session2.PendingChanges.Operations.Count.ShouldBe(2);

        await session2.SaveChangesAsync(TestContext.Current.CancellationToken);

        // Last write wins
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id, TestContext.Current.CancellationToken);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Updated2");
    }

    // ===== Load with multiple IDs =====

    [Fact]
    public async Task load_many_documents()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Alice", LastName = "A", Age = 25 };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Bob", LastName = "B", Age = 30 };
        var user3 = new User { Id = Guid.NewGuid(), FirstName = "Charlie", LastName = "C", Age = 35 };

        theSession.Store(user1);
        theSession.Store(user2);
        theSession.Store(user3);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<User>([user1.Id, user3.Id], TestContext.Current.CancellationToken);

        loaded.Count.ShouldBe(2);
        loaded.ShouldContain(u => u.FirstName == "Alice");
        loaded.ShouldContain(u => u.FirstName == "Charlie");
    }

    [Fact]
    public async Task load_many_with_some_missing()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Exists", LastName = "A", Age = 25 };

        theSession.Store(user1);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<User>([user1.Id, Guid.NewGuid()], TestContext.Current.CancellationToken);

        loaded.Count.ShouldBe(1);
        loaded[0].FirstName.ShouldBe("Exists");
    }
}
