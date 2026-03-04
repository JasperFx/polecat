using Polecat.Exceptions;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

[Collection("integration")]
public class operation_dependency_tests : IntegrationContext
{
    public operation_dependency_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task events_and_documents_atomic_failure()
    {
        // Create a stream first so that StartStream with the same ID will fail
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream(streamId, new QuestStarted("Original"));
        await theSession.SaveChangesAsync();

        // Now attempt to store a document AND start a duplicate stream in the same SaveChanges
        await using var session2 = theStore.LightweightSession();
        var user = new User { Id = Guid.NewGuid(), FirstName = "Should", LastName = "Rollback" };
        session2.Store(user);
        session2.Events.StartStream(streamId, new QuestStarted("Duplicate"));

        // The duplicate stream should cause a failure
        await Should.ThrowAsync<ExistingStreamIdCollisionException>(
            session2.SaveChangesAsync());

        // The document should NOT have been committed (atomic rollback)
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task soft_delete_and_tombstone_in_same_save()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "op_dep_tests";
            opts.Policies.AllDocumentsSoftDeleted();
        });

        // Create a doc and a stream
        await using var session1 = theStore.LightweightSession();
        var doc = new User { Id = Guid.NewGuid(), FirstName = "Delete", LastName = "Me" };
        session1.Store(doc);
        var streamId = Guid.NewGuid();
        session1.Events.StartStream(streamId, new QuestStarted("Tombstone Quest"));
        await session1.SaveChangesAsync();

        // Soft delete the doc + tombstone the stream in one save
        await using var session2 = theStore.LightweightSession();
        session2.Delete<User>(doc.Id);
        session2.Events.TombstoneStream(streamId);
        await session2.SaveChangesAsync();

        // Doc should be soft-deleted (not visible via load)
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(doc.Id);
        loaded.ShouldBeNull();

        // Stream should be completely gone
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldBeNull();
    }

    [Fact]
    public async Task eject_pending_changes_leaves_streams()
    {
        var streamId = Guid.NewGuid();
        theSession.Store(new User { Id = Guid.NewGuid(), FirstName = "Ejected" });
        theSession.Events.StartStream(streamId, new QuestStarted("Kept"));

        // EjectAllPendingChanges clears both ops and streams
        theSession.EjectAllPendingChanges();

        // After eject, there should be no outstanding work
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();

        // SaveChanges should be a no-op
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var state = await query.Events.FetchStreamStateAsync(streamId);
        state.ShouldBeNull();
    }

    [Fact]
    public async Task store_multiple_types_with_events_all_committed()
    {
        var userId = Guid.NewGuid();
        var stringDoc = new StringDoc { Id = $"test-{Guid.NewGuid():N}", Name = "Combo" };
        var stream1 = Guid.NewGuid();
        var stream2 = Guid.NewGuid();

        theSession.Store(new User { Id = userId, FirstName = "Multi", LastName = "Type" });
        theSession.Store(stringDoc);
        theSession.Events.StartStream(stream1, new QuestStarted("Quest A"));
        theSession.Events.StartStream(stream2, new QuestStarted("Quest B"));
        theSession.Events.Append(stream1, new MembersJoined(1, "Town", ["X"]));
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var user = await query.LoadAsync<User>(userId);
        user.ShouldNotBeNull();

        var loadedString = await query.LoadAsync<StringDoc>(stringDoc.Id);
        loadedString.ShouldNotBeNull();

        var events1 = await query.Events.FetchStreamAsync(stream1);
        events1.Count.ShouldBe(2);

        var events2 = await query.Events.FetchStreamAsync(stream2);
        events2.Count.ShouldBe(1);
    }
}
