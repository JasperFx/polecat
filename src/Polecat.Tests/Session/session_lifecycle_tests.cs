using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Session;

/// <summary>
///     Tests for session lifecycle edge cases: reuse, disposal, identity map behavior.
/// </summary>
[Collection("integration")]
public class session_lifecycle_tests : IntegrationContext
{
    public session_lifecycle_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    // ===== Session reuse across multiple saves =====

    [Fact]
    public async Task lightweight_session_reuse_across_saves()
    {
        await using var session = theStore.LightweightSession();

        var user1 = new User { Id = Guid.NewGuid(), FirstName = "Round1", LastName = "A", Age = 1 };
        session.Store(user1);
        await session.SaveChangesAsync();

        session.PendingChanges.HasOutstandingWork().ShouldBeFalse();

        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Round2", LastName = "B", Age = 2 };
        session.Store(user2);
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user1.Id)).ShouldNotBeNull();
        (await query.LoadAsync<User>(user2.Id)).ShouldNotBeNull();
    }

    // ===== Identity map session tracks stored documents =====

    [Fact]
    public async Task identity_session_returns_same_instance_on_load()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "Identity", LastName = "Map", Age = 30 });
        await theSession.SaveChangesAsync();

        await using var session = theStore.IdentitySession();
        var load1 = await session.LoadAsync<User>(id);
        var load2 = await session.LoadAsync<User>(id);

        // Identity map: same object reference
        ReferenceEquals(load1, load2).ShouldBeTrue();
    }

    // ===== Identity map after eject =====

    [Fact]
    public async Task identity_session_eject_clears_from_map()
    {
        var id = Guid.NewGuid();
        await using var session = theStore.IdentitySession();

        session.Store(new User { Id = id, FirstName = "Eject", LastName = "Me", Age = 1 });
        await session.SaveChangesAsync();

        var loaded1 = await session.LoadAsync<User>(id);
        loaded1.ShouldNotBeNull();

        session.Eject(loaded1);

        // After eject, loading again should get a fresh instance from DB
        var loaded2 = await session.LoadAsync<User>(id);
        loaded2.ShouldNotBeNull();
        ReferenceEquals(loaded1, loaded2).ShouldBeFalse();
    }

    // ===== Pending changes lost on dispose without save =====

    [Fact]
    public async Task pending_changes_lost_on_dispose_without_save()
    {
        var id = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new User { Id = id, FirstName = "Lost", LastName = "Data", Age = 99 });
            // Intentionally NOT calling SaveChangesAsync
        }

        // Document should not exist
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(id);
        loaded.ShouldBeNull();
    }

    // ===== SaveChanges with no pending work is no-op =====

    [Fact]
    public async Task save_changes_with_no_pending_work_is_noop()
    {
        // Should not throw
        await theSession.SaveChangesAsync();

        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }

    // ===== EjectAllPendingChanges clears everything =====

    [Fact]
    public async Task eject_all_pending_changes_clears_operations()
    {
        theSession.Store(new User { Id = Guid.NewGuid(), FirstName = "Eject1", LastName = "A", Age = 1 });
        theSession.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 1 });

        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        theSession.EjectAllPendingChanges();

        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
        theSession.PendingChanges.Operations.ShouldBeEmpty();
    }

    // ===== Query session is read-only =====

    [Fact]
    public async Task query_session_can_load_stored_documents()
    {
        var id = Guid.NewGuid();
        theSession.Store(new User { Id = id, FirstName = "ReadOnly", LastName = "Query", Age = 50 });
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("ReadOnly");
    }

    // ===== Store same document twice updates last =====

    [Fact]
    public async Task store_same_document_twice_last_write_wins()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "V1", LastName = "Win", Age = 1 };
        theSession.Store(user);

        user.FirstName = "V2";
        theSession.Store(user);

        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("V2");
    }

    // ===== Session with events and eject =====

    [Fact]
    public async Task eject_document_does_not_affect_event_streams()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Eject", LastName = "Streams", Age = 1 };
        var streamId = Guid.NewGuid();

        theSession.Store(user);
        theSession.Events.StartStream(streamId, new QuestStarted("Eject Stream Test"));

        // Eject the document but keep the stream
        theSession.Eject(user);

        theSession.PendingChanges.Operations.ShouldBeEmpty();
        theSession.PendingChanges.Streams.Count.ShouldBe(1);

        await theSession.SaveChangesAsync();

        // Document should NOT exist (ejected)
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user.Id)).ShouldBeNull();

        // Stream SHOULD exist (not ejected)
        var events = await query.Events.FetchStreamAsync(streamId);
        events.Count.ShouldBe(1);
    }
}
