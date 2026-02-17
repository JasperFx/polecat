using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

[Collection("integration")]
public class DocumentSessionTests : IntegrationContext
{
    public DocumentSessionTests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task store_and_load_guid_id_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Han", LastName = "Solo", Age = 35 };

        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);

        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Han");
        loaded.LastName.ShouldBe("Solo");
        loaded.Age.ShouldBe(35);
    }

    [Fact]
    public async Task store_and_load_string_id_document()
    {
        var doc = new StringDoc { Id = "doc-" + Guid.NewGuid(), Name = "Test Document" };

        theSession.Store(doc);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<StringDoc>(doc.Id);

        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("Test Document");
    }

    [Fact]
    public async Task load_returns_null_for_nonexistent_document()
    {
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(Guid.NewGuid());
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task insert_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Luke", LastName = "Skywalker", Age = 25 };

        theSession.Insert(user);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);

        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Luke");
    }

    [Fact]
    public async Task insert_duplicate_throws()
    {
        var id = Guid.NewGuid();
        var user = new User { Id = id, FirstName = "Leia", LastName = "Organa" };

        theSession.Insert(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var duplicate = new User { Id = id, FirstName = "Fake", LastName = "Organa" };
        session2.Insert(duplicate);

        var ex = await Should.ThrowAsync<DocumentAlreadyExistsException>(
            session2.SaveChangesAsync());

        ex.DocumentType.ShouldBe(typeof(User));
        ex.Id.ShouldBe(id);
    }

    [Fact]
    public async Task update_existing_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Anakin", LastName = "Skywalker", Age = 20 };

        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var updated = new User { Id = user.Id, FirstName = "Darth", LastName = "Vader", Age = 45 };
        session2.Update(updated);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);

        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Darth");
        loaded.LastName.ShouldBe("Vader");
        loaded.Age.ShouldBe(45);
    }

    [Fact]
    public async Task delete_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Obi-Wan", LastName = "Kenobi" };

        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete<User>(user.Id);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task delete_by_entity()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Yoda" };

        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        session2.Delete(user);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldBeNull();
    }

    [Fact]
    public async Task load_many_returns_correct_set()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "User1" };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "User2" };
        var user3 = new User { Id = Guid.NewGuid(), FirstName = "User3" };

        theSession.Store(user1, user2, user3);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<User>([user1.Id, user3.Id]);

        loaded.Count.ShouldBe(2);
        loaded.ShouldContain(u => u.FirstName == "User1");
        loaded.ShouldContain(u => u.FirstName == "User3");
    }

    [Fact]
    public async Task load_many_with_empty_ids_returns_empty()
    {
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadManyAsync<User>(Array.Empty<Guid>());
        loaded.Count.ShouldBe(0);
    }

    [Fact]
    public async Task store_upserts_existing_document()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Original", Age = 1 };

        theSession.Store(user);
        await theSession.SaveChangesAsync();

        await using var session2 = theStore.LightweightSession();
        var updated = new User { Id = user.Id, FirstName = "Updated", Age = 2 };
        session2.Store(updated);
        await session2.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Updated");
        loaded.Age.ShouldBe(2);
    }

    [Fact]
    public async Task multiple_operations_in_single_save_changes()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "First" };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "Second" };
        var target = new Target { Id = Guid.NewGuid(), Color = "Red", Number = 42 };

        theSession.Store(user1);
        theSession.Store(user2);
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(user1.Id)).ShouldNotBeNull();
        (await query.LoadAsync<User>(user2.Id)).ShouldNotBeNull();
        (await query.LoadAsync<Target>(target.Id)).ShouldNotBeNull();
    }

    [Fact]
    public async Task save_changes_with_no_ops_is_noop()
    {
        // Should not throw
        await theSession.SaveChangesAsync();
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }

    [Fact]
    public async Task pending_changes_tracks_operations()
    {
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();

        var user = new User { Id = Guid.NewGuid(), FirstName = "Test" };
        theSession.Store(user);

        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();
        theSession.PendingChanges.Operations.Count.ShouldBe(1);
    }

    [Fact]
    public async Task pending_changes_cleared_after_save()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Test" };
        theSession.Store(user);
        theSession.PendingChanges.HasOutstandingWork().ShouldBeTrue();

        await theSession.SaveChangesAsync();
        theSession.PendingChanges.HasOutstandingWork().ShouldBeFalse();
    }
}
