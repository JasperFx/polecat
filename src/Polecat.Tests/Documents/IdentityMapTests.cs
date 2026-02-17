using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

[Collection("integration")]
public class IdentityMapTests : IntegrationContext
{
    public IdentityMapTests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task load_same_id_returns_same_reference()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Chewbacca" };

        await using var session = theStore.LightweightSession();
        session.Store(user);
        await session.SaveChangesAsync();

        await using var identitySession = theStore.IdentitySession();
        var first = await identitySession.LoadAsync<User>(user.Id);
        var second = await identitySession.LoadAsync<User>(user.Id);

        first.ShouldNotBeNull();
        ReferenceEquals(first, second).ShouldBeTrue();
    }

    [Fact]
    public async Task stored_document_visible_in_identity_map()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Lando" };

        await using var session = theStore.IdentitySession();
        session.Store(user);

        // Even before SaveChanges, the identity map should return the stored object
        var loaded = await session.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        ReferenceEquals(loaded, user).ShouldBeTrue();
    }

    [Fact]
    public async Task load_many_populates_identity_map()
    {
        var user1 = new User { Id = Guid.NewGuid(), FirstName = "R2D2" };
        var user2 = new User { Id = Guid.NewGuid(), FirstName = "C3PO" };

        await using var insertSession = theStore.LightweightSession();
        insertSession.Store(user1, user2);
        await insertSession.SaveChangesAsync();

        await using var session = theStore.IdentitySession();
        var loaded = await session.LoadManyAsync<User>([user1.Id, user2.Id]);
        loaded.Count.ShouldBe(2);

        // Subsequent loads should return the same references
        var r2 = await session.LoadAsync<User>(user1.Id);
        var c3 = await session.LoadAsync<User>(user2.Id);

        loaded.ShouldContain(d => ReferenceEquals(d, r2));
        loaded.ShouldContain(d => ReferenceEquals(d, c3));
    }

    [Fact]
    public async Task lightweight_session_returns_different_references()
    {
        var user = new User { Id = Guid.NewGuid(), FirstName = "Boba Fett" };

        await using var insertSession = theStore.LightweightSession();
        insertSession.Store(user);
        await insertSession.SaveChangesAsync();

        await using var session = theStore.LightweightSession();
        var first = await session.LoadAsync<User>(user.Id);
        var second = await session.LoadAsync<User>(user.Id);

        first.ShouldNotBeNull();
        second.ShouldNotBeNull();
        // Lightweight returns new instances each time
        ReferenceEquals(first, second).ShouldBeFalse();
    }
}
