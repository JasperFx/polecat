using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Documents;

/// <summary>
/// #218: a document with a plain <c>Guid Id</c> property should get a new Guid auto-assigned when
/// the id is default (Guid.Empty), exactly like strongly typed Guid wrappers do. Previously only
/// strong-typed Guid ids were assigned, so a plain Guid id persisted as 00000000-0000-0000-0000-…
/// </summary>
[Collection("integration")]
public class plain_guid_id_assignment_tests : IntegrationContext
{
    public plain_guid_id_assignment_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "plain_guid_id"; });
    }

    public class User
    {
        public Guid Id { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
    }

    public class IdentityAttributedUser
    {
        [Identity]
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task store_assigns_a_new_guid_when_id_is_empty()
    {
        var user = new User { FirstName = "Marco", LastName = "Minerva" };
        user.Id.ShouldBe(Guid.Empty);

        await using var session = theStore.LightweightSession();
        session.Store(user);
        await session.SaveChangesAsync();

        // The in-memory instance is updated...
        user.Id.ShouldNotBe(Guid.Empty);

        // ...and the row actually persisted under that id (not Guid.Empty).
        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<User>(user.Id);
        loaded.ShouldNotBeNull();
        loaded.FirstName.ShouldBe("Marco");
    }

    [Fact]
    public async Task store_preserves_an_explicitly_set_guid()
    {
        var id = Guid.NewGuid();
        var user = new User { Id = id, FirstName = "Explicit" };

        await using var session = theStore.LightweightSession();
        session.Store(user);
        await session.SaveChangesAsync();

        user.Id.ShouldBe(id); // not overwritten

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<User>(id)).ShouldNotBeNull();
    }

    [Fact]
    public async Task identity_attribute_on_guid_id_is_assigned()
    {
        var doc = new IdentityAttributedUser { Name = "Attr" };

        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync();

        doc.Id.ShouldNotBe(Guid.Empty);
        await using var query = theStore.QuerySession();
        (await query.LoadAsync<IdentityAttributedUser>(doc.Id)).ShouldNotBeNull();
    }
}
