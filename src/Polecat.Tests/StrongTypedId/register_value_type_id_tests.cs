using Polecat.Linq;
using Polecat.Storage.ClosedShape;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

// polecat#751. Mirrors Marten's StoreOptions.RegisterValueTypeId (#5589). The registration exists for
// Native AOT, which Polecat.AotRuntimeSmoke measures -- including an UNregistered control that still
// fails there. These pin the JIT half of the contract: a registered strong-typed id takes the
// statically-closed path and behaves exactly like the reflective one, and an unregistered id still
// works through reflection, so the call stays optional off AOT.

public readonly record struct RegisteredTicketId(Guid Value);

public class RegisteredTicket
{
    public RegisteredTicketId Id { get; set; }
    public string Title { get; set; } = string.Empty;
}

public readonly record struct UnregisteredTicketId(Guid Value);

public class UnregisteredTicket
{
    public UnregisteredTicketId Id { get; set; }
    public string Title { get; set; } = string.Empty;
}

[Collection("integration")]
public class register_value_type_id_tests : IntegrationContext
{
    public register_value_type_id_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "register_value_type_id";
            opts.RegisterValueTypeId<RegisteredTicket, RegisteredTicketId, Guid>();
        });
    }

    [Fact]
    public void the_registration_roots_the_document_over_its_wrapper()
    {
        ValueTypeIdRegistry.IsRegistered(typeof(RegisteredTicket), typeof(RegisteredTicketId)).ShouldBeTrue();

        // Keyed on the document AND the wrapper: the storage is closed over both, so an id-only key
        // could not have rooted it.
        ValueTypeIdRegistry.IsRegistered(typeof(UnregisteredTicket), typeof(RegisteredTicketId)).ShouldBeFalse();
        ValueTypeIdRegistry.IsRegistered(typeof(UnregisteredTicket), typeof(UnregisteredTicketId)).ShouldBeFalse();
    }

    [Fact]
    public async Task a_registered_id_round_trips_filters_and_projects()
    {
        var id = new RegisteredTicketId(Guid.NewGuid());

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new RegisteredTicket { Id = id, Title = "registered" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<RegisteredTicket>(id, TestContext.Current.CancellationToken))!
            .Title.ShouldBe("registered");

        var filtered = await query.Query<RegisteredTicket>().Where(x => x.Id == id)
            .ToListAsync(TestContext.Current.CancellationToken);
        filtered.Single().Title.ShouldBe("registered");

        var ids = await query.Query<RegisteredTicket>().Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);
        ids.ShouldContain(id);
    }

    [Fact]
    public async Task a_registered_id_updates_and_deletes_through_identity_map_storage()
    {
        var id = new RegisteredTicketId(Guid.NewGuid());

        await using (var session = theStore.IdentitySession())
        {
            session.Store(new RegisteredTicket { Id = id, Title = "first" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);

            var loaded = await session.LoadAsync<RegisteredTicket>(id, TestContext.Current.CancellationToken);
            loaded!.Title = "second";
            session.Store(loaded);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var check = theStore.QuerySession())
        {
            (await check.LoadAsync<RegisteredTicket>(id, TestContext.Current.CancellationToken))!
                .Title.ShouldBe("second");
        }

        await using (var session = theStore.LightweightSession())
        {
            session.Delete<RegisteredTicket>(id.Value);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var after = theStore.QuerySession();
        (await after.LoadAsync<RegisteredTicket>(id, TestContext.Current.CancellationToken)).ShouldBeNull();
    }

    // Off AOT the call is optional: an unregistered strong-typed id still resolves reflectively.
    [Fact]
    public async Task an_unregistered_id_still_works_under_a_jit()
    {
        var id = new UnregisteredTicketId(Guid.NewGuid());

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new UnregisteredTicket { Id = id, Title = "unregistered" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<UnregisteredTicket>(id, TestContext.Current.CancellationToken))!
            .Title.ShouldBe("unregistered");
    }
}
