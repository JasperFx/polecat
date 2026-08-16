using System.Linq.Expressions;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.StrongTypedId;

// polecat#472 / jasperfx#665: IQuerySession.LoadAsync<T>(object).
//
// Before this overload there was no spelling of a by-id load for a document keyed by a strong-typed
// identifier: the wrapper does not compile against Guid/string/int/long, and passing the wrapped
// primitive instead is a *runtime* fault on a store that does not accept it -- which is how the
// CritterWatch MCP get_alert tool shipped broken. The shared DocumentLoadAndStoreCompliance suite
// pins the Guid-wrapped case and the boxed-canonical case; these are the Polecat-side facts it does
// not reach: all four inner id types, the inner value accepted alongside its wrapper, the identity
// map treating the two spellings as one identity, the tenant-scoped session, and the two rejections.

public record struct TicketId(Guid Value);

public class Ticket
{
    public TicketId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record struct BadgeId(string Value);

public class Badge
{
    public BadgeId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record struct SeatId(int Value);

public class Seat
{
    public SeatId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record struct AisleId(long Value);

public class Aisle
{
    public AisleId Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class PlainTicket
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class PlainBadge
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

[Collection("integration")]
public class load_by_runtime_typed_identity : IntegrationContext
{
    public load_by_runtime_typed_identity(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "load_by_object_id"; });
    }

    [Fact]
    public async Task load_a_guid_wrapped_identity_by_its_wrapper()
    {
        var ticket = new Ticket { Id = new TicketId(Guid.NewGuid()), Name = "Wrapped Guid" };
        await PersistAsync(ticket);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Ticket>(ticket.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(ticket.Id);
        loaded.Name.ShouldBe("Wrapped Guid");
    }

    [Fact]
    public async Task load_a_string_wrapped_identity_by_its_wrapper()
    {
        var badge = new Badge { Id = new BadgeId("badge-472"), Name = "Wrapped string" };
        await PersistAsync(badge);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Badge>(badge.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(badge.Id);
        loaded.Name.ShouldBe("Wrapped string");
    }

    [Fact]
    public async Task load_an_int_wrapped_identity_by_its_wrapper()
    {
        var seat = new Seat { Name = "Wrapped int" };
        await PersistAsync(seat);
        seat.Id.Value.ShouldBeGreaterThan(0);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Seat>(seat.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(seat.Id);
        loaded.Name.ShouldBe("Wrapped int");
    }

    [Fact]
    public async Task load_a_long_wrapped_identity_by_its_wrapper()
    {
        var aisle = new Aisle { Name = "Wrapped long" };
        await PersistAsync(aisle);
        aisle.Id.Value.ShouldBeGreaterThan(0);

        await using var query = theStore.QuerySession();
        var loaded = await query.LoadAsync<Aisle>(aisle.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Id.ShouldBe(aisle.Id);
        loaded.Name.ShouldBe("Wrapped long");
    }

    /// <remarks>
    ///     The typed overloads already accept the wrapped inner value for a strong-typed-id document,
    ///     so the object overload has to as well or the two spellings would disagree about which rows
    ///     exist.
    /// </remarks>
    [Fact]
    public async Task the_wrapped_inner_value_resolves_the_same_row_as_the_wrapper()
    {
        var ticket = new Ticket { Id = new TicketId(Guid.NewGuid()), Name = "Either spelling" };
        await PersistAsync(ticket);

        await using var query = theStore.QuerySession();

        object inner = ticket.Id.Value;
        var byInner = await query.LoadAsync<Ticket>(inner, TestContext.Current.CancellationToken);
        var byWrapper = await query.LoadAsync<Ticket>(ticket.Id, TestContext.Current.CancellationToken);

        byInner.ShouldNotBeNull();
        byWrapper.ShouldNotBeNull();
        byInner.Id.ShouldBe(ticket.Id);
        byWrapper.Id.ShouldBe(ticket.Id);
    }

    /// <remarks>
    ///     A caller holding any identity in an <c>object</c>-typed local reaches this overload, not
    ///     only one holding a wrapper. The shared suite asserts the Guid and string halves; Polecat's
    ///     id set is wider, so the numeric halves are asserted here.
    /// </remarks>
    [Fact]
    public async Task boxed_canonical_identities_resolve_as_the_typed_overloads_do()
    {
        var ticket = new PlainTicket { Id = Guid.NewGuid(), Name = "Boxed Guid" };
        var badge = new PlainBadge { Id = "plain-badge", Name = "Boxed string" };
        var seat = new Seat { Name = "Boxed int" };
        var aisle = new Aisle { Name = "Boxed long" };

        await PersistAsync(ticket, badge, seat, aisle);

        await using var query = theStore.QuerySession();

        object guidIdentity = ticket.Id;
        (await query.LoadAsync<PlainTicket>(guidIdentity, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("Boxed Guid");

        object stringIdentity = badge.Id;
        (await query.LoadAsync<PlainBadge>(stringIdentity, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("Boxed string");

        object intIdentity = seat.Id.Value;
        (await query.LoadAsync<Seat>(intIdentity, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("Boxed int");

        object longIdentity = aisle.Id.Value;
        (await query.LoadAsync<Aisle>(longIdentity, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("Boxed long");
    }

    [Fact]
    public async Task a_missing_strong_typed_identity_returns_null()
    {
        await using var query = theStore.QuerySession();

        (await query.LoadAsync<Ticket>(new TicketId(Guid.NewGuid()), TestContext.Current.CancellationToken))
            .ShouldBeNull();
        (await query.LoadAsync<Badge>(new BadgeId("nothing-here"), TestContext.Current.CancellationToken))
            .ShouldBeNull();
    }

    /// <remarks>
    ///     The identity map is keyed on the unwrapped inner value, so an implementation that handed
    ///     the wrapper straight down would miss entries the typed overload put there and return a
    ///     second instance of the same document. That is the reason this overload normalizes through
    ///     <c>DocumentMapping.UnwrapIdentity</c> rather than opening a parallel load path.
    /// </remarks>
    [Fact]
    public async Task the_identity_map_treats_the_wrapper_and_its_inner_value_as_one_identity()
    {
        var ticket = new Ticket { Id = new TicketId(Guid.NewGuid()), Name = "Identity" };
        await PersistAsync(ticket);

        await using var session = theStore.IdentitySession();

        var byWrapper = await session.LoadAsync<Ticket>(ticket.Id, TestContext.Current.CancellationToken);
        var byTypedOverload = await session.LoadAsync<Ticket>(ticket.Id.Value, TestContext.Current.CancellationToken);

        byWrapper.ShouldNotBeNull();
        ReferenceEquals(byWrapper, byTypedOverload).ShouldBeTrue();
    }

    /// <remarks>
    ///     <c>ForTenant</c> returns a <c>NestedTenantSession</c>, which is a separate implementation of
    ///     the session contract — the shared contract's default implementation would throw
    ///     <see cref="NotSupportedException" /> here rather than resolve the wrapper.
    /// </remarks>
    [Fact]
    public async Task a_tenant_scoped_session_resolves_a_strong_typed_identity()
    {
        var ticket = new Ticket { Id = new TicketId(Guid.NewGuid()), Name = "Tenanted" };

        theSession.ForTenant("tenant-472").Store(ticket);
        await theSession.SaveChangesAsync(TestContext.Current.CancellationToken);

        var loaded = await theSession.ForTenant("tenant-472")
            .LoadAsync<Ticket>(ticket.Id, TestContext.Current.CancellationToken);

        loaded.ShouldNotBeNull();
        loaded.Name.ShouldBe("Tenanted");
    }

    [Fact]
    public async Task an_identity_of_the_wrong_type_names_the_document_and_both_id_types()
    {
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<ArgumentException>(
            () => query.LoadAsync<Ticket>(new BadgeId("wrong-shape"), TestContext.Current.CancellationToken));

        ex.Message.ShouldContain(nameof(BadgeId));
        ex.Message.ShouldContain(nameof(Ticket));
        ex.Message.ShouldContain(nameof(TicketId));
        ex.ParamName.ShouldBe("id");
    }

    /// <remarks>
    ///     Only an <c>object</c>-typed null reaches this overload. A <c>null</c> <i>literal</i> still
    ///     binds to <c>LoadAsync&lt;T&gt;(string)</c> — <c>string</c> is the more specific of the two
    ///     applicable members — which is unchanged by this addition and is asserted below so a future
    ///     change to the overload set cannot move it silently.
    /// </remarks>
    [Fact]
    public async Task a_null_identity_is_rejected()
    {
        await using var query = theStore.QuerySession();

        object? nothing = null;
        await Should.ThrowAsync<ArgumentNullException>(
            () => query.LoadAsync<Ticket>(nothing!, TestContext.Current.CancellationToken));

        BoundParameterType<Ticket>(s => s.LoadAsync<Ticket>(null!, default)).ShouldBe(typeof(string));
    }

    /// <remarks>
    ///     The point of the overload set is that adding <c>object</c> moves no existing call site: a
    ///     Guid/string/int/long argument still binds to its own member, and only an argument that fits
    ///     none of them lands on <c>object</c>. An expression tree records what the compiler actually
    ///     bound, which is the only way to assert overload resolution from a test.
    /// </remarks>
    [Fact]
    public void the_typed_overloads_stay_preferred_by_overload_resolution()
    {
        BoundParameterType<PlainTicket>(s => s.LoadAsync<PlainTicket>(Guid.NewGuid(), default))
            .ShouldBe(typeof(Guid));
        BoundParameterType<PlainBadge>(s => s.LoadAsync<PlainBadge>("id", default))
            .ShouldBe(typeof(string));
        BoundParameterType<PlainTicket>(s => s.LoadAsync<PlainTicket>(1, default))
            .ShouldBe(typeof(int));
        BoundParameterType<PlainTicket>(s => s.LoadAsync<PlainTicket>(1L, default))
            .ShouldBe(typeof(long));

        // ...and a strong-typed identifier, which fits none of them, is what reaches the new member.
        BoundParameterType<Ticket>(s => s.LoadAsync<Ticket>(new TicketId(Guid.NewGuid()), default))
            .ShouldBe(typeof(object));
    }

    private static Type BoundParameterType<T>(Expression<Func<IQuerySession, Task<T?>>> call) where T : notnull
        => ((MethodCallExpression)call.Body).Method.GetParameters()[0].ParameterType;

    private async Task PersistAsync(params object[] documents)
    {
        await using var session = theStore.LightweightSession();
        session.StoreObjects(documents);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }
}
