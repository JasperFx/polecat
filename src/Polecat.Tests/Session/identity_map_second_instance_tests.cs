using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Session;

/// <summary>
///     #959: Polecat's identity-map storage is Weasel's shared one (weasel#811), which refuses a second,
///     different instance of a document the session already maps — Marten's rule (weasel#740). Two
///     instances of one document in one session, both stored, is a last-write-wins outcome nobody can tell
///     from a lost update; Polecat 5.x replaced the mapped instance without a word.
/// </summary>
[Collection("integration")]
public class identity_map_second_instance_tests : IntegrationContext
{
    public identity_map_second_instance_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task storing_a_different_instance_under_a_mapped_id_is_refused()
    {
        var id = Guid.NewGuid();
        await using var session = theStore.IdentitySession();

        session.Store(new LinqTarget { Id = id, Name = "first" });

        var ex = Should.Throw<InvalidOperationException>(() => session.Store(new LinqTarget { Id = id, Name = "second" }));
        ex.Message.ShouldContain("already in this session's identity map");
    }

    [Fact]
    public async Task storing_the_mapped_instance_again_is_fine()
    {
        var doc = new LinqTarget { Id = Guid.NewGuid(), Name = "first" };
        await using var session = theStore.IdentitySession();

        session.Store(doc);
        doc.Name = "changed";
        session.Store(doc);

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<LinqTarget>(doc.Id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("changed");
    }

    [Fact]
    public async Task a_type_that_declares_equality_may_store_a_second_instance()
    {
        // A record implements IEquatable<T>, which is the type saying what "the same document" means.
        var id = Guid.NewGuid();
        await using var session = theStore.IdentitySession();

        session.Store(new EquatableIdentityDoc { Id = id, Name = "first" });
        session.Store(new EquatableIdentityDoc { Id = id, Name = "second" });

        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<EquatableIdentityDoc>(id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("second");
    }

    [Fact]
    public async Task ejecting_the_mapped_instance_lets_another_be_stored()
    {
        var id = Guid.NewGuid();
        await using var session = theStore.IdentitySession();

        var first = new LinqTarget { Id = id, Name = "first" };
        session.Store(first);
        session.Eject(first);

        session.Store(new LinqTarget { Id = id, Name = "second" });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<LinqTarget>(id, TestContext.Current.CancellationToken))
            .ShouldNotBeNull().Name.ShouldBe("second");
    }
}

public record EquatableIdentityDoc
{
    public Guid Id { get; set; }
    public string? Name { get; set; }
}
