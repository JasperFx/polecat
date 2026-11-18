using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Unions;

/// <summary>
///     #808: LINQ over a C# union-typed document member: which case it holds (a type test), a member of
///     the case, a primitive case read as itself, and the default union. The type test is rendered by
///     Weasel's shared <c>SqlServerUnionSqlDialect</c> (weasel#744).
/// </summary>
/// <remarks>
///     The union pattern <c>x.Payment is UnionCard</c> does not compile in an expression tree (CS8122), so
///     every query here reaches the case through <c>IUnion.Value</c>, which is what users can write.
/// </remarks>
public class union_member_linq_tests: OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static UnionOrder order(string name, UnionPayment payment, UnionCode code = default,
        UnionPayment? maybe = null, UnionPayment? held = null) => new()
    {
        Id = Guid.NewGuid(),
        Name = name,
        Payment = payment,
        Code = code,
        Maybe = maybe,
        Holder = held == null ? null : new UnionHolder { Payment = held.Value }
    };

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(
            order("card-1234", new UnionPayment(new UnionCard("1234")), new UnionCode(5),
                maybe: new UnionPayment(new UnionCard("9999")), held: new UnionPayment(new UnionCash(1m))),
            order("card-5678", new UnionPayment(new UnionCard("5678")), new UnionCode("abc")),
            order("cash-10", new UnionPayment(new UnionCash(10m)), new UnionCode(2),
                held: new UnionPayment(new UnionCard("4444"))),
            order("cash-250", new UnionPayment(new UnionCash(250m)), new UnionCode("5")),
            order("none", default, default));
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<string[]> names(IQueryable<UnionOrder> query)
        => (await query.Select(x => x.Name).ToListAsync(Ct)).Order().ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task type_test_on_an_object_case(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => x.Payment.Value is UnionCard))).ShouldBe(["card-1234", "card-5678"]);
        (await names(docs.Where(x => x.Payment.Value is UnionCash))).ShouldBe(["cash-10", "cash-250"]);
        (await names(docs.Where(x => !(x.Payment.Value is UnionCard))))
            .ShouldBe(["cash-10", "cash-250", "none"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_documents_read_back_with_their_cases(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var cards = await query.Query<UnionOrder>().Where(x => x.Payment.Value is UnionCard)
            .OrderBy(x => x.Name).ToListAsync(Ct);

        cards.Select(x => x.Payment.Value).ShouldBe([new UnionCard("1234"), new UnionCard("5678")]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_of_a_case(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => ((UnionCard)x.Payment.Value!).Last4 == "5678"))).ShouldBe(["card-5678"]);
        (await names(docs.Where(x => (x.Payment.Value as UnionCash)!.Amount > 50m))).ShouldBe(["cash-250"]);
        (await names(docs.Where(x => x.Payment.Value is UnionCash && ((UnionCash)x.Payment.Value).Amount < 50m)))
            .ShouldBe(["cash-10"]);
        (await names(docs.Where(x => ((UnionCard)x.Payment.Value!).Last4.StartsWith("12"))))
            .ShouldBe(["card-1234"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_case_member_is_only_read_while_the_union_holds_that_case(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        // Every Cash row has no Last4, and no Card row has an Amount: a negated comparison must not pick
        // up the rows of the other case as though the member were simply missing on a Card.
        (await names(docs.Where(x => ((UnionCard)x.Payment.Value!).Last4 != "1234"))).ShouldBe(["card-5678"]);
        (await names(docs.Where(x => ((UnionCash)x.Payment.Value!).Amount != 10m))).ShouldBe(["cash-250"]);

        // The case member is NULL on every row of another case, and no Card here has a null Last4.
        (await names(docs.Where(x => ((UnionCard)x.Payment.Value!).Last4 == null))).ShouldBeEmpty();
        (await names(docs.Where(x => ((UnionCard)x.Payment.Value!).Last4 != null))).ShouldBe(["card-1234", "card-5678"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ordering_and_projecting_a_case_member(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var amounts = await query.Query<UnionOrder>().Where(x => x.Payment.Value is UnionCash)
            .OrderByDescending(x => ((UnionCash)x.Payment.Value!).Amount)
            .Select(x => x.Name).ToListAsync(Ct);

        amounts.ShouldBe(["cash-250", "cash-10"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_primitive_case(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => x.Code.Value is int))).ShouldBe(["card-1234", "cash-10"]);
        (await names(docs.Where(x => x.Code.Value is string))).ShouldBe(["card-5678", "cash-250"]);

        // "5" is a string, not the number 5: JSON_VALUE alone could not tell them apart.
        (await names(docs.Where(x => (int)x.Code.Value! > 3))).ShouldBe(["card-1234"]);
        (await names(docs.Where(x => (int)x.Code.Value! == 5))).ShouldBe(["card-1234"]);
        (await names(docs.Where(x => (string)x.Code.Value! == "5"))).ShouldBe(["cash-250"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_default_union(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => x.Payment.Value == null))).ShouldBe(["none"]);
        (await names(docs.Where(x => x.Payment.Value != null)))
            .ShouldBe(["card-1234", "card-5678", "cash-10", "cash-250"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_cast_compared_to_null_is_a_type_test(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => (x.Payment.Value as UnionCard) != null))).ShouldBe(["card-1234", "card-5678"]);
        (await names(docs.Where(x => (x.Payment.Value as UnionCard) == null)))
            .ShouldBe(["cash-10", "cash-250", "none"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_nullable_union_and_a_nested_one(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<UnionOrder>();

        (await names(docs.Where(x => x.Maybe!.Value.Value is UnionCard))).ShouldBe(["card-1234"]);
        (await names(docs.Where(x => x.Holder!.Payment.Value is UnionCard))).ShouldBe(["cash-10"]);
        (await names(docs.Where(x => ((UnionCash)x.Holder!.Payment.Value!).Amount == 1m))).ShouldBe(["card-1234"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task one_case_whose_shape_contains_the_other(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(
                new UnionLedgerDoc { Id = Guid.NewGuid(), Name = "in", Entry = new UnionDeposited(10m) },
                new UnionLedgerDoc { Id = Guid.NewGuid(), Name = "out", Entry = new UnionWithdrawn(10m, "rent") },
                new UnionLedgerDoc { Id = Guid.NewGuid(), Name = "in-big", Entry = new UnionDeposited(500m) });
            await session.SaveChangesAsync(Ct);
        }

        // STJ can write this union but cannot read it back, so these queries never materialize one.
        await using var query = theStore.QuerySession();
        var docs = query.Query<UnionLedgerDoc>();

        (await docs.Where(x => x.Entry.Value is UnionDeposited).Select(x => x.Name).ToListAsync(Ct))
            .Order().ShouldBe(["in", "in-big"]);
        (await docs.Where(x => x.Entry.Value is UnionWithdrawn).Select(x => x.Name).ToListAsync(Ct))
            .ShouldBe(["out"]);

        // The Withdrawn row has an Amount of 10 too; it is not a Deposited.
        (await docs.Where(x => ((UnionDeposited)x.Entry.Value!).Amount == 10m).Select(x => x.Name).ToListAsync(Ct))
            .ShouldBe(["in"]);
    }

    private static async Task refused(Func<Task> query, params string[] fragments)
    {
        var message = (await Should.ThrowAsync<BadLinqExpressionException>(query)).Message;
        foreach (var fragment in fragments) message.ShouldContain(fragment);
    }

    [Fact]
    public async Task refusals()
    {
        ConfigureStore(_ => { });
        await using var query = theStore.QuerySession();
        var docs = query.Query<UnionOrder>();
        var others = query.Query<UnionRefusalDoc>();

        // A type no case is.
        await refused(() => docs.Where(x => x.Payment.Value is UnionDeposited).ToListAsync(Ct),
            "is not a case of the union", "UnionCard, UnionCash");

        // Identical shapes, one JSON kind twice, and a case that can be written like another.
        await refused(() => others.Where(x => x.Length.Value is UnionMeters).ToListAsync(Ct),
            "can look exactly like", "UnionFeet");
        await refused(() => others.Where(x => ((UnionFeet)x.Length.Value!).Value > 1).ToListAsync(Ct),
            "can look exactly like");
        await refused(() => others.Where(x => x.Number.Value is int).ToListAsync(Ct), "both written as a JSON Number");
        await refused(() => others.Where(x => x.Tag.Value is UnionPlain).ToListAsync(Ct), "UnionTagged");

        // A nested union case.
        await refused(() => others.Where(x => x.Nested.Value is UnionPayment).ToListAsync(Ct), "itself a union");

        // The union itself compared to a value, or sorted and grouped by.
        await refused(() => docs.Where(x => x.Code.Value == (object)5).ToListAsync(Ct), "only compared to null");
        await refused(() => docs.OrderBy(x => x.Payment).ToListAsync(Ct), "no one value SQL can sort");
        await refused(() => docs.OrderBy(x => x.Payment.Value).ToListAsync(Ct), "no one value SQL can sort");

        // A type test on anything but a union's Value.
        await refused(() => others.Where(x => x.Anything is string).ToListAsync(Ct),
            "only type test it can answer");
    }
}
