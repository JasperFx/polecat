using Polecat.Linq;
using Polecat.Pagination;
using Polecat.Tests.Harness;
using Shouldly;
using Vogen;

namespace Polecat.Tests.StrongTypedId;

// Mirrors Marten's ValueTypeTests/linq_querying_with_value_types: several value-typed members on one
// document, ordered and filtered by, with the results projected back out as value types. The point is
// the *combination* — order by one wrapper, filter on another, project a third — because each of
// those reaches the member resolver by a different route.

[ValueObject<int>]
public readonly partial struct UpperLimit;

[ValueObject<int>]
public readonly partial struct LowerLimit;

public class LimitedDoc
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public UpperLimit Upper { get; set; }
    public LowerLimit Lower { get; set; }
}

[Collection("integration")]
public class linq_querying_with_value_types : IntegrationContext
{
    public linq_querying_with_value_types(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "linq_value_types"; });
    }

    private static readonly LimitedDoc Doc1 = new() { Lower = LowerLimit.From(1), Upper = UpperLimit.From(20) };
    private static readonly LimitedDoc Doc2 = new() { Lower = LowerLimit.From(5), Upper = UpperLimit.From(25) };
    private static readonly LimitedDoc Doc3 = new() { Lower = LowerLimit.From(4), Upper = UpperLimit.From(15) };
    private static readonly LimitedDoc Doc4 = new() { Lower = LowerLimit.From(3), Upper = UpperLimit.From(10) };

    private async Task<IDocumentSession> seed()
    {
        var session = theStore.LightweightSession();
        session.Store(Doc1, Doc2, Doc3, Doc4);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        AsyncDisposables.Add(session);
        return session;
    }

    [Fact]
    public async Task store_several_and_order_by()
    {
        var session = await seed();

        var ordered = await session.Query<LimitedDoc>()
            .OrderBy(x => x.Lower)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.ShouldBe([Doc1.Id, Doc4.Id, Doc3.Id, Doc2.Id]);
    }

    [Fact]
    public async Task store_several_and_order_by_descending()
    {
        var session = await seed();

        var ordered = await session.Query<LimitedDoc>()
            .OrderByDescending(x => x.Upper)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.ShouldBe([Doc2.Id, Doc1.Id, Doc3.Id, Doc4.Id]);
    }

    [Fact]
    public async Task store_several_and_query_by()
    {
        var session = await seed();

        var ordered = await session.Query<LimitedDoc>()
            .OrderBy(x => x.Lower)
            .Where(x => x.Upper == UpperLimit.From(10))
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.ShouldBe([Doc4.Id]);
    }

    [Fact]
    public async Task store_several_and_query_by_inequality()
    {
        // Vogen does not emit comparison operators, so a range predicate goes through the wrapper's
        // inner value. That resolves the member by its JSON path rather than as a wrapper, which is
        // the other half of the member-resolution rule and worth pinning alongside equality.
        var session = await seed();

        var ordered = await session.Query<LimitedDoc>()
            .Where(x => x.Upper != Doc4.Upper)
            .OrderBy(x => x.Lower)
            .Select(x => x.Id)
            .ToListAsync(TestContext.Current.CancellationToken);

        ordered.ShouldBe([Doc1.Id, Doc3.Id, Doc2.Id]);
    }

    [Fact]
    public async Task store_several_and_query_by_with_paging()
    {
        var session = await seed();

        var paged = await session.Query<LimitedDoc>()
            .OrderBy(x => x.Lower)
            .Where(x => x.Upper == UpperLimit.From(10))
            .Select(x => x.Id)
            .ToPagedListAsync(1, 10, TestContext.Current.CancellationToken);

        paged.ShouldBe([Doc4.Id]);
        paged.TotalItemCount.ShouldBe(1);
    }

    [Fact]
    public async Task store_several_and_select_the_value_types()
    {
        var session = await seed();

        var uppers = await session.Query<LimitedDoc>()
            .OrderBy(x => x.Lower)
            .Select(x => x.Upper)
            .ToListAsync(TestContext.Current.CancellationToken);

        uppers.ShouldBe([Doc1.Upper, Doc4.Upper, Doc3.Upper, Doc2.Upper]);
    }

    [Fact]
    public async Task store_several_and_count_by_a_value_type_predicate()
    {
        var session = await seed();

        var count = await session.Query<LimitedDoc>()
            .Where(x => x.Lower.IsOneOf(Doc1.Lower, Doc4.Lower))
            .CountAsync(TestContext.Current.CancellationToken);

        count.ShouldBe(2);
    }
}
