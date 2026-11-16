using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     gh-1011: <c>OrderBy(key, comparer)</c> and its siblings were accepted with the comparer silently dropped,
///     so <c>StringComparer.Ordinal</c> came back in the column's (case-insensitive) collation order. A .NET
///     comparer cannot run in SQL Server, so every comparer overload is refused, on the document, GroupBy and
///     GroupJoin paths alike.
/// </summary>
public class ordering_comparer_refusal_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task SeedAsync()
    {
        ConfigureStore(_ => { });

        await using var session = theStore.LightweightSession();
        // Ordinal puts "Banana" first (uppercase sorts before lowercase); a case-insensitive collation does not.
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "apple", Age = 1 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Banana", Age = 2 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "cherry", Age = 3 });
        await session.SaveChangesAsync(Ct);
    }

    public static TheoryData<string> DocumentShapes =>
    [
        "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending"
    ];

    private static IQueryable<LinqTarget> Ordered(IQueryable<LinqTarget> source, string shape) => shape switch
    {
        "OrderBy" => source.OrderBy(x => x.Name, StringComparer.Ordinal),
        "OrderByDescending" => source.OrderByDescending(x => x.Name, StringComparer.Ordinal),
        "ThenBy" => source.OrderBy(x => x.Age).ThenBy(x => x.Name, StringComparer.Ordinal),
        "ThenByDescending" => source.OrderBy(x => x.Age).ThenByDescending(x => x.Name, StringComparer.Ordinal),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    [Theory]
    [MemberData(nameof(DocumentShapes))]
    public async Task a_comparer_on_a_document_ordering_is_refused(string shape)
    {
        await SeedAsync();
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(
            () => Ordered(query.Query<LinqTarget>(), shape).ToListAsync(Ct));

        ex.Message.ShouldContain("IComparer");
        ex.Message.ShouldContain(shape);
    }

    [Fact]
    public async Task a_comparer_after_a_where_and_before_paging_is_refused()
    {
        await SeedAsync();
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqTarget>()
            .Where(x => x.Age > 0)
            .OrderBy(x => x.Name, StringComparer.Ordinal)
            .Take(2)
            .ToListAsync(Ct));

        ex.Message.ShouldContain("IComparer");
    }

    [Fact]
    public async Task a_comparer_on_a_grouped_ordering_is_refused()
    {
        await SeedAsync();
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqTarget>()
            .GroupBy(x => x.Name)
            .Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Key, StringComparer.Ordinal)
            .ToListAsync(Ct));

        ex.Message.ShouldContain("IComparer");
    }

    [Fact]
    public async Task a_comparer_on_a_group_join_ordering_is_refused()
    {
        await SeedAsync();
        await using var query = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqTarget>()
            .GroupJoin(query.Query<LinqTarget>(), a => a.Id, b => b.Id, (a, bs) => new { a, bs })
            .SelectMany(x => x.bs, (x, b) => new { x.a.Name, b.Age })
            .OrderBy(r => r.Name, StringComparer.Ordinal)
            .ToListAsync(Ct));

        ex.Message.ShouldContain("IComparer");
    }

    [Fact]
    public async Task the_same_ordering_without_a_comparer_still_translates()
    {
        await SeedAsync();
        await using var query = theStore.QuerySession();

        var names = await query.Query<LinqTarget>()
            .OrderBy(x => x.Age)
            .Select(x => x.Name)
            .ToListAsync(Ct);

        names.ShouldBe(["apple", "Banana", "cherry"]);
    }
}
