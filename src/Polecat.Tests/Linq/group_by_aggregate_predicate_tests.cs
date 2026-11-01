using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #837 (the twin of marten#5625): <c>g.Count(predicate)</c> discarded its predicate and counted
///     the whole group, in a Select and in a HAVING alike. T-SQL has no <c>FILTER</c>, so it is
///     <c>COUNT(CASE WHEN ... THEN 1 END)</c>.
/// </summary>
public class group_by_aggregate_predicate_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alpha", Age = 10 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Alpha", Age = 20 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Beta", Age = 40 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Beta", Age = 50 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Beta", Age = 60 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Gamma", Age = 70 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "Gamma", Age = 71 });
        await session.SaveChangesAsync(Token);
    }

    [Fact]
    public async Task a_count_predicate_in_select_counts_only_what_matches()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Over = g.Count(x => x.Age > 30) })
            .ToListAsync(Token);

        rows.Single(x => x.Name == "Alpha").Over.ShouldBe(0);
        rows.Single(x => x.Name == "Beta").Over.ShouldBe(3);
        rows.Single(x => x.Name == "Gamma").Over.ShouldBe(2);
    }

    [Fact]
    public async Task a_scalar_count_predicate()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => g.Count(x => x.Age > 45))
            .ToListAsync(Token);

        rows.OrderBy(x => x).ShouldBe([0, 2, 2]);
    }

    [Fact]
    public async Task a_long_count_predicate()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Over = g.LongCount(x => x.Age > 30) })
            .ToListAsync(Token);

        rows.Single(x => x.Name == "Alpha").Over.ShouldBe(0L);
        rows.Single(x => x.Name == "Beta").Over.ShouldBe(3L);
    }

    [Fact]
    public async Task two_predicates_in_one_projection_stay_independent()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => new
            {
                Name = g.Key,
                Young = g.Count(x => x.Age < 50),
                Old = g.Count(x => x.Age >= 50),
                All = g.Count()
            })
            .ToListAsync(Token);

        var beta = rows.Single(x => x.Name == "Beta");
        beta.Young.ShouldBe(1);
        beta.Old.ShouldBe(2);
        beta.All.ShouldBe(3);
    }

    [Fact]
    public async Task a_compound_predicate()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Middle = g.Count(x => x.Age > 40 && x.Age < 60) })
            .ToListAsync(Token);

        rows.Single(x => x.Name == "Beta").Middle.ShouldBe(1);
    }

    [Fact]
    public async Task a_count_predicate_in_having_filters_the_groups()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var keys = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Where(g => g.Count(x => x.Age > 30) >= 1)
            .Select(g => g.Key)
            .ToListAsync(Token);

        keys.ShouldNotContain("Alpha");
        keys.Count.ShouldBe(2);
    }

    [Fact]
    public async Task the_same_predicate_in_select_and_having_binds_on_both()
    {
        await SeedAsync();

        var threshold = 30;

        await using var query = theStore.QuerySession();
        var rows = await query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Where(g => g.Count(x => x.Age > threshold) == 2)
            .Select(g => new { Name = g.Key, Over = g.Count(x => x.Age > threshold) })
            .ToListAsync(Token);

        rows.ShouldHaveSingleItem().Name.ShouldBe("Gamma");
    }

    [Fact]
    public async Task a_predicate_that_needs_a_subquery_is_refused_with_a_reason()
    {
        await using var query = theStore.QuerySession();

        // Msg 130 otherwise: SQL Server cannot aggregate over the OPENJSON this translates to.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<LinqTarget>().GroupBy(x => x.Name)
                .Select(g => new { Name = g.Key, Tagged = g.Count(x => x.Tags.Contains("a")) })
                .ToListAsync(Token));

        ex.Message.ShouldContain("subquery");
    }

    // CWE-89, the group_by_having_sql_injection class. The whole reason the select list had to stop
    // being a plain string: the value has to reach a parameter from inside the projection.
    [Fact]
    public async Task a_predicate_value_is_bound_rather_than_rendered()
    {
        await SeedAsync();

        const string attack = "x' OR '1'='1";

        await using var query = theStore.QuerySession();

        var grouped = query.Query<LinqTarget>().GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Hits = g.Count(x => x.Name == attack) });

        query.ToSql(grouped).ShouldNotContain("OR '1'='1");

        var rows = await grouped.ToListAsync(Token);
        rows.ShouldAllBe(x => x.Hits == 0);
    }
}
