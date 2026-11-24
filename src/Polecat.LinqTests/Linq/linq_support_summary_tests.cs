using System.Text.RegularExpressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class LinqSummaryDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public List<string> Tags { get; set; } = [];
    public Dictionary<string, int> Scores { get; set; } = new();
}

/// <summary>
///     #1137 item 3: the LINQ support summary at the top of docs/documents/querying/linq/index.md makes a
///     claim for every shape it lists. These pin the claims nothing else in this project pinned: a dictionary
///     key read in a predicate, and the exception each "Not supported" row names.
/// </summary>
public class linq_support_summary_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> aSeededSessionAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LinqSummaryDoc
            {
                Id = Guid.NewGuid(), Name = "alpha", Tags = ["red", "blue"],
                Scores = new Dictionary<string, int> { ["math"] = 90, ["art"] = 40 }
            });
            session.Store(new LinqSummaryDoc
            {
                Id = Guid.NewGuid(), Name = "beta", Tags = [],
                Scores = new Dictionary<string, int> { ["math"] = 50 }
            });
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    [Fact]
    public async Task a_dictionary_key_is_read_in_a_predicate_and_an_ordering()
    {
        await using var query = await aSeededSessionAsync();

        var high = await query.Query<LinqSummaryDoc>().Where(x => x.Scores["math"] > 80).ToListAsync(Ct);
        high.ShouldHaveSingleItem().Name.ShouldBe("alpha");

        var ordered = await query.Query<LinqSummaryDoc>().OrderBy(x => x.Scores["math"]).ToListAsync(Ct);
        ordered.Select(x => x.Name).ShouldBe(["beta", "alpha"]);
    }

    [Fact]
    public async Task a_case_insensitive_equals_is_translated()
    {
        await using var query = await aSeededSessionAsync();

        var found = await query.Query<LinqSummaryDoc>()
            .Where(x => x.Name.Equals("ALPHA", StringComparison.OrdinalIgnoreCase))
            .ToListAsync(Ct);

        found.ShouldHaveSingleItem().Name.ShouldBe("alpha");
    }

    [Fact]
    public async Task a_string_method_with_a_string_comparison_is_refused()
    {
        await using var query = await aSeededSessionAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Name.StartsWith("AL", StringComparison.OrdinalIgnoreCase)).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Name.Contains("LP", StringComparison.OrdinalIgnoreCase)).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Name.EndsWith("HA", StringComparison.OrdinalIgnoreCase)).ToListAsync(Ct));
    }

    [Fact]
    public async Task regex_and_compare_are_refused()
    {
        await using var query = await aSeededSessionAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => Regex.IsMatch(x.Name, "^al")).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Name.CompareTo("b") < 0).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => string.Compare(x.Name, "b") < 0).ToListAsync(Ct));
    }

    [Fact]
    public async Task any_all_and_the_count_method_on_a_child_collection_are_refused()
    {
        await using var query = await aSeededSessionAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Tags.Any()).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Tags.Any(t => t == "red")).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Tags.All(t => t == "red")).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() => query.Query<LinqSummaryDoc>()
            .Where(x => x.Tags.Count() > 1).ToListAsync(Ct));

        // What the docs send you to instead
        (await query.Query<LinqSummaryDoc>().Where(x => x.Tags.Count > 0).ToListAsync(Ct))
            .ShouldHaveSingleItem().Name.ShouldBe("alpha");
        (await query.Query<LinqSummaryDoc>().Where(x => x.Tags.Contains("red")).ToListAsync(Ct))
            .ShouldHaveSingleItem().Name.ShouldBe("alpha");
    }

    [Fact]
    public async Task the_dotnet_9_enumerable_only_operators_enumerate_synchronously_and_are_refused()
    {
        await using var query = await aSeededSessionAsync();

        // CountBy, AggregateBy and Index have no Queryable overloads, so they bind to Enumerable and enumerate
        // the query synchronously, which Polecat refuses. They do not run in memory.
        var ex = Should.Throw<NotSupportedException>(() =>
            query.Query<LinqSummaryDoc>().CountBy(x => x.Name).ToList());
        ex.ShouldNotBeAssignableTo<BadLinqExpressionException>();

        Should.Throw<NotSupportedException>(() => query.Query<LinqSummaryDoc>().Index().ToList());
    }
}
