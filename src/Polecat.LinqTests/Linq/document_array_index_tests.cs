using System.Linq.Expressions;
using System.Text.Json.Serialization;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1014 (marten#5949, marten#5943, 23adf23e9): an index into a document's arrays and lists -- x.Values[1],
// x.Grid[1][0], x.Children[0].Name, x.Values.ElementAt(2) -- in Where, OrderBy and Select, read as a JSON path.
// Ported from Marten's cases.

public class CalcIndexedDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Category { get; set; } = "";
    public int[] Values { get; set; } = [];
    public List<int> List { get; set; } = [];
    public string[] Names { get; set; } = [];
    public List<string> NameList { get; set; } = [];
    public bool[] Flags { get; set; } = [];
    public double[] Ratios { get; set; } = [];
    public int[][] Grid { get; set; } = [];
    public List<List<int>> Nested { get; set; } = [];
    public string[][] Words { get; set; } = [];
    public int[][][] Cube { get; set; } = [];
    public CalcIndexedChild[] Children { get; set; } = [];
    public List<CalcIndexedChild> ChildList { get; set; } = [];
    public CalcIndexedChild[][] ChildGrid { get; set; } = [];
}

public record CalcIndexedChild
{
    public string Name { get; set; } = "";
    public int Score { get; set; }
}

public record CalcIndexedProjection(int Number, int Cell, string Word);

// marten#5949: an index into an array on the document translated in Marten only in a single-index Where()
// comparison. Polecat now translates an index into a document member (ArrayIndex, get_Item and ElementAt, in
// Linq/Members/DocumentPaths.cs) into a JSON path, and the probes pin Marten's answers where Polecat answers.
public class document_array_index_tests : OneOffConfigurationsContext
{
    internal static readonly CalcIndexedDoc[] Docs = Enumerable.Range(1, 6).Select(i => new CalcIndexedDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Category = i % 2 == 0 ? "even" : "odd",
        Values = [i, 10 - i, i * 3],
        List = [i * 2, 7 - i, i],
        Names = ["n" + i, "m" + (7 - i)],
        NameList = ["p" + (7 - i), "q" + i],
        Flags = [i % 2 == 0, i > 3],
        Ratios = [i / 4.0, (7 - i) / 2.0],
        Grid = [[i, 7 - i], [i * 2, 10 - i]],
        Nested = [[7 - i, i], [i % 3, i]],
        Words = [["a" + i, "b" + (7 - i)], ["c" + i % 2, "d" + i]],
        Cube = [[[i, 1], [2, 7 - i]], [[i * 3, 4], [5, i % 4]]],
        Children =
        [
            new CalcIndexedChild { Name = "c" + (7 - i), Score = i * 2 },
            new CalcIndexedChild { Name = "x" + i, Score = 10 - i }
        ],
        ChildList =
        [
            new CalcIndexedChild { Name = "y" + i, Score = 7 - i },
            new CalcIndexedChild { Name = "z" + (7 - i), Score = i }
        ],
        ChildGrid =
        [
            [new CalcIndexedChild { Name = "g" + i, Score = i }],
            [new CalcIndexedChild { Name = "h" + (7 - i), Score = 7 - i }]
        ]
    }).ToArray();

    private static CancellationToken Ct => CalcQueries.Ct;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in Docs) session.Store(doc);
        await session.SaveChangesAsync(Ct);
    }

    #region Where

    public static TheoryData<string> PredicateNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in CalcIndexedPredicates.All().Keys) data.Add(name);
        return data;
    }

    // marten#5949
    [Theory]
    [MemberData(nameof(PredicateNames))]
    public async Task where_on_an_index_matches_linq_to_objects(string name)
    {
        await SeedAsync();

        var predicate = CalcIndexedPredicates.All()[name];
        var expected = Docs.AsQueryable().Where(predicate).Select(x => x.Number).OrderBy(x => x).ToList();

        expected.ShouldNotBeEmpty(name);
        expected.Count.ShouldBeLessThan(Docs.Length, name);

        await using var session = theStore.QuerySession();
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Where(predicate).Select(x => x.Number).ToListAsync(Ct),
            actual => actual.OrderBy(x => x).ToList().ShouldBe(expected, name));
    }

    // marten#5949
    [Fact]
    public async Task an_index_past_the_end_matches_nothing()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Where(x => x.Values[7] == 3).ToListAsync(Ct),
            r => r.ShouldBeEmpty());
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Where(x => x.Grid[1][7] == 3).ToListAsync(Ct),
            r => r.ShouldBeEmpty());
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Where(x => x.Grid[7][0] == 3).ToListAsync(Ct),
            r => r.ShouldBeEmpty());
    }

    // marten#5949: Marten reads a missing element as null/default. Polecat projects a Select() in memory, where
    // the index throws like LINQ to Objects does; a clear refusal would also do, a raw IndexOutOfRangeException
    // is the divergence this probe reports.
    [Fact]
    public async Task selecting_an_index_past_the_end_reads_default()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Select(x => x.Values[7]).ToListAsync(Ct),
            r => r.ShouldAllBe(x => x == 0));
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Select(x => x.Names[7]).ToListAsync(Ct),
            r => r.ShouldAllBe(x => x == null));
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().Select(x => x.Grid[1][7]).ToListAsync(Ct),
            r => r.ShouldAllBe(x => x == 0));
    }

    // marten#5949: a negative index is refused with BadLinqExpressionException in Marten
    [Fact]
    public async Task a_negative_index_in_where_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var index = -1;

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            session.Query<CalcIndexedDoc>().Where(x => x.Values[index] == 3).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            session.Query<CalcIndexedDoc>().Where(x => x.Grid[0][index] == 3).ToListAsync(Ct));
    }

    // marten#5949
    [Fact]
    public async Task a_negative_index_in_order_by_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var index = -1;

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            session.Query<CalcIndexedDoc>().OrderBy(x => x.Values[index]).ToListAsync(Ct));
    }

    // marten#5949
    [Fact]
    public async Task a_negative_index_in_select_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var index = -1;

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            session.Query<CalcIndexedDoc>().Select(x => x.List[index]).ToListAsync(Ct));
    }

    #endregion

    #region OrderBy

    private async Task orderingMatchesOrRefused<TKey>(Expression<Func<CalcIndexedDoc, TKey>> key, bool descending)
    {
        await SeedAsync();

        var expected = (descending ? Docs.AsQueryable().OrderByDescending(key) : Docs.AsQueryable().OrderBy(key))
            .ThenBy(x => x.Number).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        await CalcQueries.ShouldMatchAsync(
            () => (descending
                    ? session.Query<CalcIndexedDoc>().OrderByDescending(key)
                    : session.Query<CalcIndexedDoc>().OrderBy(key))
                .ThenBy(x => x.Number).Select(x => x.Number).ToListAsync(Ct),
            actual => actual.ShouldBe(expected));
    }

    // marten#5949
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_value_array_index(bool descending) =>
        orderingMatchesOrRefused(x => x.Values[1], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_value_array_index_from_a_local(bool descending)
    {
        var index = 2;
        return orderingMatchesOrRefused(x => x.Values[index], descending);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_list_indexer(bool descending) =>
        orderingMatchesOrRefused(x => x.List[1], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_element_at(bool descending) =>
        orderingMatchesOrRefused(x => x.Values.ElementAt(2), descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_string_array_index(bool descending) =>
        orderingMatchesOrRefused(x => x.Names[1], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_double_array_index(bool descending) =>
        orderingMatchesOrRefused(x => x.Ratios[1], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_jagged_array_index(bool descending) =>
        orderingMatchesOrRefused(x => x.Grid[1][0], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_nested_list_indexer(bool descending) =>
        orderingMatchesOrRefused(x => x.Nested[1][0], descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_three_deep_index(bool descending) =>
        orderingMatchesOrRefused(x => x.Cube[1][1][1], descending);

    // A member read off an indexed element is a MemberExpression, the one shape Polecat's HandleOrderBy accepts,
    // so this is where a silently wrong JSON path would show up.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_member_of_indexed_child(bool descending) =>
        orderingMatchesOrRefused(x => x.Children[1].Score, descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_member_of_list_indexed_child(bool descending) =>
        orderingMatchesOrRefused(x => x.ChildList[0].Name, descending);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task order_by_member_of_jagged_indexed_child(bool descending) =>
        orderingMatchesOrRefused(x => x.ChildGrid[1][0].Score, descending);

    // marten#5949
    [Fact]
    public async Task then_by_an_index()
    {
        await SeedAsync();
        var expected = Docs.OrderBy(x => x.Category).ThenByDescending(x => x.Grid[1][1]).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcIndexedDoc>().OrderBy(x => x.Category)
                .ThenByDescending(x => x.Grid[1][1]).Select(x => x.Number).ToListAsync(Ct),
            actual => actual.ShouldBe(expected));
    }

    #endregion

    #region Select

    private async Task selectMatches<T>(Expression<Func<CalcIndexedDoc, T>> projection)
    {
        await SeedAsync();
        var expected = Docs.OrderBy(x => x.Number).AsQueryable().Select(projection).ToList();

        await using var session = theStore.QuerySession();
        var actual = await session.Query<CalcIndexedDoc>().OrderBy(x => x.Number).Select(projection).ToListAsync(Ct);

        actual.ShouldBe(expected);
    }

    // marten#5949 (and #5949 x #5942: an index in a top-level Select() is read, not refused as computed)
    [Fact]
    public Task select_value_array_index() => selectMatches(x => x.Values[1]);

    [Fact]
    public Task select_value_array_index_from_a_local()
    {
        var index = 2;
        return selectMatches(x => x.Values[index]);
    }

    [Fact]
    public Task select_list_indexer() => selectMatches(x => x.List[2]);

    [Fact]
    public Task select_element_at() => selectMatches(x => x.List.ElementAt(1));

    [Fact]
    public Task select_string_array_index() => selectMatches(x => x.Names[0]);

    [Fact]
    public Task select_string_list_indexer() => selectMatches(x => x.NameList[1]);

    [Fact]
    public Task select_bool_array_index() => selectMatches(x => x.Flags[1]);

    [Fact]
    public Task select_double_array_index() => selectMatches(x => x.Ratios[0]);

    [Fact]
    public Task select_jagged_array_index() => selectMatches(x => x.Grid[1][0]);

    [Fact]
    public Task select_jagged_array_index_from_locals()
    {
        var row = 1;
        var column = 1;
        return selectMatches(x => x.Grid[row][column]);
    }

    [Fact]
    public Task select_nested_list_indexer() => selectMatches(x => x.Nested[1][1]);

    [Fact]
    public Task select_jagged_string_array_index() => selectMatches(x => x.Words[1][0]);

    [Fact]
    public Task select_three_deep_index() => selectMatches(x => x.Cube[1][0][0]);

    [Fact]
    public Task select_member_of_indexed_child() => selectMatches(x => x.Children[1].Name);

    [Fact]
    public Task select_member_of_list_indexed_child() => selectMatches(x => x.ChildList[0].Score);

    [Fact]
    public Task select_member_of_jagged_indexed_child() => selectMatches(x => x.ChildGrid[1][0].Name);

    [Fact]
    public Task select_indexed_child() => selectMatches(x => x.Children[0]);

    // An int[] row compares by reference, so the row is checked as text
    [Fact]
    public async Task select_indexed_row_of_a_jagged_array()
    {
        await SeedAsync();
        var expected = Docs.OrderBy(x => x.Number).Select(x => string.Join(",", x.Grid[1])).ToList();

        await using var session = theStore.QuerySession();
        var actual = await session.Query<CalcIndexedDoc>().OrderBy(x => x.Number).Select(x => x.Grid[1])
            .ToListAsync(Ct);

        actual.Select(x => string.Join(",", x)).ToList().ShouldBe(expected);
    }

    [Fact]
    public Task select_indexes_into_an_anonymous_type() => selectMatches(x => new
    {
        x.Number,
        Value = x.Values[2],
        Listed = x.List[0],
        Cell = x.Grid[1][0],
        Nested = x.Nested[0][1],
        Word = x.Words[1][1],
        Deep = x.Cube[0][1][1],
        Child = x.Children[0].Name,
        Listed2 = x.ChildList[1].Score
    });

    [Fact]
    public Task select_indexes_into_a_record() =>
        selectMatches(x => new CalcIndexedProjection(x.Number, x.Grid[0][1], x.Words[0][0]));

    #endregion
}

internal static class CalcIndexedPredicates
{
    public static Dictionary<string, Expression<Func<CalcIndexedDoc, bool>>> All()
    {
        var one = 1;
        var zero = 0;

        return new()
        {
            ["value_array_literal"] = x => x.Values[1] == 7,
            ["value_array_local"] = x => x.Values[one] > 6,
            ["list_literal"] = x => x.List[1] == 4,
            ["list_local"] = x => x.List[zero] >= 8,
            ["element_at"] = x => x.Values.ElementAt(2) > 9,
            ["string_array"] = x => x.Names[1] == "m4",
            ["string_list"] = x => x.NameList[1] != "q2",
            ["double_array"] = x => x.Ratios[0] > 1.0,
            ["bool_array"] = x => x.Flags[1],
            ["not_bool_array"] = x => !x.Flags[0],
            ["bool_array_compared"] = x => x.Flags[0] == true && x.Number < 5,
            ["jagged_literal"] = x => x.Grid[1][0] == 4,
            ["jagged_locals"] = x => x.Grid[one][zero] > 6,
            ["jagged_not_equal"] = x => x.Grid[0][1] != 3,
            ["nested_list"] = x => x.Nested[1][0] == 0,
            ["jagged_string"] = x => x.Words[1][0] == "c1",
            ["three_deep"] = x => x.Cube[1][1][1] == 3,
            ["indexed_child_member"] = x => x.Children[1].Score > 6,
            ["list_indexed_child_member"] = x => x.ChildList[0].Name == "y3",
            ["jagged_indexed_child_member"] = x => x.ChildGrid[1][0].Score < 3,
            ["two_indexes_compared"] = x => x.Grid[0][0] < x.Values[1],
            ["jagged_and_member"] = x => x.Grid[1][1] >= 6 && x.Number != 2,
            ["jagged_or"] = x => x.Grid[0][0] == 1 || x.Words[0][1] == "b2",
            ["not_jagged"] = x => !(x.Grid[1][0] == 4),

            // A bool[] on the document could not be resolved at all in Marten before #5949. Marten's other case,
            // x.Flags.Any(f => f), needs Any(predicate) over a document collection, which Polecat does not translate.
            ["bool_collection_contains"] = x => x.Flags.Contains(true) && x.Number < 4
        };
    }
}
