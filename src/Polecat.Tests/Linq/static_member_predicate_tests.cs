using System.Linq.Expressions;
using System.Text.Json.Serialization;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1013 / gh-1014 (marten#5943): statics, constants and static array indexes in a predicate are values worked
// out before the query runs, beside indexes into the document's own arrays. Ported from Marten's cases.

public class CalcStaticProbeDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
    public int[] Values { get; set; } = [];
    public Dictionary<string, int> Scores { get; set; } = new();
}

public class CalcStaticProbeConfig
{
    public int Limit { get; set; }
    public int[] Values { get; set; } = [];
    public CalcStaticProbeConfig? Inner { get; set; }
}

// Everything the predicates below read that is not a document member: each one is a constant to the query.
public static class CalcStaticProbe
{
    public const int Constant = 3;
    public const string ConstantName = "three";

    public static int Field = 3;
    public static readonly int ReadonlyField = 3;
    public static int Property { get; set; } = 3;
    public static string Name = "three";

    public static int[] Array = [1, 2, 3, 4];
    public static int[][] Jagged = [[1, 2], [3, 4]];
    public static int[,] Matrix = { { 1, 2 }, { 3, 4 } };
    public static string[] Names = ["one", "two", "three"];
    public static List<int> List = [1, 2, 3, 4];
    public static IList<int> InterfaceList = new List<int> { 1, 2, 3, 4 };
    public static Dictionary<string, int> Dictionary = new() { ["three"] = 3, ["one"] = 1 };
    public static IReadOnlyDictionary<int, string> NamesByNumber = new Dictionary<int, string> { [3] = "three" };

    public static CalcStaticProbeConfig Config = new()
    {
        Limit = 3, Values = [1, 2, 3], Inner = new CalcStaticProbeConfig { Limit = 2, Values = [5, 2] }
    };

    public static CalcStaticProbeConfig ConfigProperty { get; } = new() { Limit = 3, Values = [3, 1] };
    public static List<CalcStaticProbeConfig> Configs = [new() { Limit = 1 }, new() { Limit = 3 }];

    public static int One = 1;
    public static string Key = "b";
    public static string[] Keys = ["a", "b"];
}

// marten#5943: `StaticProbe.Jagged[1][0]` inside a predicate failed to parse in Marten; dictionary indexers on
// statics silently matched nothing; Select(x => captured) silently returned defaults. Polecat evaluates any
// value side with no lambda parameter by compiling it (WhereClauseParser.ExtractValue / CompileAndInvoke), so
// the static shapes are expected to work; shapes that also need an index into the document or Any() over a
// child collection are features Polecat may not have, and are allowed a clear refusal.
public class static_member_predicate_tests : OneOffConfigurationsContext
{
    internal static readonly CalcStaticProbeDoc[] Docs = Enumerable.Range(1, 6).Select(i => new CalcStaticProbeDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Name = new[] { "one", "two", "three", "four", "five", "six" }[i - 1],
        Values = [i, i * 2, 7 - i],
        Scores = new Dictionary<string, int> { ["a"] = i, ["b"] = 7 - i }
    }).ToArray();

    private static CancellationToken Ct => CalcQueries.Ct;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in Docs) session.Store(doc);
        await session.SaveChangesAsync(Ct);
    }

    public static TheoryData<string> PredicateNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in new CalcStaticProbePredicates().All().Keys.Except(NeedsAnyWithAPredicate)) data.Add(name);
        return data;
    }

    // Any(predicate) over a collection the document holds has no translation in Polecat at all, static or not.
    // That is its own feature, outside the scope of gh-1013/gh-1014; these stay a clear refusal until it lands.
    private static readonly string[] NeedsAnyWithAPredicate =
    [
        "closed_any_predicate_true", "closed_any_predicate_false", "closed_any_predicate_indexed",
        "closed_contains_and_element", "closed_comparison_and_element", "static_compared_to_element"
    ];

    public static TheoryData<string> AnyPredicateNames() => new(NeedsAnyWithAPredicate);

    [Theory]
    [MemberData(nameof(AnyPredicateNames))]
    public async Task any_with_a_predicate_over_a_document_collection_is_refused(string name)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<CalcStaticProbeDoc>()
            .Where(new CalcStaticProbePredicates().All()[name]).ToListAsync(Ct));
        ex.Message.ShouldContain("Any");
    }

    // marten#5943
    [Theory]
    [MemberData(nameof(PredicateNames))]
    public async Task static_predicate_matches_linq_to_objects(string name)
    {
        await SeedAsync();

        var predicate = new CalcStaticProbePredicates().All()[name];
        var expected = Docs.AsQueryable().Where(predicate).Select(x => x.Number).OrderBy(x => x).ToList();

        // Every predicate selects something and leaves something out
        expected.ShouldNotBeEmpty(name);
        expected.Count.ShouldBeLessThan(Docs.Length, name);

        await using var session = theStore.QuerySession();

        async Task<IReadOnlyList<int>> run() =>
            (await session.Query<CalcStaticProbeDoc>().Where(predicate).Select(x => x.Number).ToListAsync(Ct))
            .OrderBy(x => x).ToList();

        (await run()).ShouldBe(expected, name);
    }

    // marten#5943
    [Fact]
    public async Task the_static_value_is_read_on_every_execution()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var original = CalcStaticProbe.Jagged[1][0];

        try
        {
            (await session.Query<CalcStaticProbeDoc>().Where(x => x.Number == CalcStaticProbe.Jagged[1][0])
                .Select(x => x.Number).ToListAsync(Ct)).ShouldBe([3]);

            CalcStaticProbe.Jagged[1][0] = 5;

            (await session.Query<CalcStaticProbeDoc>().Where(x => x.Number == CalcStaticProbe.Jagged[1][0])
                .Select(x => x.Number).ToListAsync(Ct)).ShouldBe([5]);
        }
        finally
        {
            CalcStaticProbe.Jagged[1][0] = original;
        }
    }

    // gh-1014: a dictionary entry read as a JSON path in a Select
    [Fact]
    public async Task select_a_document_dictionary_entry()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var key = "a";

        var actual = await session.Query<CalcStaticProbeDoc>().OrderBy(x => x.Number).Select(x => x.Scores[key])
            .ToListAsync(Ct);

        actual.ShouldBe(Docs.OrderBy(x => x.Number).Select(x => x.Scores[key]).ToList());
    }

    // marten#5943: ordering by a document dictionary entry (gh-1014 reads it as a JSON path)
    [Fact]
    public async Task order_by_document_dictionary_by_static_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Scores[CalcStaticProbe.Key]).Select(x => x.Number).ToList();
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcStaticProbeDoc>().OrderBy(x => x.Scores[CalcStaticProbe.Key])
                .Select(x => x.Number).ToListAsync(Ct),
            actual => actual.ShouldBe(expected));
    }

    // marten#5943
    [Fact]
    public async Task order_by_document_dictionary_by_static_array_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderByDescending(x => x.Scores[CalcStaticProbe.Keys[1]]).Select(x => x.Number).ToList();
        await CalcQueries.ShouldMatchAsync(
            () => session.Query<CalcStaticProbeDoc>().OrderByDescending(x => x.Scores[CalcStaticProbe.Keys[1]])
                .Select(x => x.Number).ToListAsync(Ct),
            actual => actual.ShouldBe(expected));
    }

    // marten#5943
    [Fact]
    public async Task order_by_with_a_static_filter()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.Where(x => x.Number != CalcStaticProbe.Jagged[1][0]).OrderByDescending(x => x.Number)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<CalcStaticProbeDoc>().Where(x => x.Number != CalcStaticProbe.Jagged[1][0])
            .OrderByDescending(x => x.Number).Select(x => x.Number).ToListAsync(Ct);

        actual.ShouldBe(expected);
    }

    public record CalcStaticProjection(int Number, int Limit);

    // marten#5943
    [Fact]
    public async Task select_a_static_jagged_value_into_an_anonymous_type()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).Select(x => new { x.Number, Limit = CalcStaticProbe.Jagged[1][0] })
            .ToList();
        var actual = await session.Query<CalcStaticProbeDoc>().OrderBy(x => x.Number)
            .Select(x => new { x.Number, Limit = CalcStaticProbe.Jagged[1][0] }).ToListAsync(Ct);

        actual.ShouldBe(expected);
    }

    // marten#5943
    [Fact]
    public async Task select_static_values_into_a_record()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number)
            .Select(x => new CalcStaticProjection(x.Number, CalcStaticProbe.Config.Values[2] + CalcStaticProbe.List[0]))
            .ToList();
        var actual = await session.Query<CalcStaticProbeDoc>().OrderBy(x => x.Number)
            .Select(x => new CalcStaticProjection(x.Number, CalcStaticProbe.Config.Values[2] + CalcStaticProbe.List[0]))
            .ToListAsync(Ct);

        actual.ShouldBe(expected);
    }

    // marten#5943
    [Fact]
    public async Task select_members_of_static_members()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).Select(x => new
        {
            x.Number,
            Limit = CalcStaticProbe.Config.Limit,
            Inner = CalcStaticProbe.Config.Inner!.Values[0],
            Listed = CalcStaticProbe.List[3],
            Keyed = CalcStaticProbe.Dictionary["one"],
            Named = CalcStaticProbe.Names[1],
            Field = CalcStaticProbe.Field,
            Constant = CalcStaticProbe.Constant
        }).ToList();

        var actual = await session.Query<CalcStaticProbeDoc>().OrderBy(x => x.Number).Select(x => new
        {
            x.Number,
            Limit = CalcStaticProbe.Config.Limit,
            Inner = CalcStaticProbe.Config.Inner!.Values[0],
            Listed = CalcStaticProbe.List[3],
            Keyed = CalcStaticProbe.Dictionary["one"],
            Named = CalcStaticProbe.Names[1],
            Field = CalcStaticProbe.Field,
            Constant = CalcStaticProbe.Constant
        }).ToListAsync(Ct);

        actual.ShouldBe(expected);
    }

    // marten#5943
    [Fact]
    public async Task select_a_bare_static_jagged_value()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var actual = await session.Query<CalcStaticProbeDoc>().Where(x => x.Number < 3)
            .Select(x => CalcStaticProbe.Jagged[1][1]).ToListAsync(Ct);

        actual.ShouldBe([4, 4]);
    }

    // marten#5943
    [Fact]
    public async Task select_a_bare_static_field()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var actual = await session.Query<CalcStaticProbeDoc>().Where(x => x.Number < 3)
            .Select(x => CalcStaticProbe.Field).ToListAsync(Ct);

        actual.ShouldBe([3, 3]);
    }

    // marten#5943
    [Fact]
    public async Task select_a_bare_captured_local()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var local = 3;

        var actual = await session.Query<CalcStaticProbeDoc>().Where(x => x.Number < 3).Select(x => local)
            .ToListAsync(Ct);

        actual.ShouldBe([3, 3]);
    }

    // marten#5943: Any() over a child collection is a feature, so a clear refusal is allowed
    [Fact]
    public async Task static_inside_any_over_a_value_collection()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.Where(x => x.Values.Any(v => v == CalcStaticProbe.Jagged[1][1])).Select(x => x.Number)
            .OrderBy(x => x).ToList();
        expected.ShouldNotBeEmpty();

        // Any(predicate) over a document collection is not translated (see above), so this is refused
        await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<CalcStaticProbeDoc>()
            .Where(x => x.Values.Any(v => v == CalcStaticProbe.Jagged[1][1])).Select(x => x.Number).ToListAsync(Ct));
    }

    // marten#5943
    [Fact]
    public async Task static_inside_contains_over_a_value_collection()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.Where(x => x.Values.Contains(CalcStaticProbe.Jagged[1][1])).Select(x => x.Number)
            .OrderBy(x => x).ToList();
        expected.ShouldNotBeEmpty();

        var actual = await session.Query<CalcStaticProbeDoc>()
            .Where(x => x.Values.Contains(CalcStaticProbe.Jagged[1][1]))
            .Select(x => x.Number).ToListAsync(Ct);

        actual.OrderBy(x => x).ToList().ShouldBe(expected);
    }

    // marten#5943's compiled-query facts are N/A: Polecat has no compiled queries.
}

// The predicates live on their own class so MemberData can list them, and so the instance-field controls read
// fields of a real instance: Member(Constant(this), field)
internal class CalcStaticProbePredicates
{
    private readonly int[][] _jagged = [[1, 2], [3, 4]];
    private readonly List<int> _list = [1, 2, 3, 4];
    private readonly Dictionary<string, int> _dictionary = new() { ["three"] = 3 };
    private readonly CalcStaticProbeConfig _config = new() { Limit = 3, Values = [1, 2, 3] };
    private readonly int _one = 1;

    public Dictionary<string, Expression<Func<CalcStaticProbeDoc, bool>>> All()
    {
        // Captured locals, as controls
        var localJagged = new[] { new[] { 1, 2 }, new[] { 3, 4 } };
        var localList = new List<int> { 1, 2, 3, 4 };
        var localDictionary = new Dictionary<string, int> { ["three"] = 3 };
        var localConfig = new CalcStaticProbeConfig { Limit = 3, Values = [1, 2, 3] };
        var localValue = CalcStaticProbe.Jagged[1][0];
        var localOne = 1;

        return new()
        {
            // the issue
            ["static_jagged_not_equal"] = x => x.Number != CalcStaticProbe.Jagged[1][0],
            ["static_jagged_equal"] = x => x.Number == CalcStaticProbe.Jagged[1][0],
            ["static_jagged_on_left"] = x => CalcStaticProbe.Jagged[1][0] < x.Number,
            ["static_jagged_greater"] = x => x.Number > CalcStaticProbe.Jagged[0][1],

            // plain static members
            ["static_field"] = x => x.Number == CalcStaticProbe.Field,
            ["static_readonly_field"] = x => x.Number == CalcStaticProbe.ReadonlyField,
            ["static_property"] = x => x.Number >= CalcStaticProbe.Property,
            ["constant"] = x => x.Number == CalcStaticProbe.Constant,
            ["constant_string"] = x => x.Name == CalcStaticProbe.ConstantName,
            ["static_string"] = x => x.Name != CalcStaticProbe.Name,

            // indexing into a static
            ["static_array"] = x => x.Number == CalcStaticProbe.Array[2],
            ["static_array_length"] = x => x.Number == CalcStaticProbe.Array.Length,
            ["static_string_array"] = x => x.Name == CalcStaticProbe.Names[2],
            ["static_matrix"] = x => x.Number == CalcStaticProbe.Matrix[1, 0],
            ["static_list"] = x => x.Number == CalcStaticProbe.List[2],
            ["static_list_not_equal"] = x => x.Number != CalcStaticProbe.List[2],
            ["static_interface_list"] = x => x.Number < CalcStaticProbe.InterfaceList[2],
            ["static_list_count"] = x => x.Number == CalcStaticProbe.List.Count,
            ["static_dictionary"] = x => x.Number == CalcStaticProbe.Dictionary["three"],
            ["static_dictionary_by_constant_key"] = x => x.Number == CalcStaticProbe.Dictionary[CalcStaticProbe.ConstantName],
            ["static_readonly_dictionary"] = x => x.Name == CalcStaticProbe.NamesByNumber[3],
            ["static_list_element_member"] = x => x.Number == CalcStaticProbe.Configs[1].Limit,

            // members of static members
            ["static_member_of_static"] = x => x.Number == CalcStaticProbe.Config.Limit,
            ["static_member_of_static_property"] = x => x.Number == CalcStaticProbe.ConfigProperty.Limit,
            ["static_nested_member"] = x => x.Number == CalcStaticProbe.Config.Inner!.Limit,
            ["static_member_array"] = x => x.Number == CalcStaticProbe.Config.Values[2],
            ["static_nested_member_array"] = x => x.Number == CalcStaticProbe.Config.Inner!.Values[1],
            ["static_member_of_static_property_array"] = x => x.Number == CalcStaticProbe.ConfigProperty.Values[0],

            // a static in an index or arithmetic
            ["static_index_into_static"] = x => x.Number == CalcStaticProbe.Array[CalcStaticProbe.One],
            ["static_jagged_by_static_index"] = x => x.Number == CalcStaticProbe.Jagged[CalcStaticProbe.One][0],
            ["static_arithmetic"] = x => x.Number == CalcStaticProbe.Jagged[1][0] + 1,

            // a static locates a member of the document
            ["document_array_by_static_index"] = x => x.Values[CalcStaticProbe.One] == 4,
            ["document_array_by_jagged_index"] = x => x.Values[CalcStaticProbe.Jagged[0][0]] == 4,
            ["document_dictionary_by_static_key"] = x => x.Scores[CalcStaticProbe.Key] == 4,
            ["document_array_against_static"] = x => x.Values[0] == CalcStaticProbe.Jagged[1][0],

            // a static collection searched for a document member
            ["static_jagged_row_contains"] = x => CalcStaticProbe.Jagged[1].Contains(x.Number),
            ["static_list_element_contains"] = x => CalcStaticProbe.Configs[1].Values.Contains(x.Number) || x.Number == 6,
            ["static_closed_contains_and_member"] = x => CalcStaticProbe.List.Contains(3) && x.Number > 4,
            ["static_closed_contains_or_member"] = x => CalcStaticProbe.List.Contains(99) || x.Number == 2,
            ["not_static_closed_contains"] = x => !CalcStaticProbe.Names.Contains("zero") && x.Number < 3,

            // a static inside a predicate over a child collection
            ["closed_any_predicate_true"] = x => x.Values.Any(v => CalcStaticProbe.List.Contains(3)) && x.Number < 3,
            ["closed_any_predicate_false"] = x => x.Values.Any(v => CalcStaticProbe.List.Contains(99)) || x.Number == 1,
            ["closed_any_predicate_indexed"] = x => x.Values.Any(v => CalcStaticProbe.Jagged[1][0] == 4) || x.Number == 1,
            ["closed_contains_and_element"] = x => x.Values.Any(v => CalcStaticProbe.List.Contains(3) && v > 10),
            ["closed_comparison_and_element"] = x => x.Values.Any(v => CalcStaticProbe.Jagged[1][0] == 3 && v > 10),
            ["static_compared_to_element"] = x => x.Values.Any(v => v == CalcStaticProbe.Jagged[1][1]),

            // combined
            ["static_and_static"] = x => x.Number >= CalcStaticProbe.Jagged[0][1] && x.Number <= CalcStaticProbe.List[3],
            ["static_or_static"] = x => x.Number == CalcStaticProbe.Jagged[0][0] || x.Name == CalcStaticProbe.Names[2],
            ["not_static"] = x => !(x.Number == CalcStaticProbe.Jagged[1][0]),

            // controls: captured locals
            ["local_value"] = x => x.Number != localValue,
            ["local_jagged"] = x => x.Number != localJagged[1][0],
            ["local_list"] = x => x.Number == localList[2],
            ["local_dictionary"] = x => x.Number == localDictionary["three"],
            ["local_member"] = x => x.Number == localConfig.Limit,
            ["local_member_array"] = x => x.Number == localConfig.Values[2],
            ["document_array_by_local_index"] = x => x.Values[localOne] == 4,

            // controls: instance fields
            ["instance_jagged"] = x => x.Number != _jagged[1][0],
            ["instance_list"] = x => x.Number == _list[2],
            ["instance_dictionary"] = x => x.Number == _dictionary["three"],
            ["instance_member"] = x => x.Number == _config.Limit,
            ["instance_member_array"] = x => x.Number == _config.Values[2],
            ["document_array_by_instance_index"] = x => x.Values[_one] == 4,
        };
    }
}
