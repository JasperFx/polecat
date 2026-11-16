using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Linq;

public enum FlatColour
{
    Red,
    Green,
    Blue
}

public class FlatChild
{
    public int Parent { get; set; }
    public int Index { get; set; }
    public string Name { get; set; } = "";
    public FlatColour Colour { get; set; }
}

public class FlatParent
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Title { get; set; } = "";
    public List<FlatChild> Children { get; set; } = [];
    public int[] Values { get; set; } = [];
    public string[] Words { get; set; } = [];
    public FlatColour[] Colours { get; set; } = [];
    public List<FlatColour> ColourList { get; set; } = [];
    public Guid[] Refs { get; set; } = [];
}

// gh-1016 (marten#5928, #5921, #5933, #5940, #5941): SelectMany() over a collection the documents hold -- child
// documents, values, enums and strings -- and the operators around it. Every shape is compared with LINQ to
// Objects over the same documents. Before gh-1016 every one of them was refused.
public class select_many_over_document_collections_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly FlatParent[] Parents = Enumerable.Range(1, 6).Select(n => new FlatParent
    {
        Id = Guid.NewGuid(),
        Number = n,
        Title = new[] { "ab", "abcdef", "xyz123", "hello", "six   ", "seven77" }[n - 1],
        // Parent 4 has nothing to flatten
        Children = n == 4
            ? []
            : Enumerable.Range(0, n % 3 + 1).Select(i => new FlatChild
            {
                Parent = n, Index = i, Name = $"c{n}-{i}", Colour = (FlatColour)((n + i) % 3)
            }).ToList(),
        Values = n == 4 ? [] : Enumerable.Range(0, n % 3 + 1).Select(i => n * 10 + i).ToArray(),
        Words = n == 4 ? [] : Enumerable.Range(0, n % 3 + 1).Select(i => $"word{n}{i}" + (i == 2 ? "\"q\\" : "")).ToArray(),
        Colours = n == 4 ? [] : Enumerable.Range(0, n % 3 + 1).Select(i => (FlatColour)((n + i) % 3)).ToArray(),
        ColourList = n == 4 ? [] : [(FlatColour)(n % 3)],
        Refs = n == 4 ? [] : [Guid.NewGuid()]
    }).ToArray();

    private async Task SeedAsync(EnumStorage storage = EnumStorage.AsInteger)
    {
        ConfigureStore(opts => opts.ConfigureSerialization(enumStorage: storage));

        await using var session = theStore.LightweightSession();
        // Stored out of Number order, so nothing passes by insertion order alone
        foreach (var parent in Parents.OrderByDescending(x => x.Number % 3).ThenByDescending(x => x.Number))
        {
            session.Store(parent);
        }

        await session.SaveChangesAsync(Ct);
    }

    private IQueryable<FlatParent> Ordered(IQueryable<FlatParent> parents) => parents.OrderBy(x => x.Number);

    private async Task matches<T>(Func<IQueryable<FlatParent>, IQueryable<T>> query, bool anyOrder = false)
    {
        await SeedAsync();
        var expected = query(Parents.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();
        var actual = await query(session.Query<FlatParent>()).ToListAsync(Ct);

        if (anyOrder)
        {
            actual.OrderBy(x => x).ShouldBe(expected.OrderBy(x => x));
        }
        else
        {
            actual.ShouldBe(expected);
        }
    }

    public static TheoryData<string> PagingShapes =>
    [
        "take_then_children", "skip_then_children", "skip_take_then_children", "take_then_values",
        "skip_take_then_values", "take_select_many_take", "skip_take_select_many_skip_take", "select_many_then_take",
        "where_before_the_paging", "where_after_the_select_many", "order_by_after_the_select_many",
        "select_after_the_select_many", "unpaged_children", "unpaged_values", "order_by_descending_value"
    ];

    // marten#5928: Take/Skip before SelectMany page the PARENTS; after it, the flattened rows
    [Theory]
    [MemberData(nameof(PagingShapes))]
    public Task paging_around_select_many(string shape) => shape switch
    {
        "take_then_children" => matches(q => Ordered(q).Take(3).SelectMany(x => x.Children).Select(c => c.Name)),
        "skip_then_children" => matches(q => Ordered(q).Skip(2).SelectMany(x => x.Children).Select(c => c.Name)),
        "skip_take_then_children" => matches(q => Ordered(q).Skip(1).Take(3).SelectMany(x => x.Children).Select(c => c.Name)),
        "take_then_values" => matches(q => Ordered(q).Take(3).SelectMany(x => x.Values)),
        "skip_take_then_values" => matches(q => Ordered(q).Skip(1).Take(3).SelectMany(x => x.Values)),
        "take_select_many_take" => matches(q => Ordered(q).Take(3).SelectMany(x => x.Children).Take(2).Select(c => c.Name)),
        "skip_take_select_many_skip_take" => matches(q =>
            Ordered(q).Skip(1).Take(5).SelectMany(x => x.Children).Skip(1).Take(3).Select(c => c.Name)),
        "select_many_then_take" => matches(q => Ordered(q).SelectMany(x => x.Children).Take(4).Select(c => c.Name)),
        "where_before_the_paging" => matches(q =>
            q.Where(x => x.Number > 2).OrderBy(x => x.Number).Skip(1).Take(2).SelectMany(x => x.Children)
                .Select(c => c.Name)),
        "where_after_the_select_many" => matches(q =>
            Ordered(q).Skip(1).Take(5).SelectMany(x => x.Children).Where(c => c.Index > 0).Select(c => c.Name)),
        "order_by_after_the_select_many" => matches(q =>
            Ordered(q).Take(5).SelectMany(x => x.Children).OrderByDescending(c => c.Index).ThenBy(c => c.Parent)
                .Select(c => c.Name)),
        "select_after_the_select_many" => matches(q =>
            Ordered(q).Skip(2).Take(4).SelectMany(x => x.Children).Select(c => c.Index)),
        "unpaged_children" => matches(q => Ordered(q).SelectMany(x => x.Children).Select(c => c.Name)),
        "unpaged_values" => matches(q => Ordered(q).SelectMany(x => x.Values)),
        "order_by_descending_value" => matches(q => q.SelectMany(x => x.Values).OrderByDescending(v => v)),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    [Fact]
    public async Task child_documents_come_back_whole()
    {
        await SeedAsync();
        var expected = Parents.OrderBy(x => x.Number).SelectMany(x => x.Children)
            .Select(c => (c.Parent, c.Index, c.Name, c.Colour)).ToList();

        await using var session = theStore.QuerySession();
        var actual = await Ordered(session.Query<FlatParent>()).SelectMany(x => x.Children).ToListAsync(Ct);

        actual.Select(c => (c.Parent, c.Index, c.Name, c.Colour)).ToList().ShouldBe(expected);
    }

    [Fact]
    public Task distinct_values() => matches(q => q.SelectMany(x => x.Children).Select(c => c.Index).Distinct(), anyOrder: true);

    [Fact]
    public Task strings_with_characters_json_escapes() => matches(q => Ordered(q).SelectMany(x => x.Words));

    [Fact]
    public Task guids() => matches(q => Ordered(q).SelectMany(x => x.Refs));

    // marten#5928: the terminals over the flattened rows, paged parents or not
    [Fact]
    public async Task counts_and_any()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var parents = session.Query<FlatParent>();
        IQueryable<FlatParent> local = Parents.AsQueryable();

        (await Ordered(parents).Take(3).SelectMany(x => x.Children).CountAsync(Ct))
            .ShouldBe(Ordered(local).Take(3).SelectMany(x => x.Children).Count());
        (await Ordered(parents).Skip(1).Take(3).SelectMany(x => x.Values).LongCountAsync(Ct))
            .ShouldBe(Ordered(local).Skip(1).Take(3).SelectMany(x => x.Values).LongCount());
        (await Ordered(parents).Skip(3).Take(1).SelectMany(x => x.Children).AnyAsync(Ct))
            .ShouldBe(Ordered(local).Skip(3).Take(1).SelectMany(x => x.Children).Any());
        (await parents.Where(x => x.Number == 4).SelectMany(x => x.Children).AnyAsync(Ct)).ShouldBeFalse();
        (await parents.SelectMany(x => x.Values).AnyAsync(Ct)).ShouldBeTrue();
        (await parents.SelectMany(x => x.Values).CountAsync(Ct)).ShouldBe(local.SelectMany(x => x.Values).Count());
        (await parents.SelectMany(x => x.Values).SumAsync(v => v, Ct)).ShouldBe(local.SelectMany(x => x.Values).Sum());
        (await parents.SelectMany(x => x.Children).MaxAsync(c => c.Index, Ct))
            .ShouldBe(local.SelectMany(x => x.Children).Max(c => c.Index));
    }

    // marten#5921: First/Single after SelectMany
    [Fact]
    public async Task first_and_single()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var flattened = Ordered(session.Query<FlatParent>()).SelectMany(x => x.Children);
        var local = Ordered(Parents.AsQueryable()).SelectMany(x => x.Children);

        (await flattened.FirstAsync(Ct)).Name.ShouldBe(local.First().Name);
        (await flattened.FirstAsync(c => c.Index == 2, Ct)).Name.ShouldBe(local.First(c => c.Index == 2).Name);
        (await flattened.FirstOrDefaultAsync(c => c.Index == 100, Ct)).ShouldBeNull();
        (await flattened.Where(c => c.Name == "c3-0").SingleAsync(Ct)).Name.ShouldBe("c3-0");
        (await flattened.OrderBy(c => c.Index).ThenByDescending(c => c.Parent).FirstAsync(Ct)).Name
            .ShouldBe(local.OrderBy(c => c.Index).ThenByDescending(c => c.Parent).First().Name);
        (await Ordered(session.Query<FlatParent>()).SelectMany(x => x.Values).FirstAsync(Ct))
            .ShouldBe(Ordered(Parents.AsQueryable()).SelectMany(x => x.Values).First());

        await Should.ThrowAsync<InvalidOperationException>(() => flattened.SingleAsync(Ct));
        await Should.ThrowAsync<InvalidOperationException>(() =>
            session.Query<FlatParent>().Where(x => x.Number == 4).SelectMany(x => x.Children).FirstAsync(Ct));
    }

    public static TheoryData<string> SelectShapes =>
    [
        "int_modulo", "int_to_string", "string_to_upper", "string_length", "value_anonymous_type",
        "child_int_member", "child_arithmetic", "child_string_method", "child_anonymous_type", "result_selector",
        "result_selector_then_where"
    ];

    // marten#5933: Select() after SelectMany() projects the flattened rows
    [Theory]
    [MemberData(nameof(SelectShapes))]
    public Task select_after_select_many(string shape) => shape switch
    {
        "int_modulo" => matches(q => Ordered(q).SelectMany(x => x.Values).Select(v => v % 3)),
        "int_to_string" => matches(q => Ordered(q).SelectMany(x => x.Values).Select(v => v.ToString())),
        "string_to_upper" => matches(q => Ordered(q).SelectMany(x => x.Words).Select(s => s.ToUpper())),
        "string_length" => matches(q => Ordered(q).SelectMany(x => x.Words).Select(s => s.Length)),
        "value_anonymous_type" => matches(q => Ordered(q).SelectMany(x => x.Values).Select(v => new { V = v, M = v % 3 })),
        "child_int_member" => matches(q => Ordered(q).SelectMany(x => x.Children).Select(c => c.Index)),
        "child_arithmetic" => matches(q => Ordered(q).SelectMany(x => x.Children).Select(c => c.Index * 10 + c.Parent)),
        "child_string_method" => matches(q => Ordered(q).SelectMany(x => x.Children).Select(c => c.Name.ToUpper())),
        "child_anonymous_type" => matches(q => Ordered(q).SelectMany(x => x.Children).Select(c => new { c.Index, c.Name })),
        "result_selector" => matches(q =>
            Ordered(q).SelectMany(x => x.Children, (x, c) => new { x.Number, x.Title, c.Name })),
        "result_selector_then_where" => matches(q =>
            Ordered(q).SelectMany(x => x.Children, (x, c) => new { x.Number, c.Index, c.Name })
                .Where(r => r.Number > 2 && r.Index == 0).Select(r => r.Name)),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    // marten#5940: an enum collection, under both enum storages
    [Theory]
    [InlineData(EnumStorage.AsInteger)]
    [InlineData(EnumStorage.AsString)]
    public async Task enum_elements(EnumStorage storage)
    {
        await SeedAsync(storage);
        await using var session = theStore.QuerySession();
        var parents = session.Query<FlatParent>();
        IQueryable<FlatParent> local = Parents.AsQueryable();

        (await Ordered(parents).SelectMany(x => x.Colours).ToListAsync(Ct))
            .ShouldBe(Ordered(local).SelectMany(x => x.Colours).ToList());
        (await Ordered(parents).SelectMany(x => x.ColourList).ToListAsync(Ct))
            .ShouldBe(Ordered(local).SelectMany(x => x.ColourList).ToList());
        (await Ordered(parents).SelectMany(x => x.Colours).Where(c => c != FlatColour.Red).ToListAsync(Ct))
            .ShouldBe(Ordered(local).SelectMany(x => x.Colours).Where(c => c != FlatColour.Red).ToList());
        (await parents.SelectMany(x => x.Colours).Distinct().ToListAsync(Ct)).OrderBy(x => x)
            .ShouldBe(local.SelectMany(x => x.Colours).Distinct().OrderBy(x => x));
    }

    // marten#5941: string members of a flattened string
    [Fact]
    public async Task string_elements()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var words = Ordered(session.Query<FlatParent>()).SelectMany(x => x.Words);
        var local = Ordered(Parents.AsQueryable()).SelectMany(x => x.Words);

        (await words.Where(w => w.Length == 6).ToListAsync(Ct)).ShouldBe(local.Where(w => w.Length == 6).ToList());
        (await words.Where(w => w.ToUpper() == "WORD10").ToListAsync(Ct)).ShouldBe(local.Where(w => w.ToUpper() == "WORD10").ToList());
        (await words.Where(w => w.StartsWith("word1")).ToListAsync(Ct)).ShouldBe(local.Where(w => w.StartsWith("word1")).ToList());
        (await words.OrderBy(w => w.Length).ThenByDescending(w => w).ToListAsync(Ct))
            .ShouldBe(local.OrderBy(w => w.Length).ThenByDescending(w => w, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task the_preview_renders_the_flattened_statement()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var sql = session.ToSql(Ordered(session.Query<FlatParent>()).Take(3).SelectMany(x => x.Children));

        sql.ShouldContain("CROSS APPLY OPENJSON(__pc_page.data, '$.children') AS __pc_elem");
    }

    [Fact]
    public async Task shapes_without_a_translation_are_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var parents = session.Query<FlatParent>();

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            parents.SelectMany(x => x.Children).SelectMany(c => c.Name).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            parents.Select(x => x.Children).SelectMany(c => c).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            parents.SelectMany(x => x.Children).Take(2).OrderBy(c => c.Index).ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            parents.SelectMany(x => x.Children).Distinct().ToListAsync(Ct));
    }
}
