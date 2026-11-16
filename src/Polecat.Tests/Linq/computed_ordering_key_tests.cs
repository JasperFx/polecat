using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1012 (marten#5957, 104f42c78; Bug_2213, 8bd8a2715): a computed OrderBy()/ThenBy() key -- negation,
// arithmetic, concatenation, ToUpper(), Length, ToString() of an integer -- translates exactly, and one with no
// exact translation is refused naming the OrderBy() key. Ported from Marten's cases.

public class OrderKeyComputedDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public long Count { get; set; }
    public double Amount { get; set; }
    public decimal Price { get; set; }
    public string Name { get; set; } = "";
    public string Code { get; set; } = "";
    public bool Flag { get; set; }
    public OrderKeyComputedInner Inner { get; set; } = new();
}

public class OrderKeyComputedInner
{
    public int Level { get; set; }
    public string Label { get; set; } = "";
}

public class OrderKeyCoalesceParent
{
    public Guid Id { get; set; }
    public OrderKeyCoalesceChild? First { get; set; }
    public OrderKeyCoalesceChild? Second { get; set; }
}

public class OrderKeyCoalesceChild
{
    public string Name { get; set; } = "";
}

// marten#5957: OrderBy(x => -x.Number) was silently ordered by x.Number in Marten (MemberFinder walked past
// the negation), and arithmetic keys were refused. Computed keys now translate, and a key with no exact
// translation is refused. Every ordering is compared, in order, with LINQ to Objects, ascending and
// descending, with x.Number breaking ties.
public class computed_ordering_key_tests : OneOffConfigurationsContext
{
    private static readonly OrderKeyComputedDoc[] Docs = Enumerable.Range(1, 9).Select(i => new OrderKeyComputedDoc
    {
        Id = Guid.NewGuid(),
        Number = i,
        Count = (i * 7) % 10,
        Amount = (i % 4) * 1.25 - i / 10.0,
        Price = i * 1.5m,
        Name = new[] { "pear", "fig", "banana", "kiwi", "apple", "plum", "cherry", "date", "lime" }[i - 1],
        Code = i % 2 == 0 ? "b" : "a",
        Flag = i % 3 == 0,
        Inner = new OrderKeyComputedInner { Level = (i * 5) % 7, Label = new[] { "x", "yy", "zzz" }[i % 3] }
    }).ToArray();

    private async Task SeedAsync()
    {
        // One at a time and out of Number order, so an ordering that goes missing shows up
        foreach (var doc in Docs.OrderBy(x => x.Name))
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }
    }

    private static Func<IQueryable<OrderKeyComputedDoc>, bool, IOrderedQueryable<OrderKeyComputedDoc>> key<TKey>(
        Expression<Func<OrderKeyComputedDoc, TKey>> key)
        => (q, descending) => descending ? q.OrderByDescending(key) : q.OrderBy(key);

    private static Dictionary<string, Func<IQueryable<OrderKeyComputedDoc>, bool, IOrderedQueryable<OrderKeyComputedDoc>>>
        Keys()
    {
        var divisor = 4;

        return new()
        {
            ["negate"] = key(x => -x.Number),
            ["negate_nested"] = key(x => -x.Inner.Level),
            ["negate_double"] = key(x => -x.Amount),
            ["modulo"] = key(x => x.Number % 3),
            ["modulo_by_local"] = key(x => x.Number % divisor),
            ["add_members"] = key(x => x.Number + x.Inner.Level),
            ["subtract_members"] = key(x => x.Inner.Level - x.Number),
            ["multiply_by_negative"] = key(x => x.Number * -2),
            ["integer_division"] = key(x => x.Number / 3),
            ["long_and_widened_int"] = key(x => x.Count - x.Number),
            ["double_arithmetic"] = key(x => x.Amount * -1.5),
            ["widening_cast_then_divide"] = key(x => (double)x.Number / 4),
            ["decimal_arithmetic"] = key(x => x.Price - x.Number * 2m),
            ["string_length"] = key(x => x.Name.Length),
            ["nested_string_length"] = key(x => x.Inner.Label.Length),
            ["string_concat"] = key(x => x.Code + x.Name),
            ["to_upper_member"] = key(x => x.Name.ToUpper()),
            ["to_upper_of_concat"] = key(x => (x.Inner.Label + x.Code).ToUpper()),
            ["int_to_string"] = key(x => x.Inner.Level.ToString())
        };
    }

    public static TheoryData<string, bool> KeyNames()
    {
        var data = new TheoryData<string, bool>();
        foreach (var name in Keys().Keys)
        {
            data.Add(name, false);
            data.Add(name, true);
        }

        return data;
    }

    // marten#5957: parity — the computed key translates and orders correctly
    [Theory]
    [MemberData(nameof(KeyNames))]
    public async Task computed_key_ordering_matches_linq_to_objects(string name, bool descending)
    {
        await SeedAsync();
        var order = Keys()[name];
        var expected = order(Docs.AsQueryable(), descending).ThenBy(x => x.Number).Select(x => x.Number).ToList();

        await using var session = theStore.QuerySession();
        var actual = await order(session.Query<OrderKeyComputedDoc>(), descending).ThenBy(x => x.Number)
            .Select(x => x.Number).ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected, $"{name} descending={descending}");
    }

    // marten#5957: the report — this sent `order by Number` in Marten
    [Fact]
    public async Task negation_is_not_ordered_by_the_member_underneath()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var actual = await session.Query<OrderKeyComputedDoc>().OrderBy(x => -x.Number).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(Docs.Select(x => x.Number).OrderByDescending(x => x).ToList());
    }

    // marten#5957
    [Fact]
    public async Task then_by_a_computed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Code).ThenByDescending(x => x.Number % 4).ThenBy(x => -x.Inner.Level)
            .ThenBy(x => x.Number).Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyComputedDoc>().OrderBy(x => x.Code)
            .ThenByDescending(x => x.Number % 4).ThenBy(x => -x.Inner.Level).ThenBy(x => x.Number)
            .Select(x => x.Number).ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5957
    [Fact]
    public async Task paging_after_a_computed_key_pages_the_ordered_rows()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number % 3).ThenByDescending(x => x.Number).Skip(2).Take(4)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyComputedDoc>().OrderBy(x => x.Number % 3)
            .ThenByDescending(x => x.Number).Skip(2).Take(4).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5957
    [Fact]
    public async Task first_after_a_computed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Inner.Level - x.Number).ThenBy(x => x.Number).First();
        var actual = await session.Query<OrderKeyComputedDoc>().OrderBy(x => x.Inner.Level - x.Number)
            .ThenBy(x => x.Number).FirstAsync(TestContext.Current.CancellationToken);

        actual.Number.ShouldBe(expected.Number);
    }

    // marten#5957
    [Fact]
    public async Task where_and_a_computed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.Where(x => x.Number > 2).OrderByDescending(x => x.Name.Length).ThenBy(x => x.Name)
            .Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyComputedDoc>().Where(x => x.Number > 2)
            .OrderByDescending(x => x.Name.Length).ThenBy(x => x.Name).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    // marten#5957 (with marten#5959)
    [Fact]
    public async Task a_second_order_by_with_a_computed_key()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = Docs.OrderBy(x => x.Number).OrderBy(x => x.Number % 3).Select(x => x.Number).ToList();
        var actual = await session.Query<OrderKeyComputedDoc>().OrderBy(x => x.Number).OrderBy(x => x.Number % 3)
            .Select(x => x.Number).ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(expected);
    }

    private static Dictionary<string, Func<IQueryable<OrderKeyComputedDoc>, IQueryable<OrderKeyComputedDoc>>> Refused()
    {
        return new()
        {
            ["decimal_division"] = q => q.OrderBy(x => x.Price / 3m),
            ["floating_modulo"] = q => q.OrderBy(x => x.Amount % 2.0),
            ["narrowing_cast"] = q => q.OrderBy(x => (byte)x.Number),
            ["conditional"] = q => q.OrderBy(x => x.Flag ? 1 : 0),
            ["math_call"] = q => q.OrderBy(x => Math.Abs(x.Number - 5)),
            ["substring"] = q => q.OrderBy(x => x.Number).ThenBy(x => x.Name.Substring(1)),
            ["not"] = q => q.OrderBy(x => !x.Flag),
            ["double_to_string"] = q => q.OrderBy(x => x.Amount.ToString())
        };
    }

    public static TheoryData<string> RefusedKeyNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Refused().Keys) data.Add(name);
        return data;
    }

    // marten#5957: a key without an exact translation is refused with a message that names the ordering
    [Theory]
    [MemberData(nameof(RefusedKeyNames))]
    public async Task a_key_without_an_exact_translation_is_refused(string name)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();
        var order = Refused()[name];

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            order(session.Query<OrderKeyComputedDoc>()).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("OrderBy");
    }

    // marten#5957 "narrowing_cast", over values where the narrowing changes the order: (byte)256 is 0, so ordering
    // by the member underneath would be silently wrong. It is refused.
    [Fact]
    public async Task a_narrowing_cast_key_is_refused()
    {
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<OrderKeyComputedDoc>()
            .OrderBy(x => (byte)x.Number).ThenBy(x => x.Number).Select(x => x.Number)
            .ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("cast");
    }

    // Bug_2213 (8bd8a2715): a coalesced key has no translation and is refused
    [Fact]
    public async Task a_coalesced_key_is_refused()
    {
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<OrderKeyCoalesceParent>()
            .OrderBy(x => (x.First ?? x.Second)!.Name).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("x.First ?? x.Second");
    }

    // Bug_2213 (8bd8a2715): Marten's refusal now names the OrderBy() key, not only the expression
    [Fact]
    public async Task a_coalesced_key_refusal_names_the_ordering()
    {
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<OrderKeyCoalesceParent>()
            .OrderBy(x => (x.First ?? x.Second)!.Name).ToListAsync(TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("OrderBy");
    }
}
