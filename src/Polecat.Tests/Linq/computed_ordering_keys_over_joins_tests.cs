using System.Linq.Expressions;
using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

// gh-1012 (marten#5968, 3ad522899): computed ordering keys over either side of a join, and over the child
// documents a SelectMany() flattens (gh-1016). Ported from Marten's cases.

public class OrderKeyKeyParent
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public List<OrderKeyKeyChild> Children { get; set; } = new();
}

public record OrderKeyKeyChild
{
    public int Index { get; set; }
    public string Name { get; set; } = "";
}

public class OrderKeyKeyCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Rank { get; set; }
}

public class OrderKeyKeyOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public int Amount { get; set; }
    public string Status { get; set; } = "";
}

// marten#5968: a computed key over either side of a join was silently ordered by the member underneath it.
// Polecat has no Join(); its join is GroupJoin(...).SelectMany(...), so Marten's Join() shapes are ported to
// that form. The join path in PolecatLinqQueryProvider resolves an ordering only when its body is a member
// and skips anything else without a word, so a computed key there is expected to come back UNORDERED.
public class computed_ordering_keys_over_joins_tests : OneOffConfigurationsContext
{
    private static readonly string[] Pool = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf"];

    private static readonly OrderKeyKeyCustomer[] Customers = Enumerable.Range(1, 4)
        .Select(i => new OrderKeyKeyCustomer { Id = Guid.NewGuid(), Name = Pool[i] + i, Rank = i * 3 % 5 }).ToArray();

    private static readonly OrderKeyKeyOrder[] Orders = Customers.SelectMany((c, i) => Enumerable.Range(0, 3)
        .Select(k => new OrderKeyKeyOrder
        {
            Id = Guid.NewGuid(), CustomerId = c.Id, Amount = (i + 1) * 10 + k, Status = Pool[(i + k) % Pool.Length]
        })).ToArray();

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Customers);
        session.Store(Orders);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    private async Task joined<T>(string shape,
        Func<IQueryable<OrderKeyKeyCustomer>, IQueryable<OrderKeyKeyOrder>, IQueryable<T>> query)
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;
        var expected = query(Customers.AsQueryable(), Orders.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();

        (await query(session.Query<OrderKeyKeyCustomer>(), session.Query<OrderKeyKeyOrder>()).ToListAsync(token))
            .ShouldBe(expected, shape);
        (await query(session.Query<OrderKeyKeyCustomer>(), session.Query<OrderKeyKeyOrder>()).Skip(2).Take(4)
            .ToListAsync(token)).ShouldBe(expected.Skip(2).Take(4).ToList(), shape + " Skip(2).Take(4)");
    }

    public static TheoryData<string> JoinShapes =>
    [
        "inner_negation", "inner_negation_descending", "inner_modulo_then_member", "outer_negation_then_inner",
        "concatenation_then_member", "outer_arithmetic_then_inner", "inner_member", "outer_member_then_inner"
    ];

    // marten#5968, Join() shapes written as GroupJoin(...).SelectMany(...)
    [Theory]
    [MemberData(nameof(JoinShapes))]
    public Task over_joined_rows(string shape) => shape switch
    {
        "inner_negation" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount }).OrderBy(r => -r.Amount)),
        "inner_negation_descending" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount }).OrderByDescending(r => -r.Amount)),
        "inner_modulo_then_member" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount })
                .OrderBy(r => r.Amount % 3).ThenBy(r => r.Amount)),
        "outer_negation_then_inner" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
                .OrderBy(r => -r.Rank).ThenBy(r => r.Amount)),
        "concatenation_then_member" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Status, y.Amount })
                .OrderBy(r => r.Status + "z").ThenByDescending(r => r.Amount)),
        "outer_arithmetic_then_inner" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
                .OrderBy(r => r.Rank * -3 + 1).ThenBy(r => r.Amount)),
        "inner_member" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount }).OrderByDescending(r => r.Amount)),
        "outer_member_then_inner" => joined(shape, (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount })
                .OrderBy(r => r.Name).ThenBy(r => r.Amount)),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    // marten#5959 over a join: a second OrderBy() keeps the earlier one as a tie-break. Polecat's
    // HandleGroupJoinOrderBy clears the earlier keys on a second OrderBy(), so ties within a customer are lost.
    [Fact]
    public async Task a_second_order_by_over_a_join_keeps_the_earlier_as_a_tie_break()
    {
        await joined("second_order_by", (c, o) =>
            c.GroupJoin(o, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
                .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
                .OrderByDescending(r => r.Amount).OrderBy(r => r.Rank));
    }

    public static TheoryData<string> RefusedShapes => ["join_math_call", "join_narrowing", "join_conditional"];

    // marten#5968: a join key without an exact translation is refused, not silently dropped or narrowed away
    [Theory]
    [MemberData(nameof(RefusedShapes))]
    public async Task untranslatable_join_keys_are_refused(string shape)
    {
        await SeedAsync();
        var token = TestContext.Current.CancellationToken;
        await using var session = theStore.QuerySession();
        var customers = session.Query<OrderKeyKeyCustomer>();
        var orders = session.Query<OrderKeyKeyOrder>();

        var rows = customers.GroupJoin(orders, x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Name, y.Amount });

        Func<Task> query = shape switch
        {
            "join_math_call" => () => rows.OrderBy(r => Math.Abs(r.Amount - 25)).ToListAsync(token),
            "join_narrowing" => () => rows.OrderBy(r => (short)r.Amount).ToListAsync(token),
            // Not one of Marten's refused join shapes, but its child-document "conditional" case moved over
            "join_conditional" => () => rows.OrderBy(r => r.Amount > 25 ? 0 : 1).ToListAsync(token),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(query);
        ex.Message.ShouldContain("OrderBy");
    }

    private static readonly OrderKeyKeyParent[] Parents = Enumerable.Range(1, 4).Select(n => new OrderKeyKeyParent
    {
        Id = Guid.NewGuid(),
        Number = n,
        Children = Enumerable.Range(0, 3).Select(i => new OrderKeyKeyChild
        {
            Index = (n * 7 + i * 5) % 11, Name = Pool[(n + i) % Pool.Length]
        }).ToList()
    }).ToArray();

    private async Task children<T>(string shape, Func<IQueryable<OrderKeyKeyParent>, IQueryable<T>> query)
    {
        await using (var writer = theStore.LightweightSession())
        {
            writer.Store(Parents);
            await writer.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var expected = query(Parents.AsQueryable()).ToList();

        await using var session = theStore.QuerySession();
        (await query(session.Query<OrderKeyKeyParent>()).ToListAsync(TestContext.Current.CancellationToken))
            .ShouldBe(expected, shape);
    }

    public static TheoryData<string> ChildShapes =>
    [
        "negation", "modulo_then_index", "modulo_descending_then_negation", "multiplication", "widening_cast",
        "concatenation", "lower_then_index", "length_then_index", "where_then_negation", "paged_parents_then_negation"
    ];

    // marten#5968: computed keys over the child documents a SelectMany() flattens (gh-1016)
    [Theory]
    [MemberData(nameof(ChildShapes))]
    public Task over_flattened_child_documents(string shape) => shape switch
    {
        "negation" => children(shape, q => q.SelectMany(x => x.Children).OrderBy(c => -c.Index).Select(c => c.Index)),
        "modulo_then_index" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => c.Index % 3).ThenBy(c => c.Index).Select(c => c.Index)),
        "modulo_descending_then_negation" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderByDescending(c => c.Index % 4).ThenBy(c => -c.Index).Select(c => c.Index)),
        "multiplication" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => c.Index * -2 + 1).Select(c => c.Index)),
        "widening_cast" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => (long)c.Index * -1).Select(c => c.Index)),
        "concatenation" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => c.Name + c.Index).Select(c => c.Name + c.Index)),
        "lower_then_index" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => c.Name.ToLower()).ThenBy(c => c.Index).Select(c => c.Index)),
        "length_then_index" => children(shape, q =>
            q.SelectMany(x => x.Children).OrderBy(c => c.Name.Length).ThenByDescending(c => c.Index).Select(c => c.Index)),
        "where_then_negation" => children(shape, q =>
            q.SelectMany(x => x.Children).Where(c => c.Index % 2 == 0).OrderBy(c => -c.Index).Select(c => c.Index)),
        "paged_parents_then_negation" => children(shape, q =>
            q.OrderBy(x => x.Number).Skip(1).Take(3).SelectMany(x => x.Children).OrderBy(c => -c.Index)
                .Select(c => c.Index)),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    // gh-1015 x gh-1012: a member of the side of a left join that may have no match reaches the ordering as
    // o == null ? null : o.Amount, which is what the member means in SQL, so it orders (nulls first, as in .NET)
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ordering_by_the_optional_side_of_a_left_join(bool descending)
    {
        await SeedAsync();
        var extra = new OrderKeyKeyCustomer { Id = Guid.NewGuid(), Name = "zulu", Rank = 9 };
        await using (var writer = theStore.LightweightSession())
        {
            writer.Store(extra);
            await writer.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        IQueryable<OrderKeyKeyCustomer> customers = Customers.Append(extra).AsQueryable();
        IQueryable<OrderKeyKeyOrder> orders = Orders.AsQueryable();

        static IQueryable<string> query(IQueryable<OrderKeyKeyCustomer> c, IQueryable<OrderKeyKeyOrder> o, bool descending)
        {
            var rows = c.LeftJoin(o, x => x.Id, y => y.CustomerId,
                (x, y) => new { x.Name, Amount = y == null ? (int?)null : y.Amount });
            var ordered = descending ? rows.OrderByDescending(r => r.Amount) : rows.OrderBy(r => r.Amount);
            return ordered.ThenBy(r => r.Name).Select(r => r.Name + ":" + r.Amount);
        }

        await using var session = theStore.QuerySession();
        var actual = await query(session.Query<OrderKeyKeyCustomer>(), session.Query<OrderKeyKeyOrder>(), descending)
            .ToListAsync(TestContext.Current.CancellationToken);

        actual.ShouldBe(query(customers, orders, descending).ToList());
    }

    // marten#5968: a key over the child documents without an exact translation is refused
    [Theory]
    [InlineData("child_math_call")]
    [InlineData("child_conditional")]
    [InlineData("child_decimal_division")]
    public async Task untranslatable_child_keys_are_refused(string shape)
    {
        await using var session = theStore.QuerySession();
        var parents = session.Query<OrderKeyKeyParent>();
        var token = TestContext.Current.CancellationToken;

        Func<Task> query = shape switch
        {
            "child_math_call" => () => parents.SelectMany(x => x.Children).OrderBy(c => Math.Abs(c.Index - 7)).ToListAsync(token),
            "child_conditional" => () => parents.SelectMany(x => x.Children).OrderBy(c => c.Index > 5 ? 0 : 1).ToListAsync(token),
            "child_decimal_division" => () =>
                parents.SelectMany(x => x.Children).OrderBy(c => (decimal)c.Index / 3m).ToListAsync(token),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(query);
        ex.Message.ShouldContain("OrderBy()");
    }
}
