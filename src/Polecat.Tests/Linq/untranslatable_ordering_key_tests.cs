using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class CastKeyDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public double Amount { get; set; }
    public string Name { get; set; } = "";
}

public class CastKeyCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Rank { get; set; }
    public bool Flag { get; set; }
}

public class CastKeyOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public int Amount { get; set; }
    public string Status { get; set; } = "";
}

// gh-999, mirroring marten#5957 and marten#5968. An ordering key is translated exactly or refused, never
// silently ordered by something else:
//
//   * every cast in a document ordering key was stepped over, so OrderBy(x => (byte)x.Number) ordered by
//     Number although (byte)256 is 0. Only a cast that cannot change the order is stepped over now (an exact
//     widening, boxing, a nullable or enum wrapper); any other cast is refused.
//   * over GroupJoin(...).SelectMany(...) any key that was not a plain member of the joined row was skipped
//     without a word, so the rows came back in whatever order SQL Server chose. It is refused now.
//
// gh-1012 then translated computed keys (arithmetic, concatenation); what has no exact translation stays refused.
public class untranslatable_ordering_key_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // (byte) wraps 256..258 to 0..2, so a narrowing cast changes the order of these
    private static readonly CastKeyDoc[] Docs = new[] { 256, 254, 258, 255, 257 }
        .Select(n => new CastKeyDoc { Id = Guid.NewGuid(), Number = n, Amount = n / 4.0, Name = "n" + n }).ToArray();

    private async Task SeedAsync()
    {
        foreach (var doc in Docs)
        {
            await using var session = theStore.LightweightSession();
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }
    }

    private static readonly Dictionary<string, Func<IQueryable<CastKeyDoc>, IQueryable<CastKeyDoc>>> NarrowingKeys =
        new()
        {
            ["to_byte"] = q => q.OrderBy(x => (byte)x.Number),
            ["to_byte_descending"] = q => q.OrderByDescending(x => (byte)x.Number),
            ["to_short_then_member"] = q => q.OrderBy(x => (short)x.Number).ThenBy(x => x.Number),
            ["then_by_to_byte"] = q => q.OrderBy(x => x.Name).ThenBy(x => (byte)x.Number),
            ["double_to_int"] = q => q.OrderBy(x => (int)x.Amount),
            ["int_to_float"] = q => q.OrderBy(x => (float)x.Number),
            ["checked_to_byte"] = q => q.OrderBy(x => checked((byte)x.Number))
        };

    public static TheoryData<string> NarrowingKeyNames() => new(NarrowingKeys.Keys);

    [Theory]
    [MemberData(nameof(NarrowingKeyNames))]
    public async Task a_cast_that_can_change_the_order_is_refused(string name)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            NarrowingKeys[name](session.Query<CastKeyDoc>()).ToListAsync(Ct));

        ex.Message.ShouldContain("OrderBy", Case.Sensitive, name);
        ex.Message.ShouldContain("cast", Case.Sensitive, name);
    }

    // The crash in the issue: the stripped cast made both keys the same column
    [Fact]
    public async Task a_narrowing_cast_then_its_own_member_is_refused_rather_than_a_sql_error()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<CastKeyDoc>()
            .OrderBy(x => (byte)x.Number).ThenBy(x => x.Number).ToListAsync(Ct));
    }

    // Select(x => (byte)x.Number) reads Number and narrows it in memory, so the column is not the value
    [Fact]
    public async Task ordering_a_narrowed_projection_is_refused()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<CastKeyDoc>()
            .Select(x => (byte)x.Number).OrderBy(n => n).ToListAsync(Ct));
    }

    private static readonly Dictionary<string, Func<IQueryable<CastKeyDoc>, IQueryable<CastKeyDoc>>> WideningKeys =
        new()
        {
            ["to_long"] = q => q.OrderBy(x => (long)x.Number),
            ["to_double_descending"] = q => q.OrderByDescending(x => (double)x.Number),
            ["to_decimal"] = q => q.OrderBy(x => (decimal)x.Number),
            ["to_nullable"] = q => q.OrderByDescending(x => (int?)x.Number),
            ["boxed"] = q => q.OrderBy(x => (object)x.Number),
            ["then_by_widened"] = q => q.OrderBy(x => x.Name.Length).ThenByDescending(x => (long)x.Number)
        };

    public static TheoryData<string> WideningKeyNames() => new(WideningKeys.Keys);

    [Theory]
    [MemberData(nameof(WideningKeyNames))]
    public async Task a_cast_that_cannot_change_the_order_still_orders(string name)
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var expected = WideningKeys[name](Docs.AsQueryable()).Select(x => x.Number).ToList();
        var actual = await WideningKeys[name](session.Query<CastKeyDoc>()).Select(x => x.Number).ToListAsync(Ct);

        actual.ShouldBe(expected, name);
    }

    [Fact]
    public async Task a_widened_projection_still_orders()
    {
        await SeedAsync();
        await using var session = theStore.QuerySession();

        var actual = await session.Query<CastKeyDoc>().Select(x => (long)x.Number).OrderByDescending(n => n)
            .ToListAsync(Ct);

        actual.ShouldBe(Docs.Select(x => (long)x.Number).OrderByDescending(n => n).ToList());
    }

    private static readonly CastKeyCustomer[] Customers = Enumerable.Range(1, 4)
        .Select(i => new CastKeyCustomer { Id = Guid.NewGuid(), Name = "customer" + i, Rank = i * 3 % 5, Flag = i % 2 == 0 })
        .ToArray();

    private static readonly CastKeyOrder[] Orders = Customers.SelectMany((c, i) => Enumerable.Range(0, 3)
        .Select(k => new CastKeyOrder
        {
            Id = Guid.NewGuid(), CustomerId = c.Id, Amount = (i + 1) * 10 + k, Status = "s" + (i + k) % 4
        })).ToArray();

    private async Task SeedJoinAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(Customers);
        session.Store(Orders);
        await session.SaveChangesAsync(Ct);
    }

    // gh-1012 translates negation, %, concatenation and arithmetic over either side (see
    // computed_ordering_keys_over_joins_tests); these have no exact translation and stay refused.
    public static TheoryData<string> UntranslatableJoinKeys => ["math_call", "conditional", "narrowing"];

    [Theory]
    [MemberData(nameof(UntranslatableJoinKeys))]
    public async Task a_join_key_without_an_exact_translation_is_refused(string shape)
    {
        await SeedJoinAsync();
        await using var session = theStore.QuerySession();

        var rows = session.Query<CastKeyCustomer>()
            .GroupJoin(session.Query<CastKeyOrder>(), x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Name, z.x.Rank, y.Amount, y.Status });

        Func<Task> query = shape switch
        {
            "math_call" => () => rows.OrderBy(r => Math.Abs(r.Amount - 25)).ToListAsync(Ct),
            "conditional" => () => rows.OrderBy(r => r.Amount > 25 ? 0 : 1).ToListAsync(Ct),
            "narrowing" => () => rows.OrderBy(r => (short)r.Amount).ToListAsync(Ct),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(query);
        ex.Message.ShouldContain("OrderBy", Case.Sensitive, shape);
    }

    // gh-1001 reads a key through the projection, so a member of a projected document is translatable. It
    // used to be skipped, like every key that was not a plain member of the joined row.
    [Fact]
    public async Task a_member_of_a_projected_document_over_a_join_orders()
    {
        await SeedJoinAsync();
        await using var session = theStore.QuerySession();

        var expected = Customers.AsQueryable()
            .GroupJoin(Orders.AsQueryable(), x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Name, Order = y })
            .OrderByDescending(r => r.Order.Amount).Select(r => r.Order.Amount).ToList();

        var actual = await session.Query<CastKeyCustomer>()
            .GroupJoin(session.Query<CastKeyOrder>(), x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Name, Order = y })
            .OrderByDescending(r => r.Order.Amount).ToListAsync(Ct);

        actual.Select(r => r.Order.Amount).ShouldBe(expected);
    }

    [Fact]
    public async Task a_widening_cast_over_a_join_still_orders()
    {
        await SeedJoinAsync();
        await using var session = theStore.QuerySession();

        IQueryable<CastKeyCustomer> customers = Customers.AsQueryable();
        var expected = customers
            .GroupJoin(Orders.AsQueryable(), x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
            .OrderBy(r => (long)r.Rank).ThenByDescending(r => (double)r.Amount).ToList();

        var actual = await session.Query<CastKeyCustomer>()
            .GroupJoin(session.Query<CastKeyOrder>(), x => x.Id, y => y.CustomerId, (x, ys) => new { x, ys })
            .SelectMany(z => z.ys, (z, y) => new { z.x.Rank, y.Amount })
            .OrderBy(r => (long)r.Rank).ThenByDescending(r => (double)r.Amount).ToListAsync(Ct);

        actual.ShouldBe(expected);
    }
}
