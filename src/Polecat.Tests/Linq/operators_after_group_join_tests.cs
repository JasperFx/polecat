using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class AfterJoinCustomer
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public string Name { get; set; } = "";
}

public class AfterJoinOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public int Index { get; set; }
    public decimal Amount { get; set; }
    public string Status { get; set; } = "";
}

// gh-1001 (marten#5928, marten#5921, marten#5933): the operators AFTER GroupJoin(...).SelectMany(...) read the
// joined rows. A Where()/First(predicate)/ordering used to resolve the row's members against the OUTER document
// (so a filter on o.Index returned nothing, and an ordering on x.c.Number ordered by null), Distinct() was
// ignored, Take(n).Count() counted every joined row, and Any() and Select() crashed with an ArgumentException.
public class operators_after_group_join_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly AfterJoinCustomer[] Customers = Enumerable.Range(1, 5).Select(n => new AfterJoinCustomer
    {
        Id = Guid.NewGuid(), Number = n, Name = new[] { "Ann", "Bobby", "Cy", "Deena", "Eve" }[n - 1]
    }).ToArray();

    // Customers 1-3 have three orders each, customer 4 has none, customer 5 has exactly one.
    private static readonly AfterJoinOrder[] Orders = Customers
        .SelectMany(c => Enumerable.Range(0, c.Number switch { 4 => 0, 5 => 1, _ => 3 }).Select(i => new AfterJoinOrder
        {
            Id = Guid.NewGuid(),
            CustomerId = c.Id,
            Index = i,
            Amount = c.Number * 100 + i * 10,
            Status = i == 0 ? "new" : "shipped"
        }))
        .ToArray();

    private async Task<IQuerySession> SeedAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            // Stored out of order, so nothing passes by insertion order alone
            foreach (var customer in Customers.Reverse()) session.Store(customer);
            foreach (var order in Orders.Reverse()) session.Store(order);
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    private static IQueryable<AfterJoinRow> Joined(IQuerySession session) => session.Query<AfterJoinCustomer>()
        .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
        .SelectMany(x => x.os, (x, o) => new AfterJoinRow { Number = x.c.Number, Name = x.c.Name, Index = o.Index, Amount = o.Amount });

    private static IEnumerable<(int Number, int Index)> JoinedInMemory(IEnumerable<AfterJoinCustomer> customers)
        => customers.Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => (c.Number, o.Index));

    private static List<(int, int)> Sorted(IEnumerable<(int Number, int Index)> rows)
        => rows.OrderBy(x => x.Number).ThenBy(x => x.Index).Select(x => (x.Number, x.Index)).ToList();

    public class AfterJoinRow
    {
        public int Number { get; set; }
        public string Name { get; set; } = "";
        public int Index { get; set; }
        public decimal Amount { get; set; }
    }

    // --- Where / First(predicate) ---------------------------------------------------------------------------

    [Fact]
    public async Task where_on_an_inner_member_filters_the_joined_rows()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { x.c.Number, o.Index })
            .Where(r => r.Index > 0)
            .ToListAsync(Ct);

        Sorted(rows.Select(r => (r.Number, r.Index))).ShouldBe(Sorted(JoinedInMemory(Customers).Where(r => r.Index > 0)));
    }

    [Fact]
    public async Task where_on_a_renamed_outer_member_filters_the_outer_side()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { Customer = x.c.Name, o.Amount })
            .Where(r => r.Customer == "Bobby")
            .ToListAsync(Ct);

        rows.Select(r => r.Amount).OrderBy(x => x).ShouldBe([200m, 210m, 220m]);
    }

    [Fact]
    public async Task where_over_both_sides_filters_each_side()
    {
        await using var session = await SeedAsync();

        var rows = await Joined(session).Where(r => r.Number > 1 && r.Index == 2).ToListAsync(Ct);

        Sorted(rows.Select(r => (r.Number, r.Index))).ShouldBe([(2, 2), (3, 2)]);
    }

    [Fact]
    public async Task where_comparing_the_two_sides_is_refused()
    {
        await using var session = await SeedAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            Joined(session).Where(r => r.Number == r.Index).ToListAsync(Ct));
        ex.Message.ShouldContain("compares the two sides of the join");
    }

    [Fact]
    public async Task where_on_the_inner_side_of_a_left_join_is_refused()
    {
        await using var session = await SeedAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os.DefaultIfEmpty(),
                (x, o) => new { x.c.Name, Amount = o != null ? (decimal?)o.Amount : null })
            .Where(r => r.Amount == null)
            .ToListAsync(Ct));
        ex.Message.ShouldContain("left join");
    }

    [Fact]
    public async Task where_on_the_outer_side_of_a_left_join_keeps_the_unmatched_rows()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os.DefaultIfEmpty(),
                (x, o) => new { x.c.Number, Amount = o != null ? (decimal?)o.Amount : null })
            .Where(r => r.Number >= 4)
            .ToListAsync(Ct);

        rows.OrderBy(r => r.Number).Select(r => (r.Number, r.Amount)).ShouldBe([(4, (decimal?)null), (5, 500m)]);
    }

    [Fact]
    public async Task first_with_a_predicate_on_an_inner_member()
    {
        await using var session = await SeedAsync();

        var row = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { x.c.Number, o.Amount })
            .FirstAsync(r => r.Amount == 320m, Ct);

        (row.Number, row.Amount).ShouldBe((3, 320m));

        (await Joined(session).FirstOrDefaultAsync(r => r.Amount == 9999m, Ct)).ShouldBeNull();
        await Should.ThrowAsync<InvalidOperationException>(() => Joined(session).FirstAsync(r => r.Amount == 9999m, Ct));
    }

    [Fact]
    public async Task where_order_by_then_first()
    {
        await using var session = await SeedAsync();

        var row = await Joined(session)
            .Where(r => r.Index == 2)
            .OrderByDescending(r => r.Amount)
            .FirstAsync(Ct);

        (row.Number, row.Index).ShouldBe((3, 2));
    }

    // --- ordering and paging --------------------------------------------------------------------------------

    [Fact]
    public async Task ordering_and_paging_over_projected_members_pages_the_joined_rows()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { x.c.Number, o.Index })
            .OrderBy(r => r.Number).ThenBy(r => r.Index).Skip(2).Take(4)
            .ToListAsync(Ct);

        rows.Select(r => (r.Number, r.Index)).ShouldBe([(1, 2), (2, 0), (2, 1), (2, 2)]);
    }

    [Fact]
    public async Task an_operator_after_paging_the_join_is_refused()
    {
        await using var session = await SeedAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            Joined(session).OrderBy(r => r.Amount).Take(3).Where(r => r.Index > 0).ToListAsync(Ct));
        ex.Message.ShouldContain("after Take() or Skip()");
    }

    // --- Distinct -------------------------------------------------------------------------------------------

    [Fact]
    public async Task distinct_compares_the_projected_rows()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { o.Status })
            .Distinct()
            .ToListAsync(Ct);

        rows.Select(r => r.Status).OrderBy(s => s).ShouldBe(["new", "shipped"]);
    }

    [Fact]
    public async Task distinct_over_both_sides_ordered_and_counted()
    {
        await using var session = await SeedAsync();

        IQueryable<AfterJoinNameStatus> distinct() => session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new AfterJoinNameStatus { Name = x.c.Name, Status = o.Status })
            .Distinct();

        var expected = Customers.Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => (c.Name, o.Status))
            .Distinct().OrderByDescending(x => x.Name).ThenBy(x => x.Status).ToList();

        var rows = await distinct().OrderByDescending(r => r.Name).ThenBy(r => r.Status).ToListAsync(Ct);
        rows.Select(r => (r.Name, r.Status)).ShouldBe(expected);

        (await distinct().CountAsync(Ct)).ShouldBe(expected.Count);
    }

    public class AfterJoinNameStatus
    {
        public string Name { get; set; } = "";
        public string Status { get; set; } = "";
    }

    [Fact]
    public async Task distinct_over_a_projected_scalar()
    {
        await using var session = await SeedAsync();

        var statuses = await session.Query<AfterJoinCustomer>()
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { x.c.Name, o.Status })
            .Select(r => r.Status)
            .Distinct()
            .OrderBy(s => s)
            .ToListAsync(Ct);

        statuses.ShouldBe(["new", "shipped"]);
    }

    [Fact]
    public async Task distinct_over_a_computed_projection_is_refused()
    {
        await using var session = await SeedAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            Joined(session).Select(r => r.Number * 10 + r.Index).Distinct().ToListAsync(Ct));
    }

    // --- Count / Any ----------------------------------------------------------------------------------------

    [Fact]
    public async Task count_after_take_counts_the_page()
    {
        await using var session = await SeedAsync();

        (await Joined(session).CountAsync(Ct)).ShouldBe(Orders.Length);
        (await Joined(session).Take(4).CountAsync(Ct)).ShouldBe(4);
        (await Joined(session).OrderBy(r => r.Amount).Skip(8).CountAsync(Ct)).ShouldBe(Orders.Length - 8);
        (await Joined(session).OrderBy(r => r.Amount).Skip(2).Take(3).LongCountAsync(Ct)).ShouldBe(3L);
    }

    [Fact]
    public async Task any_asks_whether_the_join_has_a_row()
    {
        await using var session = await SeedAsync();

        (await Joined(session).AnyAsync(Ct)).ShouldBeTrue();

        // Customer 4 has no orders
        (await session.Query<AfterJoinCustomer>().Where(c => c.Number == 4)
            .GroupJoin(session.Query<AfterJoinOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new { x.c.Number, o.Index })
            .AnyAsync(Ct)).ShouldBeFalse();

        (await Joined(session).AnyAsync(r => r.Amount > 500m, Ct)).ShouldBeFalse();
        (await Joined(session).AnyAsync(r => r.Amount == 500m, Ct)).ShouldBeTrue();
        (await Joined(session).OrderBy(r => r.Amount).Skip(Orders.Length).AnyAsync(Ct)).ShouldBeFalse();
    }

    // --- Select ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task select_projects_the_joined_rows()
    {
        await using var session = await SeedAsync();

        var amounts = await Joined(session).Select(r => r.Amount).ToListAsync(Ct);
        amounts.OrderBy(a => a).ShouldBe(Orders.Select(o => o.Amount).OrderBy(a => a));

        var values = await Joined(session).Select(r => r.Number * 10 + r.Index).ToListAsync(Ct);
        values.OrderBy(v => v).ShouldBe(JoinedInMemory(Customers).Select(r => r.Number * 10 + r.Index).OrderBy(v => v));

        var shaped = await Joined(session).Select(r => new { Upper = r.Name.ToUpper(), Doubled = r.Index * 2 })
            .ToListAsync(Ct);
        shaped.Select(r => $"{r.Upper}:{r.Doubled}").OrderBy(x => x, StringComparer.Ordinal).ShouldBe(
            Customers.Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => $"{c.Name.ToUpper()}:{o.Index * 2}")
                .OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public async Task operators_after_a_select_read_through_it()
    {
        await using var session = await SeedAsync();

        var amounts = await Joined(session)
            .Select(r => new { Customer = r.Number, Cost = r.Amount })
            .Where(r => r.Cost > 300m)
            .OrderByDescending(r => r.Cost)
            .Select(r => r.Cost)
            .ToListAsync(Ct);

        amounts.ShouldBe(Orders.Select(o => o.Amount).Where(a => a > 300m).OrderByDescending(a => a));
    }

    [Fact]
    public async Task an_aggregate_after_the_join_is_refused()
    {
        await using var session = await SeedAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() => Joined(session).SumAsync(r => r.Amount, Ct));
    }
}
