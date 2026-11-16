using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class JoinOpCustomer
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
}

public class JoinOpOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public string Status { get; set; } = "";
    public decimal Amount { get; set; }
}

public class JoinOpEmployee
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string City { get; set; } = "";
}

public record JoinOpRow(string? Customer, string? Status, decimal? Amount);

// gh-1015 (marten#5910, and the refusals of 02a0e6629): Queryable.Join, and .NET 10's LeftJoin and RightJoin, are
// rewritten to the GroupJoin(...).SelectMany(...) (+ DefaultIfEmpty()) form Polecat already translates. They used to
// be refused outright as untranslatable operators. Every answer here is compared with LINQ to Objects over the same
// rows; the shapes the GroupJoin path cannot answer are refused, naming the operator the query was written with.
public class join_operator_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly Guid Alice = Guid.NewGuid();
    private static readonly Guid Bob = Guid.NewGuid();
    private static readonly Guid Charlie = Guid.NewGuid();

    private static readonly JoinOpCustomer[] Customers =
    [
        new() { Id = Alice, Name = "Alice", City = "Seattle" },
        new() { Id = Bob, Name = "Bob", City = "Portland" },
        // Charlie has no orders, for the left join
        new() { Id = Charlie, Name = "Charlie", City = "Seattle" }
    ];

    private static readonly JoinOpOrder[] Orders =
    [
        new() { Id = Guid.NewGuid(), CustomerId = Alice, Status = "Active", Amount = 100m },
        new() { Id = Guid.NewGuid(), CustomerId = Alice, Status = "Shipped", Amount = 200m },
        new() { Id = Guid.NewGuid(), CustomerId = Bob, Status = "Active", Amount = 300m },
        // An order whose customer does not exist, for the right join
        new() { Id = Guid.NewGuid(), CustomerId = Guid.NewGuid(), Status = "Orphaned", Amount = 400m }
    ];

    private static readonly JoinOpEmployee[] Employees =
    [
        new() { Id = Guid.NewGuid(), Name = "Eve", City = "Seattle" },
        new() { Id = Guid.NewGuid(), Name = "Frank", City = "Portland" },
        new() { Id = Guid.NewGuid(), Name = "Grace", City = "Denver" }
    ];

    private async Task<IQuerySession> SeedAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            // Stored out of order, so nothing passes by insertion order alone
            foreach (var customer in Customers.Reverse()) session.Store(customer);
            foreach (var order in Orders.Reverse()) session.Store(order);
            foreach (var employee in Employees.Reverse()) session.Store(employee);
            await session.SaveChangesAsync(Ct);
        }

        return theStore.QuerySession();
    }

    private static List<JoinOpRow> Sorted(IEnumerable<JoinOpRow> rows) =>
        rows.OrderBy(x => x.Customer).ThenBy(x => x.Status).ThenBy(x => x.Amount).ToList();

    private static async Task<BadLinqExpressionException> RefusedAsync<T>(Func<Task<T>> act, string operatorName,
        string because)
    {
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(act);
        ex.Message.ShouldContain($"Polecat cannot translate this {operatorName}");
        ex.Message.ShouldContain(because);
        return ex;
    }

    // --- Join -------------------------------------------------------------------------------------------------

    private static IQueryable<JoinOpRow> Joined(IQuerySession session) => session.Query<JoinOpCustomer>()
        .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId,
            (c, o) => new JoinOpRow(c.Name, o.Status, o.Amount));

    private static IEnumerable<JoinOpRow> JoinedInMemory() => Customers
        .Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => new JoinOpRow(c.Name, o.Status, o.Amount));

    [Fact]
    public async Task join_returns_the_matching_rows()
    {
        await using var session = await SeedAsync();

        var rows = await Joined(session).ToListAsync(Ct);

        Sorted(rows).ShouldBe(Sorted(JoinedInMemory()));
        rows.Count.ShouldBe(3);
    }

    [Fact]
    public async Task join_with_where_before_on_both_sources_and_after()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Where(c => c.City == "Seattle")
            .Join(session.Query<JoinOpOrder>().Where(o => o.Status != "Cancelled"), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c.Name, o.Status, o.Amount))
            .Where(x => x.Amount > 150m)
            .ToListAsync(Ct);

        var expected = Customers.Where(c => c.City == "Seattle")
            .Join(Orders.Where(o => o.Status != "Cancelled"), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c.Name, o.Status, o.Amount))
            .Where(x => x.Amount > 150m);

        Sorted(rows).ShouldBe(Sorted(expected));
        rows.Single().Amount.ShouldBe(200m);
    }

    [Fact]
    public async Task join_where_after_on_the_outer_side()
    {
        await using var session = await SeedAsync();

        var rows = await Joined(session).Where(x => x.Customer == "Alice").ToListAsync(Ct);

        Sorted(rows).ShouldBe(Sorted(JoinedInMemory().Where(x => x.Customer == "Alice")));
    }

    [Fact]
    public async Task join_then_order_by_skip_and_take()
    {
        await using var session = await SeedAsync();

        var rows = await Joined(session).OrderByDescending(x => x.Amount).Skip(1).Take(2).ToListAsync(Ct);

        rows.ShouldBe(JoinedInMemory().OrderByDescending(x => x.Amount).Skip(1).Take(2).ToList());
    }

    [Fact]
    public async Task join_then_first_and_first_or_default()
    {
        await using var session = await SeedAsync();

        (await Joined(session).OrderBy(x => x.Amount).FirstAsync(Ct))
            .ShouldBe(JoinedInMemory().OrderBy(x => x.Amount).First());

        (await Joined(session).FirstOrDefaultAsync(x => x.Status == "Shipped", Ct))
            .ShouldBe(JoinedInMemory().FirstOrDefault(x => x.Status == "Shipped"));

        (await Joined(session).FirstOrDefaultAsync(x => x.Status == "Orphaned", Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task join_then_count_and_any()
    {
        await using var session = await SeedAsync();

        (await Joined(session).CountAsync(Ct)).ShouldBe(JoinedInMemory().Count());
        (await Joined(session).CountAsync(x => x.Status == "Active", Ct))
            .ShouldBe(JoinedInMemory().Count(x => x.Status == "Active"));
        (await Joined(session).AnyAsync(Ct)).ShouldBeTrue();

        // The orphaned order has no customer, so no joined row has its status
        (await Joined(session).AnyAsync(x => x.Status == "Orphaned", Ct)).ShouldBeFalse();
        Customers.Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => o.Status).Any(x => x == "Orphaned")
            .ShouldBeFalse();
    }

    [Fact]
    public async Task join_then_select_and_distinct()
    {
        await using var session = await SeedAsync();

        var names = await Joined(session).Select(x => x.Customer).Distinct().ToListAsync(Ct);

        names.OrderBy(x => x).ShouldBe(JoinedInMemory().Select(x => x.Customer).Distinct().OrderBy(x => x));
    }

    [Fact]
    public async Task join_selecting_the_outer_document()
    {
        await using var session = await SeedAsync();

        var customers = await session.Query<JoinOpCustomer>()
            .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => c)
            .Where(c => c.Name != "Bob")
            .ToListAsync(Ct);

        customers.Select(x => x.Name).ShouldBe(["Alice", "Alice"]);
    }

    [Fact]
    public async Task join_to_a_tuple_then_where_and_order_by_its_items()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => new ValueTuple<string, decimal>(c.Name, o.Amount))
            .Where(t => t.Item2 > 150m)
            .OrderBy(t => t.Item2)
            .ToListAsync(Ct);

        rows.ShouldBe(Customers.Join(Orders, c => c.Id, o => o.CustomerId, (c, o) => (c.Name, o.Amount))
            .Where(t => t.Amount > 150m).OrderBy(t => t.Amount).ToList());
    }

    [Fact]
    public async Task join_on_a_string_member_of_another_document_type()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Join(session.Query<JoinOpEmployee>(), c => c.City, e => e.City, (c, e) => new { Customer = c.Name, Employee = e.Name })
            .ToListAsync(Ct);

        var expected = Customers.Join(Employees, c => c.City, e => e.City,
            (c, e) => new { Customer = c.Name, Employee = e.Name });

        rows.OrderBy(x => x.Customer).ThenBy(x => x.Employee)
            .ShouldBe(expected.OrderBy(x => x.Customer).ThenBy(x => x.Employee));
    }

    [Fact]
    public async Task join_streams_through_to_async_enumerable()
    {
        await using var session = await SeedAsync();

        var rows = new List<JoinOpRow>();
        await foreach (var row in Joined(session).ToAsyncEnumerable(Ct)) rows.Add(row);

        Sorted(rows).ShouldBe(Sorted(JoinedInMemory()));
    }

    [Fact]
    public async Task join_answers_the_same_as_the_hand_written_group_join()
    {
        await using var session = await SeedAsync();

        var handWritten = await session.Query<JoinOpCustomer>()
            .GroupJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, os) => new { c, os })
            .SelectMany(x => x.os, (x, o) => new JoinOpRow(x.c.Name, o.Status, o.Amount))
            .ToListAsync(Ct);

        Sorted(await Joined(session).ToListAsync(Ct)).ShouldBe(Sorted(handWritten));
    }

    // --- LeftJoin ---------------------------------------------------------------------------------------------

    private static IQueryable<JoinOpRow> LeftJoined(IQuerySession session) => session.Query<JoinOpCustomer>()
        .LeftJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId,
            (c, o) => new JoinOpRow(c.Name, o == null ? null : o.Status, o == null ? null : o.Amount));

    private static IEnumerable<JoinOpRow> LeftJoinedInMemory() => Customers
        .LeftJoin(Orders, c => c.Id, o => o.CustomerId,
            (c, o) => new JoinOpRow(c.Name, o == null ? null : o.Status, o == null ? null : o.Amount));

    [Fact]
    public async Task left_join_keeps_an_outer_row_with_no_match()
    {
        await using var session = await SeedAsync();

        var rows = await LeftJoined(session).ToListAsync(Ct);

        Sorted(rows).ShouldBe(Sorted(LeftJoinedInMemory()));
        rows.ShouldContain(new JoinOpRow("Charlie", null, null));
    }

    [Fact]
    public async Task left_join_with_a_filtered_inner_source_keeps_the_outer_rows_it_leaves_unmatched()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Where(c => c.Name != "Charlie")
            .LeftJoin(session.Query<JoinOpOrder>().Where(o => o.Status == "Shipped"), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c.Name, o == null ? null : o.Status, o == null ? null : o.Amount))
            .ToListAsync(Ct);

        var expected = Customers.Where(c => c.Name != "Charlie")
            .LeftJoin(Orders.Where(o => o.Status == "Shipped"), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c.Name, o == null ? null : o.Status, o == null ? null : o.Amount));

        Sorted(rows).ShouldBe(Sorted(expected));
        rows.ShouldContain(new JoinOpRow("Bob", null, null));
    }

    [Fact]
    public async Task left_join_where_after_on_the_outer_side_then_order_and_page()
    {
        await using var session = await SeedAsync();

        // Ordered by the outer side only: a key over the optional side is a conditional (o == null ? ...), which
        // the ordering path refuses as untranslatable. Alice's two rows tie, so the page takes both of them (a page
        // ending inside the tie would hold either one) and they are compared sorted.
        var rows = await LeftJoined(session)
            .Where(x => x.Customer != "Bob")
            .OrderByDescending(x => x.Customer)
            .Take(3)
            .ToListAsync(Ct);

        Sorted(rows).ShouldBe(Sorted(LeftJoinedInMemory().Where(x => x.Customer != "Bob")
            .OrderByDescending(x => x.Customer).Take(3)));
        rows[0].ShouldBe(new JoinOpRow("Charlie", null, null));
        rows.Count.ShouldBe(3);
    }

    [Fact]
    public async Task left_join_count_and_any_count_the_unmatched_rows()
    {
        await using var session = await SeedAsync();

        (await LeftJoined(session).CountAsync(Ct)).ShouldBe(LeftJoinedInMemory().Count());
        (await LeftJoined(session).CountAsync(Ct)).ShouldBe(4);
        (await LeftJoined(session).AnyAsync(x => x.Customer == "Charlie", Ct)).ShouldBeTrue();
    }

    [Fact]
    public async Task left_join_where_after_on_the_inner_side_is_refused()
    {
        await using var session = await SeedAsync();

        // Filtering the inner rows before joining would keep Charlie, whom this filter removes in memory
        await RefusedAsync(() => LeftJoined(session).Where(x => x.Amount > 150m).ToListAsync(Ct),
            "LeftJoin()", "the inner side of a left join");
    }

    // --- RightJoin --------------------------------------------------------------------------------------------

    private static IQueryable<JoinOpRow> RightJoined(IQuerySession session) => session.Query<JoinOpCustomer>()
        .RightJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId,
            (c, o) => new JoinOpRow(c == null ? null : c.Name, o.Status, o.Amount));

    private static IEnumerable<JoinOpRow> RightJoinedInMemory() => Customers
        .RightJoin(Orders, c => c.Id, o => o.CustomerId,
            (c, o) => new JoinOpRow(c == null ? null : c.Name, o.Status, o.Amount));

    [Fact]
    public async Task right_join_keeps_an_inner_row_with_no_match()
    {
        await using var session = await SeedAsync();

        var rows = await RightJoined(session).ToListAsync(Ct);

        Sorted(rows).ShouldBe(Sorted(RightJoinedInMemory()));
        rows.ShouldContain(new JoinOpRow(null, "Orphaned", 400m));
        rows.ShouldNotContain(x => x.Customer == "Charlie");
    }

    [Fact]
    public async Task right_join_with_where_before_on_both_sources()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Where(c => c.City == "Seattle")
            .RightJoin(session.Query<JoinOpOrder>().Where(o => o.Amount >= 200m), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c == null ? null : c.Name, o.Status, o.Amount))
            .ToListAsync(Ct);

        var expected = Customers.Where(c => c.City == "Seattle")
            .RightJoin(Orders.Where(o => o.Amount >= 200m), c => c.Id, o => o.CustomerId,
                (c, o) => new JoinOpRow(c == null ? null : c.Name, o.Status, o.Amount));

        Sorted(rows).ShouldBe(Sorted(expected));

        // Bob lives in Portland, so his order is kept without him
        rows.ShouldContain(new JoinOpRow(null, "Active", 300m));
    }

    [Fact]
    public async Task right_join_where_after_on_the_inner_side_then_order_and_page()
    {
        await using var session = await SeedAsync();

        var rows = await RightJoined(session)
            .Where(x => x.Amount > 100m)
            .OrderByDescending(x => x.Amount)
            .Skip(1)
            .ToListAsync(Ct);

        rows.ShouldBe(RightJoinedInMemory().Where(x => x.Amount > 100m)
            .OrderByDescending(x => x.Amount).Skip(1).ToList());
    }

    [Fact]
    public async Task right_join_count_first_and_select()
    {
        await using var session = await SeedAsync();

        (await RightJoined(session).CountAsync(Ct)).ShouldBe(RightJoinedInMemory().Count());
        (await RightJoined(session).OrderBy(x => x.Amount).FirstAsync(Ct))
            .ShouldBe(RightJoinedInMemory().OrderBy(x => x.Amount).First());

        var statuses = await RightJoined(session).Select(x => x.Status).ToListAsync(Ct);
        statuses.OrderBy(x => x).ShouldBe(RightJoinedInMemory().Select(x => x.Status).OrderBy(x => x));
    }

    [Fact]
    public async Task right_join_where_after_on_the_outer_side_is_refused()
    {
        await using var session = await SeedAsync();

        // Filtering the customers before joining would keep the orphaned order, which this filter removes in memory
        await RefusedAsync(() => RightJoined(session).Where(x => x.Customer == "Alice").ToListAsync(Ct),
            "RightJoin()", "the outer side of a right join");
    }

    // --- refusals ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task an_operator_other_than_where_before_a_join_is_refused()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .OrderBy(c => c.Name)
                .Take(2)
                .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "Join()", "only Where() may come before it on the outer source, and this query applies Take()");

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .LeftJoin(session.Query<JoinOpOrder>().OrderBy(o => o.Amount), c => c.Id, o => o.CustomerId,
                    (c, o) => c.Name)
                .ToListAsync(Ct),
            "LeftJoin()", "the inner source may only be session.Query<T>() filtered by Where(), and it applies OrderBy()");
    }

    [Fact]
    public async Task an_operator_before_either_side_of_a_right_join_is_refused()
    {
        await using var session = await SeedAsync();

        // The outer source of a RightJoin becomes the GroupJoin's inner one, so it is held to the same rule
        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Skip(1)
                .RightJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "RightJoin()", "the outer source may only be session.Query<T>() filtered by Where(), and it applies Skip()");

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .RightJoin(session.Query<JoinOpOrder>().Select(o => o), c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "RightJoin()", "the inner source may only be session.Query<T>() filtered by Where(), and it applies Select()");
    }

    [Fact]
    public async Task an_in_memory_inner_source_is_refused()
    {
        await using var session = await SeedAsync();

        var inMemory = new List<JoinOpOrder> { new() { Id = Guid.NewGuid(), CustomerId = Alice, Amount = 1m } };

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Join(inMemory, c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "Join()", "the inner source must be a Polecat session.Query<T>()");

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .RightJoin(inMemory, c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "RightJoin()", "the inner source must be a Polecat session.Query<T>()");
    }

    [Fact]
    public async Task a_composite_key_is_refused()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Join(session.Query<JoinOpEmployee>(), c => new { c.City, c.Name }, e => new { e.City, e.Name },
                    (c, e) => e.Name)
                .ToListAsync(Ct),
            "Join()", "the outer key selector builds a composite key");
    }

    [Fact]
    public async Task a_computed_key_is_refused()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .LeftJoin(session.Query<JoinOpEmployee>(), c => c.City, e => e.City.ToUpper(), (c, e) => c.Name)
                .ToListAsync(Ct),
            "LeftJoin()", "the inner key selector must be a member of the document");
    }

    [Fact]
    public async Task the_comparer_overload_is_refused()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Join(session.Query<JoinOpEmployee>(), c => c.City, e => e.City, (c, e) => e.Name,
                    StringComparer.Ordinal)
                .ToListAsync(Ct),
            "Join()", "IEqualityComparer");

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .RightJoin(session.Query<JoinOpEmployee>(), c => c.City, e => e.City, (c, e) => e.Name,
                    StringComparer.Ordinal)
                .ToListAsync(Ct),
            "RightJoin()", "IEqualityComparer");
    }

    [Fact]
    public async Task a_join_of_a_join_is_refused()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => c)
                .Join(session.Query<JoinOpEmployee>(), c => c.City, e => e.City, (c, e) => e.Name)
                .ToListAsync(Ct),
            "Join()", "its outer source is itself a join");

        await RefusedAsync(() => session.Query<JoinOpEmployee>()
                .LeftJoin(session.Query<JoinOpCustomer>()
                        .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => c),
                    e => e.City, c => c.City, (e, c) => e.Name)
                .ToListAsync(Ct),
            "LeftJoin()", "its inner source is itself a join");

        // A hand-written GroupJoin over a Join is the same thing
        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => c)
                .GroupJoin(session.Query<JoinOpEmployee>(), c => c.City, e => e.City, (c, es) => new { c, es })
                .SelectMany(x => x.es, (x, e) => e.Name)
                .ToListAsync(Ct),
            "GroupJoin(...).SelectMany(...)", "its outer source is itself a join");
    }

    [Fact]
    public async Task tenancy_widened_on_the_outer_source_of_a_right_join_is_refused()
    {
        await using var session = await SeedAsync();

        // On a Join() it covers both sides; on a RightJoin() that side becomes the one that may have no match
        await RefusedAsync(() => session.Query<JoinOpCustomer>()
                .AnyTenant()
                .RightJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => o.Amount)
                .ToListAsync(Ct),
            "RightJoin()", "AnyTenant() is applied to its outer source");
    }

    [Fact]
    public async Task an_operator_after_a_join_with_no_translation_names_the_join()
    {
        await using var session = await SeedAsync();

        await RefusedAsync(() => Joined(session).LastAsync(Ct), "Join()", "Last() after the join has no translation");
    }

    [Fact]
    public async Task to_sql_over_a_join_is_refused()
    {
        await using var session = await SeedAsync();

        var ex = Should.Throw<BadLinqExpressionException>(() => session.ToSql(Joined(session)));
        ex.Message.ShouldContain("GroupJoin has no single statement to render");
    }

#if NET11_0_OR_GREATER
    // .NET 11 adds Join/LeftJoin/RightJoin overloads with no result selector, returning (Outer, Inner) tuples
    [Fact]
    public async Task the_tuple_overload_of_join_returns_both_documents()
    {
        await using var session = await SeedAsync();

        var rows = await session.Query<JoinOpCustomer>()
            .Join(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId)
            .Where(t => t.Inner.Amount > 150m)
            .ToListAsync(Ct);

        rows.Select(t => (t.Outer.Name, t.Inner.Amount)).OrderBy(x => x.Amount)
            .ShouldBe([("Alice", 200m), ("Bob", 300m)]);
    }

    [Fact]
    public async Task full_join_is_still_refused()
    {
        await using var session = await SeedAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() => session.Query<JoinOpCustomer>()
            .FullJoin(session.Query<JoinOpOrder>(), c => c.Id, o => o.CustomerId, (c, o) => o)
            .ToListAsync(Ct));
        ex.Message.ShouldContain("'FullJoin'");
    }
#endif
}
