using System.Text.Json;
using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Pagination;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #837: what comes after <c>GroupBy(...).Select(...)</c>. A terminal operator used to run against
///     the grouped select list and read an empty result, so these either answer over the groups or say
///     why they will not.
/// </summary>
public class group_by_terminal_operator_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private async Task SeedAsync()
    {
        await using var session = theStore.LightweightSession();
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "A", Age = 1 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "B", Age = 2 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "B", Age = 3 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "C", Age = 4 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "C", Age = 5 });
        session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "C", Age = 6 });
        await session.SaveChangesAsync(Token);
    }

    private IQueryable<LinqTarget> Targets()
    {
        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);
        return query.Query<LinqTarget>();
    }

    private IQueryable<LinqTarget> NoTargets() => Targets().Where(x => x.Age > 100);

    // ---- counting ---------------------------------------------------------------------------------

    [Fact]
    public async Task count_counts_the_groups_rather_than_the_rows()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).CountAsync(Token)).ShouldBe(3);
    }

    [Fact]
    public async Task count_over_a_projection()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() }).CountAsync(Token))
            .ShouldBe(3);
    }

    [Fact]
    public async Task long_count()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).LongCountAsync(Token)).ShouldBe(3L);
    }

    [Fact]
    public async Task count_without_a_select()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).CountAsync(Token)).ShouldBe(3);
    }

    [Fact]
    public async Task count_after_having()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Where(g => g.Count() > 1).Select(g => g.Key).CountAsync(Token))
            .ShouldBe(2);
    }

    [Fact]
    public async Task count_with_an_ordering()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).CountAsync(Token)).ShouldBe(3);
    }

    [Fact]
    public async Task count_of_a_page()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).Take(2).CountAsync(Token))
            .ShouldBe(2);
    }

    [Fact]
    public async Task count_with_an_inline_predicate_filters_the_groups()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
            .CountAsync(x => x.C > 1, Token)).ShouldBe(2);
    }

    [Fact]
    public async Task count_of_no_groups()
    {
        (await NoTargets().GroupBy(x => x.Name).Select(g => g.Key).CountAsync(Token)).ShouldBe(0);
    }

    [Fact]
    public async Task distinct_values_of_a_grouped_scalar()
    {
        await SeedAsync();

        // Six groups, but only three names among them.
        var names = await Targets().GroupBy(x => new { x.Name, x.Age }).Select(g => g.Key.Name).Distinct()
            .CountAsync(Token);

        names.ShouldBe(3);
    }

    // ---- any --------------------------------------------------------------------------------------

    [Fact]
    public async Task any_is_true_with_groups_and_false_without()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).AnyAsync(Token)).ShouldBeTrue();
        (await NoTargets().GroupBy(x => x.Name).Select(g => g.Key).AnyAsync(Token)).ShouldBeFalse();
    }

    [Fact]
    public async Task any_with_a_predicate_filters_the_groups()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
            .AnyAsync(x => x.C > 2, Token)).ShouldBeTrue();

        (await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
            .AnyAsync(x => x.C > 3, Token)).ShouldBeFalse();
    }

    [Fact]
    public async Task any_without_a_select()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).AnyAsync(Token)).ShouldBeTrue();
    }

    // ---- one row ----------------------------------------------------------------------------------

    [Fact]
    public async Task first_of_a_scalar()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Count()).OrderByDescending(x => x).FirstAsync(Token))
            .ShouldBe(3);
    }

    [Fact]
    public async Task first_of_a_projection()
    {
        await SeedAsync();

        var row = await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
            .OrderByDescending(x => x.C).FirstAsync(Token);

        row.Key.ShouldBe("C");
        row.C.ShouldBe(3);
    }

    [Fact]
    public async Task first_of_no_groups_throws_and_first_or_default_does_not()
    {
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await NoTargets().GroupBy(x => x.Name).Select(g => g.Key).FirstAsync(Token));

        (await NoTargets().GroupBy(x => x.Name).Select(g => g.Key).FirstOrDefaultAsync(Token)).ShouldBeNull();
        (await NoTargets().GroupBy(x => x.Name).Select(g => g.Count()).FirstOrDefaultAsync(Token)).ShouldBe(0);
    }

    [Fact]
    public async Task single_of_one_group()
    {
        await SeedAsync();

        var row = await Targets().Where(x => x.Name == "B").GroupBy(x => x.Name)
            .Select(g => new { g.Key, C = g.Count() }).SingleAsync(Token);

        row.C.ShouldBe(2);
    }

    [Fact]
    public async Task single_of_several_groups_throws()
    {
        await SeedAsync();

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => g.Key).SingleAsync(Token));

        ex.Message.ShouldContain("more than one");
    }

    [Fact]
    public async Task single_or_default_of_no_groups()
    {
        (await NoTargets().GroupBy(x => x.Name).Select(g => g.Key).SingleOrDefaultAsync(Token)).ShouldBeNull();
    }

    [Fact]
    public async Task last_reverses_the_ordering()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).LastAsync(Token)).ShouldBe("C");
        (await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x).LastOrDefaultAsync(Token))
            .ShouldBe("C");
    }

    // ---- aggregates over the groups -----------------------------------------------------------------

    [Fact]
    public async Task an_aggregate_over_a_grouped_scalar()
    {
        await SeedAsync();

        var grouped = Targets().GroupBy(x => x.Name).Select(g => g.Count());

        (await grouped.MaxAsync(x => x, Token)).ShouldBe(3);
        (await grouped.MinAsync(x => x, Token)).ShouldBe(1);
    }

    [Fact]
    public async Task an_aggregate_over_a_projected_member()
    {
        await SeedAsync();

        var grouped = Targets().GroupBy(x => x.Name).Select(g => new { g.Key, Total = g.Sum(x => x.Age) });

        (await grouped.MaxAsync(x => x.Total, Token)).ShouldBe(15);
        (await grouped.SumAsync(x => x.Total, Token)).ShouldBe(21);
        (await grouped.AverageAsync(x => x.Total, Token)).ShouldBe(7d);
    }

    // ---- HAVING against more than a bare aggregate ----------------------------------------------------

    [Fact]
    public async Task where_against_the_key()
    {
        await SeedAsync();

        (await Targets().GroupBy(x => x.Name).Where(g => g.Key == "B").Select(g => new { g.Key, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem().C.ShouldBe(2);
    }

    [Fact]
    public async Task two_wheres_each_become_having_clauses()
    {
        await SeedAsync();

        // Each lambda has a parameter of its own, and the second used to be resolved against the first's.
        var keys = await Targets().GroupBy(x => x.Name)
            .Where(g => g.Count() > 1)
            .Where(g => g.Max(x => x.Age) < 6)
            .Select(g => g.Key).ToListAsync(Token);

        keys.ShouldBe(["B"]);
    }

    [Fact]
    public async Task where_against_a_projected_member()
    {
        await SeedAsync();

        var rows = await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
            .Where(x => x.C >= 2).OrderBy(x => x.Key).ToListAsync(Token);

        rows.Select(x => x.Key).ShouldBe(["B", "C"]);
    }

    [Fact]
    public async Task where_against_a_null_key()
    {
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = null, Age = 1 });
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Name = "x", Age = 1 });
            await session.SaveChangesAsync(Token);
        }

        (await Targets().GroupBy(x => x.Name).Where(g => g.Key == null).Select(g => g.Count()).ToListAsync(Token))
            .ShouldBe([1]);
    }

    [Fact]
    public async Task where_against_an_enum_key_stored_as_a_string()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(enumStorage: Weasel.Core.EnumStorage.AsString));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Color = TargetColor.Blue });
            session.Store(new LinqTarget { Id = Guid.NewGuid(), Color = TargetColor.Red });
            await session.SaveChangesAsync(Token);
        }

        (await Targets().GroupBy(x => x.Color).Where(g => g.Key == TargetColor.Blue).Select(g => g.Key)
            .ToListAsync(Token)).ShouldHaveSingleItem().ShouldBe(TargetColor.Blue);
    }

    // ---- other ways of reading a grouped query ------------------------------------------------------

    [Fact]
    public async Task to_json_array_streams_a_projection()
    {
        await SeedAsync();

        var json = await Targets().GroupBy(x => x.Name).Select(g => new { Name = g.Key, Count = g.Count() })
            .OrderBy(x => x.Name).ToJsonArrayAsync(Token);

        using var parsed = JsonDocument.Parse(json);
        parsed.RootElement.GetArrayLength().ShouldBe(3);
        parsed.RootElement[1].GetProperty("name").GetString().ShouldBe("B");
        parsed.RootElement[1].GetProperty("count").GetInt32().ShouldBe(2);
    }

    [Fact]
    public async Task a_paged_list_of_groups()
    {
        await SeedAsync();

        var page = await Targets().GroupBy(x => x.Name).Select(g => g.Key).OrderBy(x => x)
            .ToPagedListAsync(2, 2, Token);

        page.TotalItemCount.ShouldBe(3);
        page.ShouldHaveSingleItem().ShouldBe("C");
    }

    [Fact]
    public async Task to_json_array_refuses_a_scalar_projection()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => g.Key).ToJsonArrayAsync(Token));
    }

    [Fact]
    public async Task to_sql_describes_the_grouped_query()
    {
        await using var query = theStore.QuerySession();

        var sql = query.ToSql(query.Query<LinqTarget>().GroupBy(x => x.Name).Select(g => g.Key));

        sql.ShouldContain("GROUP BY");
    }

    [Fact]
    public async Task a_cursor_page_of_groups_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => new { g.Key }).ToJsonPageByCursorAsync(null, 10, Token));
    }

    [Fact]
    public async Task a_paged_stream_of_groups_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
        {
            using var stream = new MemoryStream();
            await Targets().GroupBy(x => x.Name).Select(g => new { g.Key })
                .StreamPagedJsonArray(1, 10, stream, Token);
        });
    }

    [Fact]
    public async Task first_with_version_of_groups_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => new { g.Key }).ToJsonFirstWithVersionAsync(Token));
    }

    [Fact]
    public async Task a_group_by_in_a_batch_is_refused()
    {
        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();

        Should.Throw<BadLinqExpressionException>(() => batch.QueryByPlan(new GroupedPlan()));
    }

    private class GroupedPlan : QueryListPlan<string>
    {
        public override IQueryable<string> Query(IQuerySession session)
            => session.Query<LinqTarget>().GroupBy(x => x.Name!).Select(g => g.Key);
    }

    // ---- shapes with no faithful translation -----------------------------------------------------------

    [Fact]
    public async Task a_second_select_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
                .Select(x => x.C).ToListAsync(Token));
    }

    [Fact]
    public async Task distinct_by_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).Select(g => new { g.Key, C = g.Count() })
                .DistinctBy(x => x.C).ToListAsync(Token));
    }

    [Fact]
    public async Task a_select_in_front_of_the_group_by_is_refused()
    {
        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().Select(x => new { N = x.Name }).GroupBy(x => x.N).Select(g => g.Key).ToListAsync(Token));
    }

    [Fact]
    public async Task a_group_by_with_no_select_is_refused_where_it_needs_one()
    {
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Targets().GroupBy(x => x.Name).ToListAsync(Token));

        ex.Message.ShouldContain("Select()");
    }
}
