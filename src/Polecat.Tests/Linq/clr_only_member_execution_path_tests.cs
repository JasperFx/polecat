using System.Text.Json;
using JasperFx.Events;
using Polecat.Batching;
using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.TestUtils;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ClrOnlyPathDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int? N { get; set; }
    public DateTimeOffset Created { get; set; }
    public List<string> Items { get; set; } = [];
}

public class ClrOnlyPathChild
{
    public Guid Id { get; set; }
    public int Size { get; set; }
}

public record ClrOnlyPathEvent(string Name, int? N, List<string> Items, DateTimeOffset At);

public class ClrOnlyPathPlan : QueryListPlan<ClrOnlyPathDoc>
{
    public override IQueryable<ClrOnlyPathDoc> Query(IQuerySession session)
        => session.Query<ClrOnlyPathDoc>().Where(x => x.N.HasValue == false && x.Items.Count == 0);
}

/// <summary>
///     #840: one rule in the member resolvers, so these run the shapes down every way of executing a
///     query. A member is translated the same way wherever it is written, or it is refused there.
/// </summary>
public class clr_only_member_execution_path_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static readonly ClrOnlyPathDoc Ann = new()
    {
        Id = Guid.NewGuid(),
        Name = "Ann",
        N = 5,
        Created = new DateTimeOffset(2024, 1, 1, 1, 0, 0, TimeSpan.FromHours(5)),
        Items = ["a", "b", "c"]
    };

    private static readonly ClrOnlyPathDoc Bob = new()
    {
        Id = Guid.NewGuid(),
        Name = "Bob",
        Created = new DateTimeOffset(2024, 6, 1, 1, 0, 0, TimeSpan.Zero)
    };

    private static readonly ClrOnlyPathDoc Cal = new()
    {
        Id = Guid.NewGuid(),
        Name = "Calvin",
        N = 7,
        Created = new DateTimeOffset(2025, 6, 1, 1, 0, 0, TimeSpan.Zero),
        Items = ["x"]
    };

    private async Task<IQuerySession> SeededAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Ann, Bob, Cal);
            await session.SaveChangesAsync(Token);
        }

        return theStore.QuerySession();
    }

    // ── Select ─────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task select_reads_each_translation_per_row()
    {
        await using var query = await SeededAsync();
        var ordered = query.Query<ClrOnlyPathDoc>().OrderBy(x => x.Name);

        (await ordered.Select(x => x.Items.Count).ToListAsync(Token)).ShouldBe([3, 0, 1]);
        (await ordered.Select(x => x.Created.Year).ToListAsync(Token)).ShouldBe([2024, 2024, 2025]);
        (await ordered.Select(x => x.Name.Length).ToListAsync(Token)).ShouldBe([3, 3, 6]);
    }

    [Fact]
    public async Task select_of_nullable_value_reads_the_stored_value()
    {
        await using var query = await SeededAsync();

        var rows = await query.Query<ClrOnlyPathDoc>().Where(x => x.N.HasValue).OrderBy(x => x.Name)
            .Select(x => x.N!.Value).ToListAsync(Token);

        rows.ShouldBe([5, 7]);
    }

    // ── OrderBy ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task order_by_each_translation()
    {
        await using var query = await SeededAsync();

        async Task<string[]> Names(IQueryable<ClrOnlyPathDoc> ordered)
            => (await ordered.ToListAsync(Token)).Select(x => x.Name).ToArray();

        (await Names(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Items.Count))).ShouldBe(["Ann", "Calvin", "Bob"]);
        (await Names(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Name.Length))).First().ShouldBe("Calvin");
        (await Names(query.Query<ClrOnlyPathDoc>().OrderBy(x => x.N.HasValue).ThenBy(x => x.Name))).ShouldBe(["Bob", "Ann", "Calvin"]);
        (await Names(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.N!.Value))).ShouldBe(["Calvin", "Ann", "Bob"]);
        (await Names(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Created.Year).ThenBy(x => x.Name))).First().ShouldBe("Calvin");
    }

    // ── GroupBy ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task group_by_a_translated_key()
    {
        await using var query = await SeededAsync();

        var years = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created.Year)
            .Select(g => new ClrOnlyKeyCount { Year = g.Key, Count = g.Count() })
            .OrderBy(x => x.Year).ToListAsync(Token);

        years.Select(x => (x.Year, x.Count)).ShouldBe([(2024, 2), (2025, 1)]);

        var lengths = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Name.Length)
            .Select(g => g.Key).OrderBy(x => x).ToListAsync(Token);

        lengths.ShouldBe([3, 6]);
    }

    // #835 reads a string in full, which runs past 4000 characters, and which is a subquery on nvarchar(max)
    // storage. So Length has to be right past the limit, and still be a legal GROUP BY key.
    private async Task<IQuerySession> SeededWithLongNameAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Ann, new ClrOnlyPathDoc { Id = Guid.NewGuid(), Name = new string('x', 5000), N = 9 });
            await session.SaveChangesAsync(Token);
        }

        return theStore.QuerySession();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_length_past_4000_characters_is_filtered_on(bool nativeJson)
    {
        await using var query = await SeededWithLongNameAsync(nativeJson);

        (await query.Query<ClrOnlyPathDoc>().Where(x => x.Name.Length == 5000).Select(x => x.N).ToListAsync(Token))
            .ShouldBe([9]);
        (await query.Query<ClrOnlyPathDoc>().Where(x => x.Name.Length > 4000).Select(x => x.N).ToListAsync(Token))
            .ShouldBe([9]);
        (await query.Query<ClrOnlyPathDoc>().Where(x => x.Name.Length != 5000).Select(x => x.N).ToListAsync(Token))
            .ShouldBe([Ann.N]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_length_past_4000_characters_is_selected_and_ordered_by(bool nativeJson)
    {
        await using var query = await SeededWithLongNameAsync(nativeJson);

        (await query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Name.Length).Select(x => x.Name.Length)
                .ToListAsync(Token))
            .ShouldBe([5000, 3]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_length_past_4000_characters_is_aggregated(bool nativeJson)
    {
        await using var query = await SeededWithLongNameAsync(nativeJson);

        (await query.Query<ClrOnlyPathDoc>().MaxAsync(x => x.Name.Length, Token)).ShouldBe(5000);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_length_is_a_group_key_on_either_storage(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Ann, Bob, Cal);
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        var lengths = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Name.Length)
            .Select(g => new ClrOnlyKeyCount { Year = g.Key, Count = g.Count() })
            .OrderBy(x => x.Year).ToListAsync(Token);

        lengths.Select(x => (x.Year, x.Count)).ShouldBe([(3, 2), (6, 1)]);
    }

    [RequiresNativeJsonFact(true)]
    public async Task a_length_past_4000_characters_is_a_group_key_on_native_storage()
    {
        await using var query = await SeededWithLongNameAsync(nativeJson: true);

        var lengths = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Name.Length)
            .Select(g => g.Key).OrderBy(x => x).ToListAsync(Token);

        lengths.ShouldBe([3, 5000]);
    }

    [Fact]
    public async Task group_by_has_value()
    {
        await using var query = await SeededAsync();

        var groups = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.N.HasValue)
            .Select(g => new ClrOnlyFlagCount { Present = g.Key, Count = g.Count() }).ToListAsync(Token);

        groups.Single(x => x.Present).Count.ShouldBe(2);
        groups.Single(x => !x.Present).Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_group_aggregate_over_a_translated_member()
    {
        await using var query = await SeededAsync();

        var max = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created.Year)
            .Select(g => g.Max(x => x.N!.Value)).OrderBy(x => x).ToListAsync(Token);

        max.ShouldBe([5, 7]);
    }

    [Fact]
    public async Task a_group_count_predicate_over_has_value()
    {
        await using var query = await SeededAsync();

        var counts = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created.Year)
            .Select(g => g.Count(x => x.N.HasValue == false)).OrderBy(x => x).ToListAsync(Token);

        counts.ShouldBe([0, 1]);
    }

    [Fact]
    public async Task a_having_guard_on_a_captured_nullable()
    {
        await using var query = await SeededAsync();

        int? empty = null;
        var all = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created.Year)
            .Where(g => !empty.HasValue || g.Count() > empty.Value)
            .Select(g => g.Key).OrderBy(x => x).ToListAsync(Token);

        all.ShouldBe([2024, 2025]);

        int? one = 1;
        var some = await query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created.Year)
            .Where(g => !one.HasValue || g.Count() > one.Value)
            .Select(g => g.Key).ToListAsync(Token);

        some.ShouldBe([2024]);
    }

    [Fact]
    public async Task the_value_of_a_nullable_group_key_is_the_key()
    {
        await using var query = await SeededAsync();

        var keys = await query.Query<ClrOnlyPathDoc>().Where(x => x.N.HasValue).GroupBy(x => x.N)
            .Select(g => g.Key!.Value).OrderBy(x => x).ToListAsync(Token);

        keys.ShouldBe([5, 7]);
    }

    [Fact]
    public async Task a_member_of_a_single_value_group_key_is_refused_by_name()
    {
        await using var query = await SeededAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Created)
                .Select(g => new { g.Key.Year, Count = g.Count() }).ToListAsync(Token));

        ex.Message.ShouldContain("Year");
        ex.Message.ShouldNotContain("composite");
    }

    [Fact]
    public async Task a_collection_count_is_refused_where_sql_server_forbids_a_subquery()
    {
        await using var query = await SeededAsync();

        var key = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Items.Count)
                .Select(g => new { g.Key, Count = g.Count() }).ToListAsync(Token));
        key.Message.ShouldContain("GroupBy key");

        var composite = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyPathDoc>().GroupBy(x => new { x.Name, x.Items.Count })
                .Select(g => new { g.Key.Name, Rows = g.Count() }).ToListAsync(Token));
        composite.Message.ShouldContain("GroupBy key");

        var grouped = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyPathDoc>().GroupBy(x => x.Name)
                .Select(g => new { g.Key, Total = g.Sum(x => x.Items.Count) }).ToListAsync(Token));
        grouped.Message.ShouldContain("inside Sum()");
    }

    // ── Aggregates, Any, First, Distinct ───────────────────────────────────────────────────────────

    [Fact]
    public async Task an_aggregate_over_a_translated_member()
    {
        await using var query = await SeededAsync();

        (await query.Query<ClrOnlyPathDoc>().SumAsync(x => x.N!.Value, Token)).ShouldBe(12);
        (await query.Query<ClrOnlyPathDoc>().MaxAsync(x => x.Created.Year, Token)).ShouldBe(2025);
    }

    [Fact]
    public async Task an_aggregate_over_a_collection_count_runs_over_a_derived_table()
    {
        await using var query = await SeededAsync();

        var docs = query.Query<ClrOnlyPathDoc>();

        (await docs.SumAsync(x => x.Items.Count, Token)).ShouldBe(4);
        (await docs.MaxAsync(x => x.Items.Count, Token)).ShouldBe(3);
        (await docs.MinAsync(x => x.Items.Count, Token)).ShouldBe(0);
        (await docs.AverageAsync(x => x.Items.Count, Token)).ShouldBe(4d / 3, 0.0001);
    }

    [Fact]
    public async Task an_aggregate_over_a_collection_count_keeps_its_filter_and_its_page()
    {
        await using var query = await SeededAsync();

        var docs = query.Query<ClrOnlyPathDoc>();

        (await docs.Where(x => x.N.HasValue).SumAsync(x => x.Items.Count, Token)).ShouldBe(4);
        (await docs.Where(x => !x.N.HasValue).SumAsync(x => x.Items.Count, Token)).ShouldBe(0);
        (await docs.OrderBy(x => x.Name).SumAsync(x => x.Items.Count, Token)).ShouldBe(4);
        (await docs.OrderBy(x => x.Name).Take(2).SumAsync(x => x.Items.Count, Token)).ShouldBe(3);
    }

    [Fact]
    public async Task terminal_operators_with_an_inline_predicate()
    {
        await using var query = await SeededAsync();

        (await query.Query<ClrOnlyPathDoc>().CountAsync(x => x.Items.Count > 0, Token)).ShouldBe(2);
        (await query.Query<ClrOnlyPathDoc>().AnyAsync(x => x.N!.Value == 7, Token)).ShouldBeTrue();
        (await query.Query<ClrOnlyPathDoc>().FirstOrDefaultAsync(x => x.N.HasValue == false, Token))!.Name.ShouldBe("Bob");
    }

    [Fact]
    public async Task distinct_by_a_translated_key()
    {
        await using var query = await SeededAsync();

        (await query.Query<ClrOnlyPathDoc>().DistinctBy(x => x.Created.Year).ToListAsync(Token)).Count.ShouldBe(2);
        (await query.Query<ClrOnlyPathDoc>().DistinctBy(x => x.N.HasValue).ToListAsync(Token)).Count.ShouldBe(2);
        (await query.Query<ClrOnlyPathDoc>().DistinctBy(x => x.Items.Count).ToListAsync(Token)).Count.ShouldBe(3);
    }

    // ── The raw JSON paths ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task to_json_array_with_a_projection_of_translated_members()
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");

        await using var query = await SeededAsync();

        var json = await query.Query<ClrOnlyPathDoc>().OrderBy(x => x.Name)
            .Select(x => new { x.Name, Present = x.N.HasValue, Elements = x.Items.Count, x.Created.Year, Value = x.N!.Value })
            .ToJsonArrayAsync(Token);

        using var document = JsonDocument.Parse(json);
        var ann = document.RootElement[0];
        ann.GetProperty("present").GetBoolean().ShouldBeTrue();
        ann.GetProperty("elements").GetInt32().ShouldBe(3);
        ann.GetProperty("year").GetInt32().ShouldBe(2024);
        ann.GetProperty("value").GetInt32().ShouldBe(5);

        var bob = document.RootElement[1];
        bob.GetProperty("present").GetBoolean().ShouldBeFalse();
        bob.GetProperty("elements").GetInt32().ShouldBe(0);
    }

    [Fact]
    public async Task to_json_array_filtered_by_translated_members()
    {
        await using var query = await SeededAsync();

        var json = await query.Query<ClrOnlyPathDoc>().Where(x => x.Items.Count > 1).ToJsonArrayAsync(Token);

        using var document = JsonDocument.Parse(json);
        document.RootElement.GetArrayLength().ShouldBe(1);
    }

    [Fact]
    public async Task stream_paged_with_a_projection_of_translated_members()
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");

        await using var query = await SeededAsync();

        using var stream = new MemoryStream();
        await query.Query<ClrOnlyPathDoc>().Where(x => x.N.HasValue == false).OrderBy(x => x.Name)
            .Select(x => new { x.Name, Elements = x.Items.Count })
            .StreamPagedJsonArray(1, 10, stream, Token);

        stream.Position = 0;
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: Token);
        document.RootElement.GetProperty("totalItemCount").GetInt32().ShouldBe(1);
        document.RootElement.GetProperty("items")[0].GetProperty("elements").GetInt32().ShouldBe(0);
    }

    [Fact]
    public async Task an_untranslatable_member_in_a_streamed_projection_is_refused()
    {
        await using var query = await SeededAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyPathDoc>().Select(x => new { x.Created.Ticks }).ToJsonArrayAsync(Token));

        // Without native json no projection can be streamed at all, which is refused before the member is read
        if (ConnectionSource.SupportsNativeJson)
        {
            ex.Message.ShouldContain("exists only in .NET");
        }
    }

    [Fact]
    public async Task an_in_memory_projection_is_untouched_because_nothing_is_translated()
    {
        await using var query = await SeededAsync();

        var rows = await query.Query<ClrOnlyPathDoc>().OrderBy(x => x.Name)
            .Select(x => new { x.Created.Ticks }).ToListAsync(Token);

        rows.Select(x => x.Ticks).ShouldBe([Ann.Created.Ticks, Bob.Created.Ticks, Cal.Created.Ticks]);
    }

    // ── Cursor paging ──────────────────────────────────────────────────────────────────────────────

    private static async Task<List<string>> EveryPageAsync(IQueryable<ClrOnlyPathDoc> ordered)
    {
        var names = new List<string>();
        string? cursor = null;

        do
        {
            var page = await ordered.ToJsonPageByCursorAsync(cursor, 1, Token);
            using var document = JsonDocument.Parse(page.ItemsJson);
            names.AddRange(document.RootElement.EnumerateArray().Select(x => x.GetProperty("name").GetString()!));
            cursor = page.NextCursor;
        } while (cursor != null);

        return names;
    }

    [Fact]
    public async Task cursor_paging_over_a_collection_count()
    {
        await using var query = await SeededAsync();

        (await EveryPageAsync(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Items.Count).ThenBy(x => x.Id)))
            .ShouldBe(["Ann", "Calvin", "Bob"]);
    }

    [Fact]
    public async Task cursor_paging_over_has_value()
    {
        await using var query = await SeededAsync();

        (await EveryPageAsync(query.Query<ClrOnlyPathDoc>().OrderBy(x => x.N.HasValue).ThenBy(x => x.Id)))
            .First().ShouldBe("Bob");
    }

    [Fact]
    public async Task cursor_paging_over_a_nullable_value_and_a_date_part()
    {
        await using var query = await SeededAsync();

        (await EveryPageAsync(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.N!.Value).ThenBy(x => x.Id)))
            .ShouldBe(["Calvin", "Ann", "Bob"]);

        (await EveryPageAsync(query.Query<ClrOnlyPathDoc>().OrderByDescending(x => x.Created.Year).ThenBy(x => x.Id)))
            .First().ShouldBe("Calvin");
    }

    // ── Joins ──────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task a_join_keyed_and_filtered_by_translated_members()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Ann, Bob, Cal);
            session.Store(new ClrOnlyPathChild { Id = Guid.NewGuid(), Size = 3 });
            session.Store(new ClrOnlyPathChild { Id = Guid.NewGuid(), Size = 1 });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Query<ClrOnlyPathDoc>()
            .Where(x => x.N.HasValue && x.Items.Count > 0)
            .GroupJoin(query.Query<ClrOnlyPathChild>(), x => x.Items.Count, c => c.Size, (x, cs) => new { x, cs })
            .SelectMany(t => t.cs, (t, c) => new { t.x.Name, c.Size })
            .ToListAsync(Token);

        rows.OrderBy(x => x.Name).Select(x => (x.Name, x.Size)).ShouldBe([("Ann", 3), ("Calvin", 1)]);
    }

    // ── Batches and plans ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task a_batched_queryable()
    {
        await using var query = await SeededAsync();

        var batch = query.CreateBatchQuery();
        var count = batch.Query<ClrOnlyPathDoc>().Where(x => x.Items.Count > 0).Count();
        var byValue = batch.Query<ClrOnlyPathDoc>().Where(x => x.N!.Value == 5).ToList();

        int? empty = null;
        var guarded = batch.Query<ClrOnlyPathDoc>().Where(x => !empty.HasValue || x.N != empty).Count();
        await batch.Execute(Token);

        (await count).ShouldBe(2);
        (await byValue).Single().Name.ShouldBe("Ann");
        (await guarded).ShouldBe(3);
    }

    [Fact]
    public async Task a_query_plan_on_its_own_and_in_a_batch()
    {
        await using var query = await SeededAsync();

        (await query.QueryByPlanAsync(new ClrOnlyPathPlan(), Token)).Single().Name.ShouldBe("Bob");

        var batch = query.CreateBatchQuery();
        var planned = batch.QueryByPlan(new ClrOnlyPathPlan());
        await batch.Execute(Token);

        (await planned).Single().Name.ShouldBe("Bob");
    }

    // ── Writes that take a predicate ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task delete_where_reaches_the_rows_it_names()
    {
        await using var query = await SeededAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.DeleteWhere<ClrOnlyPathDoc>(x => x.N!.Value == 5);
            session.DeleteWhere<ClrOnlyPathDoc>(x => x.Items.Count == 0);
            await session.SaveChangesAsync(Token);
        }

        (await query.Query<ClrOnlyPathDoc>().ToListAsync(Token)).Single().Name.ShouldBe("Calvin");
    }

    // ── ToSql ──────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task to_sql_shows_the_translation_rather_than_a_member_of_the_clr_type()
    {
        await using var query = await SeededAsync();

        var sql = query.ToSql(query.Query<ClrOnlyPathDoc>().Where(x => x.N!.Value == 5 && x.Items.Count > 0));

        sql.ShouldContain("$.n'");
        sql.ShouldNotContain("$.n.value");
        sql.ShouldContain("OPENJSON(data, '$.items')");
        sql.ShouldNotContain("$.items.count");
    }

    // ── Event queries ──────────────────────────────────────────────────────────────────────────────

    private async Task<IQuerySession> SeededEventsAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(),
                new ClrOnlyPathEvent("one", 5, ["a", "b"], new DateTimeOffset(2024, 1, 1, 1, 0, 0, TimeSpan.FromHours(5))));
            session.Events.StartStream(Guid.NewGuid(),
                new ClrOnlyPathEvent("two", null, [], new DateTimeOffset(2025, 6, 1, 1, 0, 0, TimeSpan.Zero)));
            await session.SaveChangesAsync(Token);
        }

        return theStore.QuerySession();
    }

    [Fact]
    public async Task event_data_queries_follow_the_same_rule()
    {
        await using var query = await SeededEventsAsync();

        async Task<string> Single(System.Linq.Expressions.Expression<Func<ClrOnlyPathEvent, bool>> predicate)
            => (await query.Events.QueryRawEventDataOnly<ClrOnlyPathEvent>().Where(predicate).ToListAsync(Token)).Single().Name;

        (await Single(x => x.N!.Value == 5)).ShouldBe("one");
        (await Single(x => x.N.HasValue == false)).ShouldBe("two");
        (await Single(x => x.Items.Count == 2)).ShouldBe("one");
        (await Single(x => x.At.Year == 2024)).ShouldBe("one");
        (await Single(x => x.Name.Length == 3 && x.Name == "two")).ShouldBe("two");

        (await query.Events.QueryRawEventDataOnly<ClrOnlyPathEvent>().SumAsync(x => x.Items.Count, Token)).ShouldBe(2);
    }

    [Fact]
    public async Task event_data_queries_refuse_the_same_members()
    {
        await using var query = await SeededEventsAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryRawEventDataOnly<ClrOnlyPathEvent>().Where(x => x.At.Ticks > 0).ToListAsync(Token));

        ex.Message.ShouldContain("exists only in .NET");
    }

    [Fact]
    public async Task raw_event_columns_translate_and_are_not_looked_up_by_a_nested_name()
    {
        await using var query = await SeededEventsAsync();

        var thisYearAndLater = await query.Events.QueryAllRawEvents()
            .Where(x => x.Timestamp.Year > 2000).ToListAsync(Token);
        thisYearAndLater.Count.ShouldBe(2);

        // Version is a column of the table, and this is the version of a Guid.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryAllRawEvents().Where(x => x.Id.Version > 0).ToListAsync(Token));
    }

    [Fact]
    public async Task stream_state_columns_translate_and_are_not_looked_up_by_a_nested_name()
    {
        await using var query = await SeededEventsAsync();

        var states = ((IEventStore)theStore).OpenReadOnlyEventStore().QueryStreamStates();

        (await states.Where(x => x.Created.Year > 2000).ToListAsync(Token)).Count.ShouldBe(2);

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            states.Where(x => x.Id.Version > 0).ToListAsync(Token));
    }
}

public class ClrOnlyKeyCount
{
    public int Year { get; set; }
    public int Count { get; set; }
}

public class ClrOnlyFlagCount
{
    public bool Present { get; set; }
    public int Count { get; set; }
}
