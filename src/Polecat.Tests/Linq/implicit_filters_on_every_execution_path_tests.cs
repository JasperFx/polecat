using System.Linq.Expressions;
using System.Text.Json;
using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Linq.SoftDeletes;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ScopedDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class ScopedBase
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Number { get; set; }
}

public class ScopedSubA : ScopedBase;

public class ScopedSubB : ScopedBase;

/// <summary>
///     #836: the implicit filters are decided in one place, and every way of running a query goes
///     through it. These run the same query down each path and require the same answer, because the
///     failure this guards against is a path that quietly returns more than the others.
/// </summary>
public class implicit_filters_on_every_execution_path_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <summary>
    ///     How many rows <paramref name="query" /> yields down each path that returns rows. Each path is
    ///     named so that a failure says which one disagreed.
    /// </summary>
    private static async Task<Dictionary<string, int>> ThroughEveryPathAsync<T>(
        IQueryable<T> query, Func<IQueryable<T>, IQueryable<T>> ordered, Expression<Func<T, string>> key)
    {
        var page = await ordered(query).ToJsonPageByCursorAsync(null, 100, Token);

        using var stream = new MemoryStream();
        await ordered(query).StreamPagedJsonArray(1, 100, stream, Token);
        stream.Position = 0;
        using var paged = await JsonDocument.ParseAsync(stream, cancellationToken: Token);

        using var array = JsonDocument.Parse(await query.ToJsonArrayAsync(Token));

        return new Dictionary<string, int>
        {
            ["ToListAsync"] = (await query.ToListAsync(Token)).Count,
            ["CountAsync"] = await query.CountAsync(Token),
            ["ToJsonArrayAsync"] = array.RootElement.GetArrayLength(),
            ["ToJsonPageByCursorAsync"] = page.Count,
            ["StreamPagedJsonArray"] = paged.RootElement.GetProperty("totalItemCount").GetInt32(),
            ["GroupBy"] = (await query.GroupBy(key).Select(g => g.Count()).ToListAsync(Token)).Sum()
        };
    }

    private static void ShouldAllBe(Dictionary<string, int> counts, int expected)
    {
        foreach (var (path, count) in counts)
        {
            count.ShouldBe(expected, path);
        }
    }

    private async Task SeedHierarchyAsync()
    {
        ConfigureStore(opts => opts.Schema.For<ScopedBase>().AddSubClass<ScopedSubA>().AddSubClass<ScopedSubB>());
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await using var session = theStore.LightweightSession();
        session.Store<ScopedBase>(new ScopedSubA { Id = Guid.NewGuid(), Name = "a", Number = 1 });
        session.Store<ScopedBase>(new ScopedSubB { Id = Guid.NewGuid(), Name = "b", Number = 2 });
        await session.SaveChangesAsync(Token);
    }

    private async Task<Guid> SeedSoftDeletedAsync()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsSoftDeleted());
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        var deleted = new ScopedDoc { Id = Guid.NewGuid(), Name = "A" };
        await using var session = theStore.LightweightSession();
        session.Store(deleted);
        session.Store(new ScopedDoc { Id = Guid.NewGuid(), Name = "A" });
        await session.SaveChangesAsync(Token);

        session.Delete(deleted);
        await session.SaveChangesAsync(Token);

        return deleted.Id;
    }

    [Fact]
    public async Task a_sub_class_query_stays_on_its_own_rows_on_every_path()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();
        var counts = await ThroughEveryPathAsync(query.Query<ScopedSubA>(),
            x => x.OrderBy(d => d.Number).ThenBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 1);
    }

    [Fact]
    public async Task a_root_query_reads_the_whole_hierarchy_on_every_path()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();
        var counts = await ThroughEveryPathAsync(query.Query<ScopedBase>(),
            x => x.OrderBy(d => d.Number).ThenBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 2);
    }

    [Fact]
    public async Task a_soft_deleted_row_is_out_of_every_path()
    {
        await SeedSoftDeletedAsync();

        await using var query = theStore.QuerySession();
        var counts = await ThroughEveryPathAsync(query.Query<ScopedDoc>(),
            x => x.OrderBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 1);
    }

    [Fact]
    public async Task maybe_deleted_brings_the_row_back_on_every_path()
    {
        await SeedSoftDeletedAsync();

        await using var query = theStore.QuerySession();
        var counts = await ThroughEveryPathAsync(query.Query<ScopedDoc>().MaybeDeleted(),
            x => x.OrderBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 2);
    }

    [Fact]
    public async Task is_deleted_narrows_every_path_to_the_deleted_row()
    {
        await SeedSoftDeletedAsync();

        await using var query = theStore.QuerySession();
        var counts = await ThroughEveryPathAsync(query.Query<ScopedDoc>().IsDeleted(),
            x => x.OrderBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 1);
    }

    [Fact]
    public async Task the_deleted_window_is_honored_on_every_path()
    {
        await SeedSoftDeletedAsync();

        await using var query = theStore.QuerySession();
        var future = DateTimeOffset.UtcNow.AddDays(1);

        // Only the main path used to read these: everywhere else DeletedSince meant IsDeleted.
        var counts = await ThroughEveryPathAsync(query.Query<ScopedDoc>().DeletedSince(future),
            x => x.OrderBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 0);
    }

    [Fact]
    public async Task the_modified_window_is_honored_on_every_path()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();
        var future = DateTimeOffset.UtcNow.AddDays(1);

        var counts = await ThroughEveryPathAsync(
            Polecat.Linq.Metadata.MetadataExtensions.ModifiedSince(query.Query<ScopedBase>(), future),
            x => x.OrderBy(d => d.Number).ThenBy(d => d.Id), x => x.Name);

        ShouldAllBe(counts, 0);
    }

    [Fact]
    public async Task the_single_document_json_path_narrows_too()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();

        var first = await query.Query<ScopedSubB>().ToJsonFirstWithVersionAsync(Token);

        first.ShouldNotBeNull();
        first.Json.ShouldContain("\"b\"");
    }

    [Fact]
    public async Task a_soft_deleted_group_disappears_with_its_last_row()
    {
        ConfigureStore(opts => opts.Policies.AllDocumentsSoftDeleted());
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        var dead = new ScopedDoc { Id = Guid.NewGuid(), Name = "gone" };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(dead);
            session.Store(new ScopedDoc { Id = Guid.NewGuid(), Name = "kept" });
            await session.SaveChangesAsync(Token);
            session.Delete(dead);
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();
        var groups = await query.Query<ScopedDoc>().GroupBy(x => x.Name)
            .Select(g => new { Name = g.Key, Count = g.Count() }).ToListAsync(Token);

        groups.ShouldHaveSingleItem().Name.ShouldBe("kept");
    }

    [Fact]
    public async Task to_sql_narrows_a_sub_class_too()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();

        query.ToSql(query.Query<ScopedSubA>()).ShouldContain("doc_type");
    }

    [Fact]
    public async Task the_batched_queryable_narrows_a_sub_class()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var rows = batch.Query<ScopedSubA>().ToList();
        var count = batch.Query<ScopedSubA>().Count();
        await batch.Execute(Token);

        (await rows).Count.ShouldBe(1);
        (await count).ShouldBe(1);
    }

    [Fact]
    public async Task a_query_list_plan_in_a_batch_narrows_a_sub_class()
    {
        await SeedHierarchyAsync();

        await using var query = theStore.QuerySession();
        var batch = query.CreateBatchQuery();
        var rows = batch.QueryByPlan(new SubClassPlan());
        await batch.Execute(Token);

        (await rows).Count.ShouldBe(1);
    }

    private class SubClassPlan : QueryListPlan<ScopedSubA>
    {
        public override IQueryable<ScopedSubA> Query(IQuerySession session) => session.Query<ScopedSubA>();
    }

    [Fact]
    public async Task a_join_can_read_a_soft_deleted_outer_row_with_maybe_deleted()
    {
        await SeedSoftDeletedAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new JoinPartner { Id = Guid.NewGuid(), Name = "A" });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Query<ScopedDoc>().MaybeDeleted()
            .GroupJoin(query.Query<JoinPartner>(), x => x.Name, x => x.Name, (x, ps) => new { x, ps })
            .SelectMany(t => t.ps, (t, p) => new { t.x.Name, Partner = p.Name })
            .ToListAsync(Token);

        // Soft deletes are configured for every document, partners included, so the partner stays put.
        rows.Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_join_narrows_both_sides_to_their_sub_classes()
    {
        await SeedHierarchyAsync();

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new JoinPartner { Id = Guid.NewGuid(), Name = "a" });
            session.Store(new JoinPartner { Id = Guid.NewGuid(), Name = "b" });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        var outer = await query.Query<ScopedSubA>()
            .GroupJoin(query.Query<JoinPartner>(), x => x.Name, x => x.Name, (x, ps) => new { x, ps })
            .SelectMany(t => t.ps, (t, p) => new { p.Name })
            .ToListAsync(Token);

        var inner = await query.Query<JoinPartner>()
            .GroupJoin(query.Query<ScopedSubB>(), x => x.Name, x => x.Name, (x, bs) => new { x, bs })
            .SelectMany(t => t.bs, (t, b) => new { b.Name })
            .ToListAsync(Token);

        outer.ShouldHaveSingleItem().Name.ShouldBe("a");
        inner.ShouldHaveSingleItem().Name.ShouldBe("b");
    }

    [Fact]
    public async Task a_filter_on_the_inner_side_of_a_join_is_honored()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ScopedDoc { Id = Guid.NewGuid(), Name = "a" });
            session.Store(new JoinPartner { Id = Guid.NewGuid(), Name = "a", Rank = 1 });
            session.Store(new JoinPartner { Id = Guid.NewGuid(), Name = "a", Rank = 2 });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Query<ScopedDoc>()
            .GroupJoin(query.Query<JoinPartner>().Where(x => x.Rank == 2), x => x.Name, x => x.Name,
                (x, ps) => new { x, ps })
            .SelectMany(t => t.ps, (t, p) => new { p.Rank })
            .ToListAsync(Token);

        rows.ShouldHaveSingleItem().Rank.ShouldBe(2);
    }

    [Fact]
    public async Task paging_the_inner_side_of_a_join_is_refused()
    {
        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<ScopedDoc>()
                .GroupJoin(query.Query<JoinPartner>().Take(1), x => x.Name, x => x.Name, (x, ps) => new { x, ps })
                .SelectMany(t => t.ps, (t, p) => new { p.Rank })
                .ToListAsync(Token));
    }

    public class JoinPartner
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
        public int Rank { get; set; }
    }
}

/// <summary>
///     The non-stale wait is the one implicit promise that is not a predicate, so it needs its own
///     proof: an async projection with no daemon running can never catch up, so a path that honors the
///     wait times out and one that skipped it returns.
/// </summary>
public class non_stale_wait_on_every_execution_path_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public record StaleEvent(Guid Id);

    public class StaleView
    {
        public Guid Id { get; set; }
        public void Apply(StaleEvent _) { }
    }

    private async Task<IQuerySession> AStoreThatIsAlwaysStaleAsync()
    {
        ConfigureStore(opts =>
        {
            opts.Projections.Add<SingleStreamProjection<StaleView, Guid>>(
                JasperFx.Events.Projections.ProjectionLifecycle.Async);
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Token);

        await using (var session = theStore.LightweightSession())
        {
            var id = Guid.NewGuid();
            session.Events.StartStream(id, new StaleEvent(id));
            await session.SaveChangesAsync(Token);
        }

        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);
        return query;
    }

    private static readonly TimeSpan Briefly = TimeSpan.FromMilliseconds(250);

    [Fact]
    public async Task the_list_path_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly).ToListAsync(Token));
    }

    [Fact]
    public async Task group_by_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly)
                .GroupBy(x => x.Name).Select(g => g.Count()).ToListAsync(Token));
    }

    [Fact]
    public async Task a_join_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly)
                .GroupJoin(query.Query<ScopedBase>(), x => x.Name, x => x.Name, (x, ps) => new { x, ps })
                .SelectMany(t => t.ps, (t, p) => new { p.Name })
                .ToListAsync(Token));
    }

    [Fact]
    public async Task the_json_array_path_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly).ToJsonArrayAsync(Token));
    }

    [Fact]
    public async Task the_first_with_version_path_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly).ToJsonFirstWithVersionAsync(Token));
    }

    [Fact]
    public async Task the_paged_stream_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
        {
            using var stream = new MemoryStream();
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly).OrderBy(x => x.Id)
                .StreamPagedJsonArray(1, 10, stream, Token);
        });
    }

    [Fact]
    public async Task the_cursor_page_waits()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        await Should.ThrowAsync<TimeoutException>(async () =>
            await query.Query<ScopedDoc>().QueryForNonStaleData(Briefly).OrderBy(x => x.Id)
                .ToJsonPageByCursorAsync(null, 10, Token));
    }

    [Fact]
    public async Task a_batch_refuses_the_wait_rather_than_skipping_it()
    {
        var query = await AStoreThatIsAlwaysStaleAsync();

        var batch = query.CreateBatchQuery();

        Should.Throw<BadLinqExpressionException>(() => batch.QueryByPlan(new StalePlan()));
    }

    private class StalePlan : QueryListPlan<ScopedDoc>
    {
        public override IQueryable<ScopedDoc> Query(IQuerySession session)
            => session.Query<ScopedDoc>().QueryForNonStaleData(Briefly);
    }
}
