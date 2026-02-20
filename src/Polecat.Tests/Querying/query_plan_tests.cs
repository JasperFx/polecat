using Polecat.Batching;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Querying;

[Collection("integration")]
public class query_plan_tests : IntegrationContext
{
    public query_plan_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<DocumentStore> CreateStore()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "query_plan_tests";
        });
        await theStore.Advanced.CleanAsync<Target>();
        return theStore;
    }

    [Fact]
    public async Task query_by_plan_returns_matching_documents()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 2 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green", Number = 3 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var results = await query.QueryByPlanAsync(new ColorTargetsPlan("Blue"));

        results.Count.ShouldBe(2);
        results.ShouldAllBe(t => t.Color == "Blue");
    }

    [Fact]
    public async Task query_by_plan_with_ordering()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 30 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 10 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 20 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var results = await query.QueryByPlanAsync(new ColorTargetsPlan("Red"));

        results.Count.ShouldBe(3);
        results[0].Number.ShouldBe(10);
        results[1].Number.ShouldBe(20);
        results[2].Number.ShouldBe(30);
    }

    [Fact]
    public async Task query_by_plan_returns_empty_when_no_matches()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var results = await query.QueryByPlanAsync(new ColorTargetsPlan("Yellow"));

        results.ShouldBeEmpty();
    }

    [Fact]
    public async Task custom_query_plan_with_scalar_result()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 5 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 15 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green", Number = 25 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var count = await query.QueryByPlanAsync(new CountByColorPlan("Blue"));

        count.ShouldBe(2);
    }

    [Fact]
    public async Task query_by_plan_on_document_session()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green", Number = 7 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green", Number = 3 });
        await session.SaveChangesAsync();

        // QueryByPlanAsync works on IDocumentSession too (inherits from IQuerySession)
        var results = await session.QueryByPlanAsync(new ColorTargetsPlan("Green"));

        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task batch_query_by_plan_with_manual_implementation()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 1 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 2 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Green", Number = 3 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var batch = query.CreateBatchQuery();

        var blueFetcher = batch.QueryByPlan(new ManualBatchColorPlan("Blue"));
        var greenFetcher = batch.QueryByPlan(new ManualBatchColorPlan("Green"));

        await batch.Execute();

        var blues = await blueFetcher;
        var greens = await greenFetcher;

        blues.Count.ShouldBe(2);
        greens.Count.ShouldBe(1);
    }

    [Fact]
    public async Task batch_query_by_plan_with_query_list_plan()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 10 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = 20 });
        session.Store(new Target { Id = Guid.NewGuid(), Color = "Red", Number = 30 });
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var batch = query.CreateBatchQuery();

        // QueryListPlan works with batch via the parent session
        var blueFetcher = batch.QueryByPlan(new ColorTargetsPlan("Blue"));
        var redFetcher = batch.QueryByPlan(new ColorTargetsPlan("Red"));

        await batch.Execute();

        var blues = await blueFetcher;
        var reds = await redFetcher;

        blues.Count.ShouldBe(2);
        reds.Count.ShouldBe(1);
        reds[0].Number.ShouldBe(30);
    }

    [Fact]
    public async Task query_plan_with_take_and_skip()
    {
        var store = await CreateStore();
        await using var session = store.LightweightSession();

        for (int i = 1; i <= 10; i++)
        {
            session.Store(new Target { Id = Guid.NewGuid(), Color = "Blue", Number = i });
        }
        await session.SaveChangesAsync();

        await using var query = store.QuerySession();
        var results = await query.QueryByPlanAsync(new PagedColorPlan("Blue", skip: 2, take: 3));

        results.Count.ShouldBe(3);
        results[0].Number.ShouldBe(3);
        results[1].Number.ShouldBe(4);
        results[2].Number.ShouldBe(5);
    }
}

/// <summary>
///     QueryListPlan: specification pattern that returns a filtered, ordered list.
/// </summary>
public class ColorTargetsPlan : QueryListPlan<Target>
{
    public string Color { get; }

    public ColorTargetsPlan(string color)
    {
        Color = color;
    }

    public override IQueryable<Target> Query(IQuerySession session)
    {
        return session.Query<Target>()
            .Where(x => x.Color == Color)
            .OrderBy(x => x.Number);
    }
}

/// <summary>
///     Custom IQueryPlan returning a scalar (count).
/// </summary>
public class CountByColorPlan : IQueryPlan<int>
{
    public string Color { get; }

    public CountByColorPlan(string color)
    {
        Color = color;
    }

    public async Task<int> Fetch(IQuerySession session, CancellationToken token)
    {
        return await session.Query<Target>()
            .Where(x => x.Color == Color)
            .CountAsync(token);
    }
}

/// <summary>
///     Manual IBatchQueryPlan that uses the batch's Query API directly.
/// </summary>
public class ManualBatchColorPlan : IBatchQueryPlan<IReadOnlyList<Target>>
{
    public string Color { get; }

    public ManualBatchColorPlan(string color)
    {
        Color = color;
    }

    public Task<IReadOnlyList<Target>> Fetch(IBatchedQuery query)
    {
        return query.Query<Target>()
            .Where(x => x.Color == Color)
            .ToList();
    }
}

/// <summary>
///     QueryListPlan with pagination.
/// </summary>
public class PagedColorPlan : QueryListPlan<Target>
{
    public string Color { get; }
    public int Skip { get; }
    public int Take { get; }

    public PagedColorPlan(string color, int skip, int take)
    {
        Color = color;
        Skip = skip;
        Take = take;
    }

    public override IQueryable<Target> Query(IQuerySession session)
    {
        return session.Query<Target>()
            .Where(x => x.Color == Color)
            .OrderBy(x => x.Number)
            .Skip(Skip)
            .Take(Take);
    }
}
