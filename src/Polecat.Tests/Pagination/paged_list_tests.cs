using Polecat.Linq;
using Polecat.Pagination;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Pagination;

[Collection("integration")]
public class paged_list_tests : IntegrationContext
{
    public paged_list_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "pagination"; });

        // Clean existing data
        await theStore.Advanced.CleanAsync<Target>();

        // Seed 50 documents
        var targets = Enumerable.Range(1, 50).Select(i => new Target
        {
            Id = Guid.NewGuid(),
            Color = i <= 30 ? "Blue" : "Green",
            Number = i
        }).ToList();

        await theStore.Advanced.BulkInsertAsync(targets);
    }

    [Fact]
    public async Task returns_correct_page_of_items()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(1, 10);

        page.Count.ShouldBe(10);
        page[0].Number.ShouldBe(1);
        page[9].Number.ShouldBe(10);
    }

    [Fact]
    public async Task returns_correct_total_count()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(1, 10);

        page.TotalItemCount.ShouldBe(50);
    }

    [Fact]
    public async Task calculates_page_count()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(1, 10);

        page.PageCount.ShouldBe(5);
    }

    [Fact]
    public async Task has_next_page_on_first_page()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(1, 10);

        page.HasNextPage.ShouldBeTrue();
        page.HasPreviousPage.ShouldBeFalse();
        page.IsFirstPage.ShouldBeTrue();
        page.IsLastPage.ShouldBeFalse();
    }

    [Fact]
    public async Task is_last_page()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(5, 10);

        page.HasNextPage.ShouldBeFalse();
        page.HasPreviousPage.ShouldBeTrue();
        page.IsFirstPage.ShouldBeFalse();
        page.IsLastPage.ShouldBeTrue();
    }

    [Fact]
    public async Task middle_page_has_both_neighbors()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(3, 10);

        page.HasNextPage.ShouldBeTrue();
        page.HasPreviousPage.ShouldBeTrue();
        page.IsFirstPage.ShouldBeFalse();
        page.IsLastPage.ShouldBeFalse();
    }

    [Fact]
    public async Task first_and_last_item_on_page()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(2, 10);

        page.FirstItemOnPage.ShouldBe(11);
        page.LastItemOnPage.ShouldBe(20);
    }

    [Fact]
    public async Task last_page_item_indices()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(5, 10);

        page.FirstItemOnPage.ShouldBe(41);
        page.LastItemOnPage.ShouldBe(50);
    }

    [Fact]
    public async Task works_with_where_clause()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .Where(x => x.Color == "Blue")
            .OrderBy(x => x.Number)
            .ToPagedListAsync(1, 10);

        page.TotalItemCount.ShouldBe(30);
        page.PageCount.ShouldBe(3);
        page.Count.ShouldBe(10);
    }

    [Fact]
    public async Task page_beyond_results_returns_empty()
    {
        await using var session = theStore.QuerySession();
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(10, 10);

        page.Count.ShouldBe(0);
        page.TotalItemCount.ShouldBe(50);
        page.IsLastPage.ShouldBeTrue();
    }

    [Fact]
    public async Task throws_on_invalid_page_number()
    {
        await using var session = theStore.QuerySession();
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
        {
            await session.Query<Target>().ToPagedListAsync(0, 10);
        });
    }

    [Fact]
    public async Task throws_on_invalid_page_size()
    {
        await using var session = theStore.QuerySession();
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
        {
            await session.Query<Target>().ToPagedListAsync(1, 0);
        });
    }

    [Fact]
    public async Task uneven_page_size()
    {
        await using var session = theStore.QuerySession();
        // 50 items / 7 per page = 8 pages (7*7=49 + 1 on last page)
        var page = await session.Query<Target>()
            .OrderBy(x => x.Number)
            .ToPagedListAsync(8, 7);

        page.PageCount.ShouldBe(8);
        page.Count.ShouldBe(1);
        page.IsLastPage.ShouldBeTrue();
        page.LastItemOnPage.ShouldBe(50);
    }
}
