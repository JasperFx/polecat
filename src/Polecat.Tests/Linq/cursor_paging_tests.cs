using System.Text.Json;
using Polecat.Linq.CursorPaging;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     Keyset (cursor / seek) pagination coverage — marten#5016 parity, polecat#357. Each test
///     isolates its own document set behind a unique color filter, and orders with the document
///     identity (Id) as the terminal key so the ordering is a total order.
/// </summary>
[Collection("integration")]
public class cursor_paging_tests : IntegrationContext
{
    public cursor_paging_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<string> SeedNumberedAsync(int count)
    {
        var color = $"Cursor_{Guid.NewGuid():N}";
        for (var i = 0; i < count; i++)
        {
            theSession.Store(new Target { Id = Guid.NewGuid(), Color = color, Number = i });
        }

        await theSession.SaveChangesAsync();
        return color;
    }

    private IQueryable<Target> Ascending(string color) =>
        theStore.QuerySession().Query<Target>()
            .Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id);

    private static int[] Numbers(CursorPageResult page) =>
        JsonDocument.Parse(page.ItemsJson).RootElement.EnumerateArray()
            .Select(x => x.GetProperty("number").GetInt32())
            .ToArray();

    private async Task<List<int>> PageAllNumbersAscendingAsync(string color, int pageSize)
    {
        var all = new List<int>();
        string? cursor = null;
        while (true)
        {
            await using var q = theStore.QuerySession();
            var page = await q.Query<Target>()
                .Where(t => t.Color == color)
                .OrderBy(t => t.Number).ThenBy(t => t.Id)
                .ToJsonPageByCursorAsync(cursor, pageSize);

            all.AddRange(Numbers(page));
            if (page.NextCursor is null) break;
            cursor = page.NextCursor;

            if (all.Count > 100_000) throw new InvalidOperationException("cursor pagination did not terminate");
        }

        return all;
    }

    [Fact]
    public async Task first_page()
    {
        var color = await SeedNumberedAsync(10);

        await using var q = theStore.QuerySession();
        var page = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(null, 3);

        Numbers(page).ShouldBe([0, 1, 2]);
        page.Count.ShouldBe(3);
        page.NextCursor.ShouldNotBeNull();
        page.NextCursor!.ShouldStartWith("v1:");
    }

    [Fact]
    public async Task subsequent_page()
    {
        var color = await SeedNumberedAsync(10);

        await using var q = theStore.QuerySession();
        var first = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(null, 3);

        var second = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(first.NextCursor, 3);

        Numbers(second).ShouldBe([3, 4, 5]);
        second.NextCursor.ShouldNotBeNull();
    }

    [Fact]
    public async Task last_page_has_null_cursor()
    {
        var color = await SeedNumberedAsync(5);

        await using var q = theStore.QuerySession();
        var first = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(null, 3);
        var last = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(first.NextCursor, 3);

        Numbers(last).ShouldBe([3, 4]);
        last.Count.ShouldBe(2);
        last.NextCursor.ShouldBeNull();
    }

    [Fact]
    public async Task exact_multiple_of_page_size_terminates()
    {
        var color = await SeedNumberedAsync(6);

        var all = await PageAllNumbersAscendingAsync(color, 3);
        all.ShouldBe([0, 1, 2, 3, 4, 5]);
    }

    [Fact]
    public async Task paginates_entire_set_in_order()
    {
        var color = await SeedNumberedAsync(10);

        var all = await PageAllNumbersAscendingAsync(color, 3);
        all.ShouldBe([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    [Fact]
    public async Task empty_set()
    {
        var color = $"CursorNever_{Guid.NewGuid():N}";

        await using var q = theStore.QuerySession();
        var page = await q.Query<Target>().Where(t => t.Color == color)
            .OrderBy(t => t.Number).ThenBy(t => t.Id)
            .ToJsonPageByCursorAsync(null, 3);

        page.Count.ShouldBe(0);
        page.NextCursor.ShouldBeNull();
        page.ItemsJson.ShouldBe("[]");
    }

    [Fact]
    public async Task mixed_ascending_descending()
    {
        var color = await SeedNumberedAsync(10);

        var all = new List<int>();
        string? cursor = null;
        while (true)
        {
            await using var q = theStore.QuerySession();
            var page = await q.Query<Target>().Where(t => t.Color == color)
                .OrderByDescending(t => t.Number).ThenBy(t => t.Id)
                .ToJsonPageByCursorAsync(cursor, 3);

            all.AddRange(Numbers(page));
            if (page.NextCursor is null) break;
            cursor = page.NextCursor;
        }

        all.ShouldBe([9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
    }

    [Fact]
    public async Task exhaustive_pagination_through_duplicate_leading_keys()
    {
        // 20 targets that ALL share the same leading sort key (Number = 5). The Id tie-breaker makes
        // the ordering a total order; paginating with a small page must visit every id exactly once —
        // no skips, no duplicates across the tie. This is the core keyset-correctness invariant.
        var color = $"CursorTie_{Guid.NewGuid():N}";
        var expected = new List<Guid>();
        for (var i = 0; i < 20; i++)
        {
            var id = Guid.NewGuid();
            expected.Add(id);
            theSession.Store(new Target { Id = id, Color = color, Number = 5 });
        }

        await theSession.SaveChangesAsync();

        var seen = new List<Guid>();
        string? cursor = null;
        while (true)
        {
            await using var q = theStore.QuerySession();
            var page = await q.Query<Target>().Where(t => t.Color == color)
                .OrderBy(t => t.Number).ThenBy(t => t.Id)
                .ToJsonPageByCursorAsync(cursor, 3);

            foreach (var el in JsonDocument.Parse(page.ItemsJson).RootElement.EnumerateArray())
            {
                seen.Add(el.GetProperty("id").GetGuid());
            }

            if (page.NextCursor is null) break;
            cursor = page.NextCursor;
        }

        seen.Count.ShouldBe(20);
        seen.Distinct().Count().ShouldBe(20);
        seen.OrderBy(x => x).ShouldBe(expected.OrderBy(x => x));
    }

    [Fact]
    public async Task uniqueidentifier_ordering_seek_matches_order_by()
    {
        // SQL Server orders `uniqueidentifier` by byte groups (differs from Postgres uuid). As long as
        // the seek predicate compares the same `id` column with the same operator as the ORDER BY, the
        // boundary lines up and pagination neither skips nor duplicates. Order by Id alone (Id is the
        // identity terminal key) and page through, verifying every id is visited once.
        var color = $"CursorGuid_{Guid.NewGuid():N}";
        var ids = new List<Guid>();
        for (var i = 0; i < 25; i++)
        {
            var id = Guid.NewGuid();
            ids.Add(id);
            theSession.Store(new Target { Id = id, Color = color, Number = i });
        }

        await theSession.SaveChangesAsync();

        var seen = new List<Guid>();
        string? cursor = null;
        while (true)
        {
            await using var q = theStore.QuerySession();
            var page = await q.Query<Target>().Where(t => t.Color == color)
                .OrderBy(t => t.Id)
                .ToJsonPageByCursorAsync(cursor, 4);

            foreach (var el in JsonDocument.Parse(page.ItemsJson).RootElement.EnumerateArray())
            {
                seen.Add(el.GetProperty("id").GetGuid());
            }

            if (page.NextCursor is null) break;
            cursor = page.NextCursor;
        }

        seen.Count.ShouldBe(25);
        seen.Distinct().Count().ShouldBe(25);
        // The visited order must be strictly ascending in SQL Server's uniqueidentifier ordering.
        seen.ShouldBe(seen.OrderBy(x => x, new SqlGuidComparer()).ToList());
    }

    [Fact]
    public async Task refuses_query_without_order_by()
    {
        await using var q = theStore.QuerySession();
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await q.Query<Target>().Where(t => t.Number > 0).ToJsonPageByCursorAsync(null, 3));
    }

    [Fact]
    public async Task refuses_non_identity_terminal_key()
    {
        await using var q = theStore.QuerySession();
        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await q.Query<Target>().OrderBy(t => t.Number).ToJsonPageByCursorAsync(null, 3));
    }

    [Fact]
    public async Task refuses_non_positive_page_size()
    {
        await using var q = theStore.QuerySession();
        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await q.Query<Target>().OrderBy(t => t.Id).ToJsonPageByCursorAsync(null, 0));
    }

    // Mirrors System.Data.SqlTypes.SqlGuid ordering (the byte-group order SQL Server uses).
    private sealed class SqlGuidComparer : IComparer<Guid>
    {
        public int Compare(Guid x, Guid y) =>
            new System.Data.SqlTypes.SqlGuid(x).CompareTo(new System.Data.SqlTypes.SqlGuid(y));
    }
}
