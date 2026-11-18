using System.Text.Json;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     Unit-level coverage for <see cref="JsonQueryExtensions.StreamPagedJsonArray{T}"/> — the
///     paged JSON envelope streamed in one round trip (marten#5014 parity, polecat#355). Each test
///     isolates its own document set behind a unique color filter so the running-total window
///     (COUNT(*) OVER()) counts only that set on the shared integration schema.
/// </summary>
[Collection("integration")]
public class stream_paged_tests : IntegrationContext
{
    public stream_paged_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    private async Task<string> SeedTargetsAsync(int count)
    {
        var color = $"Paged_{Guid.NewGuid():N}";
        for (var i = 0; i < count; i++)
        {
            theSession.Store(new Target { Id = Guid.NewGuid(), Color = color, Number = i });
        }

        await theSession.SaveChangesAsync();
        return color;
    }

    private async Task<JsonDocument> StreamPageAsync(string color, int pageNumber, int pageSize)
    {
        await using var query = theStore.QuerySession();
        using var stream = new MemoryStream();
        await query.Query<Target>()
            .Where(t => t.Color == color)
            .OrderBy(t => t.Number)
            .StreamPagedJsonArray(pageNumber, pageSize, stream);

        stream.Position = 0;
        return await JsonDocument.ParseAsync(stream);
    }

    private static int[] ItemNumbers(JsonDocument doc) =>
        doc.RootElement.GetProperty("items").EnumerateArray()
            .Select(x => x.GetProperty("number").GetInt32())
            .ToArray();

    [Fact]
    public async Task stream_paged_json_array_first_page()
    {
        var color = await SeedTargetsAsync(10);

        using var doc = await StreamPageAsync(color, 1, 3);
        var root = doc.RootElement;

        root.GetProperty("pageNumber").GetInt32().ShouldBe(1);
        root.GetProperty("pageSize").GetInt32().ShouldBe(3);
        root.GetProperty("totalItemCount").GetInt32().ShouldBe(10);
        root.GetProperty("pageCount").GetInt32().ShouldBe(4);
        root.GetProperty("hasNextPage").GetBoolean().ShouldBeTrue();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeFalse();
        ItemNumbers(doc).ShouldBe([0, 1, 2]);
    }

    [Fact]
    public async Task stream_paged_json_array_middle_page()
    {
        var color = await SeedTargetsAsync(10);

        using var doc = await StreamPageAsync(color, 2, 3);
        var root = doc.RootElement;

        root.GetProperty("hasNextPage").GetBoolean().ShouldBeTrue();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeTrue();
        ItemNumbers(doc).ShouldBe([3, 4, 5]);
    }

    [Fact]
    public async Task stream_paged_json_array_last_partial_page()
    {
        var color = await SeedTargetsAsync(10);

        using var doc = await StreamPageAsync(color, 4, 3);
        var root = doc.RootElement;

        root.GetProperty("totalItemCount").GetInt32().ShouldBe(10);
        root.GetProperty("pageCount").GetInt32().ShouldBe(4);
        root.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeTrue();
        ItemNumbers(doc).ShouldBe([9]);
    }

    [Fact]
    public async Task stream_paged_json_array_single_page()
    {
        var color = await SeedTargetsAsync(5);

        using var doc = await StreamPageAsync(color, 1, 25);
        var root = doc.RootElement;

        root.GetProperty("totalItemCount").GetInt32().ShouldBe(5);
        root.GetProperty("pageCount").GetInt32().ShouldBe(1);
        root.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("items").GetArrayLength().ShouldBe(5);
    }

    [Fact]
    public async Task stream_paged_json_array_no_hits()
    {
        var color = $"NeverExists_{Guid.NewGuid():N}";

        using var doc = await StreamPageAsync(color, 1, 10);
        var root = doc.RootElement;

        root.GetProperty("totalItemCount").GetInt32().ShouldBe(0);
        root.GetProperty("pageCount").GetInt32().ShouldBe(0);
        root.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("items").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task stream_paged_json_array_no_hits_beyond_first_page_has_previous_page()
    {
        var color = $"NeverExists_{Guid.NewGuid():N}";

        // page > 1 on an empty set: total 0 but hasPreviousPage stays true (pins the empty-branch semantics)
        using var doc = await StreamPageAsync(color, 2, 10);
        var root = doc.RootElement;

        root.GetProperty("totalItemCount").GetInt32().ShouldBe(0);
        root.GetProperty("pageCount").GetInt32().ShouldBe(0);
        root.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        root.GetProperty("hasPreviousPage").GetBoolean().ShouldBeTrue();
        root.GetProperty("items").GetArrayLength().ShouldBe(0);
    }

    [Fact]
    public async Task stream_paged_json_array_throws_for_page_number_below_one()
    {
        await using var query = theStore.QuerySession();
        using var stream = new MemoryStream();

        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await query.Query<Target>().OrderBy(t => t.Number).StreamPagedJsonArray(0, 10, stream));
    }

    [Fact]
    public async Task stream_paged_json_array_throws_for_page_size_below_one()
    {
        await using var query = theStore.QuerySession();
        using var stream = new MemoryStream();

        await Should.ThrowAsync<ArgumentOutOfRangeException>(async () =>
            await query.Query<Target>().OrderBy(t => t.Number).StreamPagedJsonArray(1, 0, stream));
    }

    [Fact]
    public async Task stream_paged_json_array_past_end_of_non_empty_set_reports_zero_total()
    {
        // 10 items, request page 5 of pageSize 3 → OFFSET 12 → zero rows. The window-function total
        // is lost, so the envelope reports totalItemCount 0 even though items exist. Same quirk as
        // PagedList / Marten — pin it so we don't diverge without a coordinated decision.
        var color = await SeedTargetsAsync(10);

        using var doc = await StreamPageAsync(color, 5, 3);
        var root = doc.RootElement;

        root.GetProperty("totalItemCount").GetInt32().ShouldBe(0);
        root.GetProperty("pageCount").GetInt32().ShouldBe(0);
        root.GetProperty("items").GetArrayLength().ShouldBe(0);
    }
}
