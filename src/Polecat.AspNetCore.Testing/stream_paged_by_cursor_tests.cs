using System.Text.Json;
using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     Alba HTTP coverage for the <c>StreamPagedByCursor&lt;T&gt;</c> minimal-API result over
///     <c>/api/issues/paged-cursor/{pageSize}?cursor=</c> (marten#5016 parity, polecat#357).
/// </summary>
public class stream_paged_by_cursor_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AlbaHost.For<Program>();

        var store = (DocumentStore)_host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await store.Advanced.CleanAllDocumentsAsync();

        await using var session = store.LightweightSession();
        for (var i = 0; i < 10; i++)
        {
            session.Store(new StreamingIssue { Id = Guid.NewGuid(), Title = $"Issue {i}", Number = i });
        }

        await session.SaveChangesAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
    }

    private async Task<(int[] numbers, string? nextCursor, string? header)> GetPageAsync(int pageSize, string? cursor)
    {
        var url = $"/api/issues/paged-cursor/{pageSize}";
        if (cursor != null) url += $"?cursor={Uri.EscapeDataString(cursor)}";

        var result = await _host.Scenario(s =>
        {
            s.Get.Url(url);
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var root = JsonDocument.Parse(result.ReadAsText()).RootElement;
        var numbers = root.GetProperty("items").EnumerateArray()
            .Select(x => x.GetProperty("number").GetInt32()).ToArray();

        var nextCursor = root.GetProperty("nextCursor").ValueKind == JsonValueKind.Null
            ? null
            : root.GetProperty("nextCursor").GetString();

        var header = result.Context.Response.Headers.TryGetValue("Polecat-Continuation", out var h)
            ? h.ToString()
            : null;

        return (numbers, nextCursor, header);
    }

    [Fact]
    public async Task first_page_sets_cursor_and_continuation_header()
    {
        var (numbers, nextCursor, header) = await GetPageAsync(3, null);

        numbers.ShouldBe([0, 1, 2]);
        nextCursor.ShouldNotBeNull();
        header.ShouldBe(nextCursor);
    }

    [Fact]
    public async Task follows_cursor_to_next_page()
    {
        var (_, firstCursor, _) = await GetPageAsync(3, null);
        var (numbers, _, _) = await GetPageAsync(3, firstCursor);

        numbers.ShouldBe([3, 4, 5]);
    }

    [Fact]
    public async Task paginates_entire_set_in_order()
    {
        var all = new List<int>();
        string? cursor = null;
        do
        {
            var (numbers, nextCursor, _) = await GetPageAsync(3, cursor);
            all.AddRange(numbers);
            cursor = nextCursor;
        } while (cursor != null);

        all.ShouldBe([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    [Fact]
    public async Task last_page_has_null_cursor_and_no_header()
    {
        // Page size 25 → all 10 items in one page → end of set, no continuation.
        var (numbers, nextCursor, header) = await GetPageAsync(25, null);

        numbers.Length.ShouldBe(10);
        nextCursor.ShouldBeNull();
        header.ShouldBeNull();
    }
}
