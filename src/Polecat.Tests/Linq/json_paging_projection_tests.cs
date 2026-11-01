using System.Text.Json;
using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.Linq;

public class PagedSecretDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string Secret { get; set; } = "";
    public int Number { get; set; }
    public bool Active { get; set; }
}

/// <summary>
///     #836: a page of JSON has to be what the query selected. Both paging paths used to hand back the
///     whole document whatever the <c>Select()</c> said, and a projection is the ordinary way to keep a
///     field out of a response, so what they exposed was the field it hid.
/// </summary>
public class json_paging_projection_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private async Task SeedAsync(int count = 3)
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "A streamed projection needs native JSON.");

        await using var session = theStore.LightweightSession();
        for (var i = 0; i < count; i++)
        {
            session.Store(new PagedSecretDoc
            {
                Id = Guid.NewGuid(), Name = $"n{i}", Secret = "hidden", Number = i, Active = i % 2 == 0
            });
        }

        await session.SaveChangesAsync(Token);
    }

    private IQueryable<PagedSecretDoc> Docs()
    {
        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);
        return query.Query<PagedSecretDoc>();
    }

    [Fact]
    public async Task a_cursor_page_honors_a_projection()
    {
        await SeedAsync();

        var page = await Docs().OrderBy(x => x.Id).Select(x => new { x.Id, x.Name })
            .ToJsonPageByCursorAsync(null, 10, Token);

        page.ItemsJson.ShouldNotContain("hidden");
        page.ItemsJson.ShouldNotContain("secret");

        using var items = JsonDocument.Parse(page.ItemsJson);
        items.RootElement.GetArrayLength().ShouldBe(3);
        items.RootElement[0].GetProperty("name").GetString().ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public async Task a_projected_cursor_keeps_paging_on_a_key_it_did_not_select()
    {
        await SeedAsync(5);

        var seen = new List<string>();
        string? cursor = null;
        do
        {
            var page = await Docs().OrderBy(x => x.Number).ThenBy(x => x.Id).Select(x => new { x.Name })
                .ToJsonPageByCursorAsync(cursor, 2, Token);

            page.ItemsJson.ShouldNotContain("hidden");

            using var items = JsonDocument.Parse(page.ItemsJson);
            seen.AddRange(items.RootElement.EnumerateArray().Select(x => x.GetProperty("name").GetString()!));

            cursor = page.NextCursor;
        } while (cursor != null);

        seen.ShouldBe(["n0", "n1", "n2", "n3", "n4"]);
    }

    [Fact]
    public async Task a_cursor_page_of_a_scalar_is_refused_rather_than_answered_with_documents()
    {
        await SeedAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Docs().OrderBy(x => x.Id).Select(x => x.Name).ToJsonPageByCursorAsync(null, 10, Token));
    }

    [Fact]
    public async Task a_cursor_page_of_a_computed_projection_is_refused()
    {
        await SeedAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await Docs().OrderBy(x => x.Id).Select(x => new { Upper = x.Name.ToUpper() })
                .ToJsonPageByCursorAsync(null, 10, Token));
    }

    [Fact]
    public async Task a_paged_stream_honors_a_projection()
    {
        await SeedAsync();

        using var stream = new MemoryStream();
        await Docs().OrderBy(x => x.Number).Select(x => new { x.Id, x.Name, x.Active })
            .StreamPagedJsonArray(1, 2, stream, Token);

        var json = System.Text.Encoding.UTF8.GetString(stream.ToArray());
        json.ShouldNotContain("hidden");

        using var envelope = JsonDocument.Parse(json);
        envelope.RootElement.GetProperty("totalItemCount").GetInt32().ShouldBe(3);
        envelope.RootElement.GetProperty("hasNextPage").GetBoolean().ShouldBeTrue();

        var items = envelope.RootElement.GetProperty("items");
        items.GetArrayLength().ShouldBe(2);
        items[0].GetProperty("name").GetString().ShouldBe("n0");
        items[0].GetProperty("active").GetBoolean().ShouldBeTrue();
    }

    [Fact]
    public async Task a_paged_stream_of_a_scalar_is_refused()
    {
        await SeedAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
        {
            using var stream = new MemoryStream();
            await Docs().OrderBy(x => x.Number).Select(x => x.Name).StreamPagedJsonArray(1, 2, stream, Token);
        });
    }

    [Fact]
    public async Task an_unprojected_page_is_still_the_whole_document()
    {
        await SeedAsync();

        var page = await Docs().OrderBy(x => x.Id).ToJsonPageByCursorAsync(null, 10, Token);

        page.ItemsJson.ShouldContain("hidden");
    }

    // The deliberate exception, pinned from this side too. An ETag belongs to the document, so
    // ToJsonFirstWithVersionAsync reads it whole; streaming_etag_hardening_tests pins that over HTTP.
    [Fact]
    public async Task first_with_version_stays_the_whole_document_under_a_projection()
    {
        await SeedAsync(1);

        var first = await Docs().Select(x => new { x.Name }).ToJsonFirstWithVersionAsync(Token);

        first.ShouldNotBeNull();
        first.Json.ShouldContain("hidden");
    }
}
