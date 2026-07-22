using System.Text.Json;
using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     Alba HTTP coverage for the <c>StreamPaged&lt;T&gt;</c> minimal-API result over
///     <c>/api/issues/paged/{pageNumber}/{pageSize}</c> (marten#5014 parity, polecat#355).
///     Each scenario asserts 200, application/json, and the deserialized envelope shape.
/// </summary>
public class stream_paged_tests : IAsyncLifetime
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

    private async Task<JsonElement> GetPageAsync(int pageNumber, int pageSize)
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/paged/{pageNumber}/{pageSize}");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        return JsonDocument.Parse(result.ReadAsText()).RootElement.Clone();
    }

    private static int[] ItemNumbers(JsonElement envelope) =>
        envelope.GetProperty("items").EnumerateArray()
            .Select(x => x.GetProperty("number").GetInt32())
            .ToArray();

    [Fact]
    public async Task first_page()
    {
        var envelope = await GetPageAsync(1, 3);

        envelope.GetProperty("pageNumber").GetInt32().ShouldBe(1);
        envelope.GetProperty("pageSize").GetInt32().ShouldBe(3);
        envelope.GetProperty("totalItemCount").GetInt32().ShouldBe(10);
        envelope.GetProperty("pageCount").GetInt32().ShouldBe(4);
        envelope.GetProperty("hasNextPage").GetBoolean().ShouldBeTrue();
        envelope.GetProperty("hasPreviousPage").GetBoolean().ShouldBeFalse();
        ItemNumbers(envelope).ShouldBe([0, 1, 2]);
    }

    [Fact]
    public async Task middle_page()
    {
        var envelope = await GetPageAsync(2, 3);

        envelope.GetProperty("hasNextPage").GetBoolean().ShouldBeTrue();
        envelope.GetProperty("hasPreviousPage").GetBoolean().ShouldBeTrue();
        ItemNumbers(envelope).ShouldBe([3, 4, 5]);
    }

    [Fact]
    public async Task last_partial_page()
    {
        var envelope = await GetPageAsync(4, 3);

        envelope.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        envelope.GetProperty("hasPreviousPage").GetBoolean().ShouldBeTrue();
        ItemNumbers(envelope).ShouldBe([9]);
    }

    [Fact]
    public async Task single_page()
    {
        var envelope = await GetPageAsync(1, 25);

        envelope.GetProperty("totalItemCount").GetInt32().ShouldBe(10);
        envelope.GetProperty("pageCount").GetInt32().ShouldBe(1);
        envelope.GetProperty("hasNextPage").GetBoolean().ShouldBeFalse();
        envelope.GetProperty("hasPreviousPage").GetBoolean().ShouldBeFalse();
        envelope.GetProperty("items").GetArrayLength().ShouldBe(10);
    }
}
