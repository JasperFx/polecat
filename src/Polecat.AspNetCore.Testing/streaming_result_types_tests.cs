using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

public class streaming_result_types_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AlbaHost.For<Program>();

        // Apply schema
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        await store.Advanced.CleanAllDocumentsAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
    }

    [Fact]
    public async Task stream_one_returns_200_with_document()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();

        var issue = new StreamingIssue { Id = Guid.NewGuid(), Title = "Bug #1", IsOpen = true };
        await using (var session = store.LightweightSession())
        {
            session.Store(issue);
            await session.SaveChangesAsync();
        }

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{issue.Id}");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        body.ShouldContain("Bug #1");
    }

    [Fact]
    public async Task stream_one_returns_404_when_not_found()
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{Guid.NewGuid()}");
            s.StatusCodeShouldBe(404);
        });
    }

    [Fact]
    public async Task stream_many_returns_200_with_json_array()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();

        await using (var session = store.LightweightSession())
        {
            session.Store(new StreamingIssue { Id = Guid.NewGuid(), Title = "Issue A" });
            session.Store(new StreamingIssue { Id = Guid.NewGuid(), Title = "Issue B" });
            await session.SaveChangesAsync();
        }

        var result = await _host.Scenario(s =>
        {
            s.Get.Url("/api/issues");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        body.ShouldStartWith("[");
        body.ShouldContain("Issue A");
        body.ShouldContain("Issue B");
    }

    [Fact]
    public async Task stream_many_returns_empty_array_when_no_results()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        await store.Advanced.CleanAllDocumentsAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url("/api/issues");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        body.ShouldBe("[]");
    }

    [Fact]
    public async Task stream_aggregate_returns_200_with_aggregate()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        var streamId = Guid.NewGuid();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId,
                new StreamingQuestStarted("Fellowship"),
                new StreamingMembersJoined(["Frodo", "Sam", "Gandalf"]));
            await session.SaveChangesAsync();
        }

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{streamId}");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        body.ShouldContain("Fellowship");
    }

    [Fact]
    public async Task stream_aggregate_returns_404_for_unknown_id()
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{Guid.NewGuid()}");
            s.StatusCodeShouldBe(404);
        });
    }
}
