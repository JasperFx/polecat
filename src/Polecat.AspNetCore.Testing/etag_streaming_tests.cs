using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     Alba HTTP coverage for ETag / If-None-Match → 304 conditional-request support on
///     <c>StreamOne&lt;T&gt;</c> and <c>StreamAggregate&lt;T&gt;</c> (marten#5015 parity, polecat#356).
/// </summary>
public class etag_streaming_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;

    public async Task InitializeAsync()
    {
        _host = await AlbaHost.For<Program>();

        var store = (DocumentStore)_host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await store.Advanced.CleanAllDocumentsAsync();
    }

    public async Task DisposeAsync()
    {
        await _host.DisposeAsync();
    }

    private async Task<Guid> StoreIssueAsync()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Store(new StreamingIssue { Id = id, Title = "Conditional" });
        await session.SaveChangesAsync();
        return id;
    }

    private async Task<Guid> StartQuestAsync()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        var id = Guid.NewGuid();
        await using var session = store.LightweightSession();
        session.Events.StartStream(id,
            new StreamingQuestStarted("Fellowship"),
            new StreamingMembersJoined(["Frodo", "Sam"]));
        await session.SaveChangesAsync();
        return id;
    }

    // ---------- StreamOne ----------

    [Fact]
    public async Task stream_one_emits_etag_on_hit()
    {
        var id = await StoreIssueAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{id}");
            s.StatusCodeShouldBe(200);
        });

        result.Context.Response.Headers.ETag.ToString().ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public async Task stream_one_returns_304_when_if_none_match_matches()
    {
        var id = await StoreIssueAsync();

        var first = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{id}");
            s.StatusCodeShouldBe(200);
        });
        var etag = first.Context.Response.Headers.ETag.ToString();

        var conditional = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{id}");
            s.WithRequestHeader("If-None-Match", etag);
            s.StatusCodeShouldBe(304);
        });

        conditional.Context.Response.Headers.ETag.ToString().ShouldBe(etag);
        conditional.ReadAsText().ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_one_returns_full_body_when_if_none_match_stale()
    {
        var id = await StoreIssueAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{id}");
            s.WithRequestHeader("If-None-Match", "\"99999\"");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        result.ReadAsText().ShouldContain("Conditional");
    }

    [Fact]
    public async Task stream_one_no_etag_when_emit_etag_false()
    {
        var id = await StoreIssueAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-noetag/{id}");
            s.StatusCodeShouldBe(200);
        });

        result.Context.Response.Headers.ETag.ToString().ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_one_no_etag_behavior_ignores_if_none_match_when_disabled()
    {
        var id = await StoreIssueAsync();

        // Even a matching-looking If-None-Match must NOT produce a 304 when EmitETag is off.
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-noetag/{id}");
            s.WithRequestHeader("If-None-Match", "*");
            s.StatusCodeShouldBe(200);
        });

        result.ReadAsText().ShouldContain("Conditional");
    }

    // ---------- StreamAggregate ----------

    [Fact]
    public async Task stream_aggregate_emits_etag_on_hit()
    {
        var id = await StartQuestAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{id}");
            s.StatusCodeShouldBe(200);
        });

        result.Context.Response.Headers.ETag.ToString().ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public async Task stream_aggregate_returns_304_when_if_none_match_matches()
    {
        var id = await StartQuestAsync();

        var first = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{id}");
            s.StatusCodeShouldBe(200);
        });
        var etag = first.Context.Response.Headers.ETag.ToString();

        var conditional = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{id}");
            s.WithRequestHeader("If-None-Match", etag);
            s.StatusCodeShouldBe(304);
        });

        conditional.Context.Response.Headers.ETag.ToString().ShouldBe(etag);
        conditional.ReadAsText().ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_aggregate_returns_full_body_when_if_none_match_stale()
    {
        var id = await StartQuestAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{id}");
            s.WithRequestHeader("If-None-Match", "\"99999\"");
            s.StatusCodeShouldBe(200);
            s.ContentTypeShouldBe("application/json");
        });

        result.ReadAsText().ShouldContain("Fellowship");
    }

    [Fact]
    public async Task stream_aggregate_etag_changes_after_appending_events()
    {
        var store = _host.Services.GetRequiredService<IDocumentStore>();
        var id = await StartQuestAsync();

        var before = await _host.Scenario(s => s.Get.Url($"/api/aggregates/{id}"));
        var etagBefore = before.Context.Response.Headers.ETag.ToString();

        await using (var session = store.LightweightSession())
        {
            session.Events.Append(id, new StreamingMembersJoined(["Gandalf"]));
            await session.SaveChangesAsync();
        }

        var after = await _host.Scenario(s => s.Get.Url($"/api/aggregates/{id}"));
        var etagAfter = after.Context.Response.Headers.ETag.ToString();

        etagAfter.ShouldNotBe(etagBefore);

        // The previously-cached ETag is now stale → full 200 body, not a 304.
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{id}");
            s.WithRequestHeader("If-None-Match", etagBefore);
            s.StatusCodeShouldBe(200);
        });
    }

    [Fact]
    public async Task stream_aggregate_no_etag_when_emit_etag_false()
    {
        var id = await StartQuestAsync();

        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates-noetag/{id}");
            s.StatusCodeShouldBe(200);
        });

        result.Context.Response.Headers.ETag.ToString().ShouldBeEmpty();
    }

    [Fact]
    public async Task stream_aggregate_404_for_unknown_stream()
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/aggregates/{Guid.NewGuid()}");
            s.StatusCodeShouldBe(404);
        });
    }
}
