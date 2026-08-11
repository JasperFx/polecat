using Alba;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     polecat#438: the regression matrix Marten grew <em>after</em> Polecat's #5015-parity port of the
///     streaming result types (polecat#356) — marten#5120, #5157, #5158, #5166 and #5029. None of these
///     behaviors was pinned here, and the SQL Server version-source difference means some of them are
///     expected to hold for free; the point is that they are held on purpose from now on.
/// </summary>
public class streaming_etag_hardening_tests : IAsyncLifetime
{
    private IAlbaHost _host = null!;
    private readonly Guid _issueId = Guid.NewGuid();
    private readonly Guid _revisionedId = Guid.NewGuid();

    public async ValueTask InitializeAsync()
    {
        _host = await AlbaHost.For(TestApp.CreateBuilder(), TestApp.Configure);

        var store = (DocumentStore)_host.Services.GetRequiredService<IDocumentStore>();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await store.Advanced.CleanAllDocumentsAsync();

        await using var session = store.LightweightSession();
        session.Store(new StreamingIssue { Id = _issueId, Title = "Original", Number = 1 });
        session.Store(new RevisionedIssue { Id = _revisionedId, Title = "Revisioned" });
        await session.SaveChangesAsync();
    }

    public async ValueTask DisposeAsync() => await _host.DisposeAsync();

    // ---- marten#5120: a numerically revisioned document still gets an ETag --------------------

    /// <summary>
    ///     Marten gated the StreamOne ETag on a Guid version, so the common CQRS read-model shape — a
    ///     projection-target document, which <c>ProjectionDocumentPolicy</c> forces onto numeric
    ///     revisioning — could never emit one. Polecat's <c>version</c> column is <c>bigint</c> for
    ///     every document, so no such gate exists; pinned here rather than assumed.
    /// </summary>
    [Fact]
    public async Task a_revisioned_document_emits_an_etag()
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/revisioned/{_revisionedId}");
            s.StatusCodeShouldBeOk();
        });

        var etag = result.Context.Response.Headers.ETag.ToString();
        etag.ShouldNotBeNullOrEmpty();
        etag.ShouldStartWith("\"");
    }

    [Fact]
    public async Task a_revisioned_documents_etag_round_trips_to_304()
    {
        var first = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/revisioned/{_revisionedId}");
            s.StatusCodeShouldBeOk();
        });

        var etag = first.Context.Response.Headers.ETag.ToString();

        var second = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/revisioned/{_revisionedId}");
            s.WithRequestHeader("If-None-Match", etag);
            s.StatusCodeShouldBe(304);
        });

        second.ReadAsText().ShouldBeEmpty();
    }

    // ---- marten#5157: a 304 writes no body at all ---------------------------------------------

    /// <summary>
    ///     A conditional-request hit must not copy the payload into the response. Observably: an empty
    ///     body and <c>Content-Length: 0</c>, with the validator echoed back so a cache can refresh its
    ///     freshness without a second request.
    /// </summary>
    [Fact]
    public async Task a_304_has_an_empty_body_and_zero_content_length()
    {
        var first = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{_issueId}");
            s.StatusCodeShouldBeOk();
        });

        var etag = first.Context.Response.Headers.ETag.ToString();

        var second = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{_issueId}");
            s.WithRequestHeader("If-None-Match", etag);
            s.StatusCodeShouldBe(304);
        });

        second.ReadAsText().ShouldBeEmpty();
        second.Context.Response.ContentLength.ShouldBe(0);
        second.Context.Response.Headers.ETag.ToString().ShouldBe(etag);
    }

    [Fact]
    public async Task a_wildcard_if_none_match_also_yields_an_empty_304()
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{_issueId}");
            s.WithRequestHeader("If-None-Match", "*");
            s.StatusCodeShouldBe(304);
        });

        result.ReadAsText().ShouldBeEmpty();
        result.Context.Response.ContentLength.ShouldBe(0);
    }

    // ---- marten#5158: a Select() projection keeps its ETag source column ----------------------

    /// <summary>
    ///     Marten's <c>StreamOne</c> threw outright whenever the queryable carried a <c>Select()</c>
    ///     and <c>EmitETag</c> was on, because the rebuilt select list dropped the payload alias.
    ///     Polecat composes the select list as an explicit <c>data, version</c> pair and reads it by
    ///     ordinal, so the projection cannot lose the ETag source — but a projected StreamOne must
    ///     still answer, and answer with the DOCUMENT body rather than a half-formed row.
    /// </summary>
    [Fact]
    public async Task a_select_projection_still_answers_with_an_etag()
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-projected/{_issueId}");
            s.StatusCodeShouldBeOk();
            s.ContentTypeShouldBe("application/json");
        });

        result.Context.Response.Headers.ETag.ToString().ShouldNotBeNullOrEmpty();

        // Polecat streams the persisted document JSON, so the Title the projection selected is present.
        result.ReadAsText().ShouldContain("Original");
    }

    [Fact]
    public async Task a_select_projections_etag_round_trips_to_304()
    {
        var first = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-projected/{_issueId}");
            s.StatusCodeShouldBeOk();
        });

        var etag = first.Context.Response.Headers.ETag.ToString();

        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-projected/{_issueId}");
            s.WithRequestHeader("If-None-Match", etag);
            s.StatusCodeShouldBe(304);
        });
    }

    // ---- marten#5166: a tracking session streams the body, not the id ------------------------

    /// <summary>
    ///     Marten aliased the payload column BY POSITION, on the strength of a comment claiming the
    ///     selection order is "data, id, everything else". Through an identity-map session the order
    ///     differed and <c>StreamOne</c> streamed the document's <em>id</em> as the body. Polecat names
    ///     both columns explicitly, so this pins that the payload is the document.
    /// </summary>
    [Fact]
    public async Task a_tracking_session_streams_the_document_body_not_its_id()
    {
        var result = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-tracked/{_issueId}");
            s.StatusCodeShouldBeOk();
            s.ContentTypeShouldBe("application/json");
        });

        var body = result.ReadAsText();
        body.ShouldContain("\"title\"");
        body.ShouldContain("Original");
        body.ShouldStartWith("{");
        body.Trim().ShouldNotBe($"\"{_issueId}\"");
    }

    [Fact]
    public async Task a_tracking_session_emits_the_same_etag_as_a_lightweight_one()
    {
        var lightweight = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/{_issueId}");
            s.StatusCodeShouldBeOk();
        });

        var tracked = await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues-tracked/{_issueId}");
            s.StatusCodeShouldBeOk();
        });

        tracked.Context.Response.Headers.ETag.ToString()
            .ShouldBe(lightweight.Context.Response.Headers.ETag.ToString());
    }

    // ---- marten#5029: a malformed continuation cursor is a 400, not a 500 ---------------------

    [Theory]
    [InlineData("garbage")]                 // no version prefix
    [InlineData("v1:!!!not-base64!!!")]     // versioned, undecodable payload
    [InlineData("v1:")]                     // versioned, empty payload
    [InlineData("v2:abc")]                  // a version this build does not issue
    public async Task a_malformed_cursor_is_a_bad_request(string cursor)
    {
        await _host.Scenario(s =>
        {
            s.Get.Url($"/api/issues/paged-cursor/3?cursor={Uri.EscapeDataString(cursor)}");
            s.StatusCodeShouldBe(400);
        });
    }

    /// <summary>
    ///     A structurally valid cursor carrying the wrong number of keys for this query's ordering is
    ///     the same class of client error, not a server fault.
    /// </summary>
    [Fact]
    public async Task a_cursor_with_the_wrong_key_arity_is_a_bad_request()
    {
        var json = System.Text.Json.JsonSerializer.Serialize(new object[] { 1 });
        var cursor = "v1:" + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(json));

        await _host.Scenario(s =>
        {
            // The endpoint orders by Number then Id, so a one-key cursor cannot match it.
            s.Get.Url($"/api/issues/paged-cursor/3?cursor={Uri.EscapeDataString(cursor)}");
            s.StatusCodeShouldBe(400);
        });
    }

    [Fact]
    public async Task a_wellformed_cursor_is_still_served()
    {
        // The 400 path must not have swallowed the happy one.
        await _host.Scenario(s =>
        {
            s.Get.Url("/api/issues/paged-cursor/3");
            s.StatusCodeShouldBeOk();
            s.ContentTypeShouldBe("application/json");
        });
    }
}
