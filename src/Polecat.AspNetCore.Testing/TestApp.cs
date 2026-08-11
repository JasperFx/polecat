using JasperFx;
using JasperFx.Events;
using Polecat.TestUtils;

namespace Polecat.AspNetCore.Testing;

/// <summary>
///     The ASP.NET Core application under test, exposed as a factory rather than as this assembly's
///     entry point.
/// </summary>
/// <remarks>
///     This used to be top-level statements in Program.cs, with the tests bootstrapping it through
///     <c>AlbaHost.For&lt;Program&gt;()</c> (WebApplicationFactory, which invokes the assembly's entry
///     point). That cannot survive the move to xUnit v3: a v3 test assembly is a self-executing test
///     runner, so it needs to own <c>Main</c>. With top-level statements here the generated runner
///     entry point loses, the test executable boots Kestrel instead of discovering tests, and the
///     whole assembly reports zero tests ("Test process did not respond within 60 seconds").
///
///     Handing Alba a pre-built <see cref="WebApplicationBuilder" /> gets the same host without
///     anything in this assembly claiming an entry point.
/// </remarks>
public static class TestApp
{
    public static WebApplicationBuilder CreateBuilder()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = AppContext.BaseDirectory
        });

        builder.Services.AddPolecat(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;

            // #374: an isolated schema, NOT the default dbo. Polecat.Tests owns dbo, and its per-tenant
            // partitioning coverage reshapes dbo.pc_streams; this host's ApplyAllConfiguredChangesToDatabaseAsync
            // would then try to migrate that back, which means dropping the pc_streams primary key that
            // pc_events has a foreign key onto — "Could not drop constraint", 37 failures that look exactly
            // like a regression in whatever you just changed. CI never sees it (fresh database per project),
            // so it only ever bites locally. Mirrors what Polecat.EntityFrameworkCore.Tests and this
            // assembly's own HighWaterHealthCheckTests already do.
            opts.DatabaseSchemaName = "polecat_aspnetcore";

            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Events.StreamIdentity = StreamIdentity.AsGuid;
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableCausationId = true;
        });

        return builder;
    }

    public static void Configure(WebApplication app)
    {
        app.MapPolecatMcp();

        // StreamOne endpoint — returns first matching document or 404
        app.MapGet("/api/issues/{id:guid}", async (Guid id, IQuerySession session) =>
            new StreamOne<StreamingIssue>(session.Query<StreamingIssue>().Where(x => x.Id == id)));

        // StreamMany endpoint — returns JSON array of all documents
        app.MapGet("/api/issues", async (IQuerySession session) =>
            new StreamMany<StreamingIssue>(session.Query<StreamingIssue>()));

        // StreamPaged endpoint — returns one page of documents plus paging metadata in one round trip
        app.MapGet("/api/issues/paged/{pageNumber:int}/{pageSize:int}",
            (int pageNumber, int pageSize, IQuerySession session) =>
                new StreamPaged<StreamingIssue>(
                    session.Query<StreamingIssue>().OrderBy(x => x.Number), pageNumber, pageSize));

        // StreamAggregate endpoint — returns latest aggregate state or 404
        app.MapGet("/api/aggregates/{id:guid}", async (Guid id, IQuerySession session) =>
            new StreamAggregate<StreamingQuestParty>(session, id));

        // EmitETag = false variants — restore the pre-ETag behavior (no ETag header, no 304)
        app.MapGet("/api/issues-noetag/{id:guid}", (Guid id, IQuerySession session) =>
            new StreamOne<StreamingIssue>(session.Query<StreamingIssue>().Where(x => x.Id == id))
            {
                EmitETag = false
            });

        app.MapGet("/api/aggregates-noetag/{id:guid}", (Guid id, IQuerySession session) =>
            new StreamAggregate<StreamingQuestParty>(session, id) { EmitETag = false });

        // StreamPagedByCursor endpoint — one keyset page + continuation cursor
        app.MapGet("/api/issues/paged-cursor/{pageSize:int}",
            (int pageSize, string? cursor, IQuerySession session) =>
                new StreamPagedByCursor<StreamingIssue>(
                    session.Query<StreamingIssue>().OrderBy(x => x.Number).ThenBy(x => x.Id), cursor, pageSize));

        // #438 regression matrix (marten#5120/#5157/#5158/#5166) --------------------------------
        //
        // A REVISIONED document: Polecat's version column is bigint for every document, so the
        // Guid-only ETag gate that marten#5120 reported cannot exist here -- but the point is to pin
        // that, not to assume it.
        app.MapGet("/api/revisioned/{id:guid}", (Guid id, IQuerySession session) =>
            new StreamOne<RevisionedIssue>(session.Query<RevisionedIssue>().Where(x => x.Id == id)));

        // A Select() PROJECTION through StreamOne (marten#5158): the ETag source column must survive
        // the projection rather than the query throwing or the alias being lost.
        app.MapGet("/api/issues-projected/{id:guid}", (Guid id, IQuerySession session) =>
            new StreamOne<IssueTitle>(session.Query<StreamingIssue>()
                .Where(x => x.Id == id)
                .Select(x => new IssueTitle { Title = x.Title })));

        // The same read through a TRACKING (identity map) session (marten#5166): the payload must be
        // the document body, never its id.
        app.MapGet("/api/issues-tracked/{id:guid}", (Guid id, IDocumentStore store) =>
            new StreamOne<StreamingIssue>(
                store.IdentitySession().Query<StreamingIssue>().Where(x => x.Id == id)));

        // #370 StreamEventState endpoint — stream metadata (version/timestamps/archived) or 404
        app.MapGet("/api/streams/{id:guid}/state", (Guid id, IQuerySession session) =>
            new StreamEventState(session, id));

        // #370 StreamEvents endpoint — the stream's raw events as a JSON array, or 404 when empty
        app.MapGet("/api/streams/{id:guid}/events", (Guid id, IQuerySession session) =>
            new StreamEvents(session, id));

        // OnEmptyStatus opt-out — an empty stream answers 200 with an empty array rather than 404, which is
        // what a caller paging forward with fromVersion wants when it runs off the end.
        app.MapGet("/api/streams/{id:guid}/events-empty200", (Guid id, long? fromVersion, IQuerySession session) =>
            new StreamEvents(session, id, fromVersion: fromVersion ?? 0)
            {
                OnEmptyStatus = StatusCodes.Status200OK
            });

        // Pre-built plan constructor — a handler can build the plan once and either batch it or return it
        app.MapGet("/api/streams/{id:guid}/events-by-plan", (Guid id, IQuerySession session) =>
            new StreamEvents(session, new FetchStreamPlan(id, version: 1)));

        app.MapGet("/api/streams/{id:guid}/state-by-plan", (Guid id, IQuerySession session) =>
            new StreamEventState(session, new FetchStreamStatePlan(id)));
    }
}

// Test document types
public class StreamingIssue
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public bool IsOpen { get; set; } = true;

    // Ordering key for paged / cursor streaming endpoints.
    public int Number { get; set; }
}

// Aggregate type for StreamAggregate tests
public partial class StreamingQuestParty
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int MemberCount { get; set; }

    public static StreamingQuestParty Create(StreamingQuestStarted e) =>
        new() { Name = e.Name, MemberCount = 0 };

    public void Apply(StreamingMembersJoined e) => MemberCount += e.Members.Length;
}

// #438 / marten#5120: a numerically revisioned document. IRevisioned puts the mapping on
// UseNumericRevisions, which is the shape a projection-target read model has.
public class RevisionedIssue : JasperFx.IRevisioned
{
    public Guid Id { get; set; }
    public string Title { get; set; } = "";
    public int Version { get; set; }
}

// #438 / marten#5158: the target of a Select() projection over StreamingIssue.
public class IssueTitle
{
    public string Title { get; set; } = "";
}

public record StreamingQuestStarted(string Name);

public record StreamingMembersJoined(string[] Members);
