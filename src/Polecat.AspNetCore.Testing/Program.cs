using JasperFx;
using JasperFx.Events;
using Polecat;
using Polecat.AspNetCore;
using Polecat.AspNetCore.Testing;
using Polecat.TestUtils;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
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

var app = builder.Build();

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

app.Run();

namespace Polecat.AspNetCore.Testing
{
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

    public record StreamingQuestStarted(string Name);
    public record StreamingMembersJoined(string[] Members);
}
