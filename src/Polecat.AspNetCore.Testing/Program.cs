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
