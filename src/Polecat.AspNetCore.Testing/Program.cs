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
});

var app = builder.Build();

app.MapPolecatMcp();

// StreamOne endpoint — returns first matching document or 404
app.MapGet("/api/issues/{id:guid}", async (Guid id, IQuerySession session) =>
    new StreamOne<StreamingIssue>(session.Query<StreamingIssue>().Where(x => x.Id == id)));

// StreamMany endpoint — returns JSON array of all documents
app.MapGet("/api/issues", async (IQuerySession session) =>
    new StreamMany<StreamingIssue>(session.Query<StreamingIssue>()));

// StreamAggregate endpoint — returns latest aggregate state or 404
app.MapGet("/api/aggregates/{id:guid}", async (Guid id, IQuerySession session) =>
    new StreamAggregate<StreamingQuestParty>(session, id));

app.Run();

namespace Polecat.AspNetCore.Testing
{
    // Test document types
    public class StreamingIssue
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = "";
        public bool IsOpen { get; set; } = true;
    }

    // Aggregate type for StreamAggregate tests
    public class StreamingQuestParty
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
