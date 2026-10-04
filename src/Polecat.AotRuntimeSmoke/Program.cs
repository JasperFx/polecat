// AOT RUNTIME smoke test (#725).
//
// Publishes native and RUNS, against a real database. Its companion
// Polecat.AotSmoke builds and never runs — a static gate on annotations — and
// cannot catch a runtime AOT failure even with a read in it, because
// PolecatQueryableExtensions carries class-level [UnconditionalSuppressMessage]
// for IL2026/IL2060/IL3050. The analyzer is therefore silent on exactly the path
// Marten's four AOT bugs were on, and "writes were fine, the first read threw"
// (marten#5328) is what a build-only lane reports as green.
//
// Each shape below is numbered to the Marten issue it mirrors. The program
// prints a line per shape and returns non-zero on the first failure, so the CI
// log says WHICH shape broke rather than only that something did.

using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.AotRuntimeSmoke;
using Polecat.Linq;

var connectionString =
    Environment.GetEnvironmentVariable("POLECAT_TESTING_DATABASE")
    ?? "Server=localhost,11433;User Id=sa;Password=P@55w0rd;Timeout=5;MultipleActiveResultSets=True;Initial Catalog=master;Encrypt=False";

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddPolecat((StoreOptions opts) =>
{
    opts.ConnectionString = connectionString;
    opts.DatabaseSchemaName = "aot_runtime_smoke";
    opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.All;
});

using var host = builder.Build();

var failures = new List<string>();

async Task Shape(string name, string marten, Func<Task> action)
{
    try
    {
        await action();
        Console.WriteLine($"  ok    {name}  ({marten})");
    }
    catch (Exception e)
    {
        Console.WriteLine($"  FAIL  {name}  ({marten})");
        Console.WriteLine($"        {e.GetType().FullName}: {e.Message}");
        failures.Add(name);
    }
}

Console.WriteLine("Polecat AOT runtime smoke:");

var questId = Guid.NewGuid();

// --- 0. a write, so the reads have something to find -------------------------
// Not one of Marten's four, but the control: if this fails, nothing below means
// anything, and "writes were fine" is the premise of #5328.
await Shape("write a document", "control", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    session.Store(new Quest { Id = questId, Title = "smoke-test", Difficulty = Difficulty.Hard });
    await session.SaveChangesAsync();
});

// --- 1. enumerate a document query -----------------------------------------
await Shape("read a document through LINQ", "marten#5328", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>().Where(q => q.Title == "smoke-test").ToListAsync();
    if (found.Count == 0) throw new InvalidOperationException("the write is not readable");
});

// --- 2. load by id ----------------------------------------------------------
await Shape("load a document by id", "marten#5328", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var loaded = await session.LoadAsync<Quest>(questId);
    if (loaded is null) throw new InvalidOperationException("LoadAsync returned null");
});

// --- 3. an enum compared to a VARIABLE --------------------------------------
// The shape most likely to reach WhereClauseParser's CompileAndInvoke fallback:
// an enum operand is commonly wrapped in a Convert node, which is neither a bare
// constant nor a closure member.
await Shape("LINQ comparing an enum to a variable", "marten#5361", async () =>
{
    var wanted = Difficulty.Hard;
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>().Where(q => q.Difficulty == wanted).ToListAsync();
    if (found.Count == 0) throw new InvalidOperationException("the enum filter matched nothing");
});

// --- 4. read an EVENT -------------------------------------------------------
// Exercises dotnet_type -> CLR type resolution, which is reflective by nature.
await Shape("append and read an event", "marten#5373", async () =>
{
    var streamId = Guid.NewGuid();

    await using (var scope = host.Services.CreateAsyncScope())
    {
        var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
        session.Events.StartStream<Quest>(streamId, new QuestStarted("aot"));
        await session.SaveChangesAsync();
    }

    await using var read = host.Services.CreateAsyncScope();
    var reader = read.ServiceProvider.GetRequiredService<IDocumentSession>();
    var events = await reader.Events.FetchStreamAsync(streamId);
    if (events.Count == 0) throw new InvalidOperationException("no events came back");
    if (events[0].Data is not QuestStarted) throw new InvalidOperationException(
        $"dotnet_type resolved to {events[0].Data.GetType().FullName}");
});

// --- 5. live aggregation ----------------------------------------------------
await Shape("aggregate a stream live", "marten#5373", async () =>
{
    var streamId = Guid.NewGuid();

    await using (var scope = host.Services.CreateAsyncScope())
    {
        var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
        session.Events.StartStream<Quest>(streamId, new QuestStarted("aggregated"));
        await session.SaveChangesAsync();
    }

    await using var read = host.Services.CreateAsyncScope();
    var reader = read.ServiceProvider.GetRequiredService<IDocumentSession>();
    var quest = await reader.Events.AggregateStreamAsync<Quest>(streamId);
    if (quest?.Title != "aggregated") throw new InvalidOperationException(
        $"aggregated to '{quest?.Title}'");
});

// --- 6. a child-collection filter ------------------------------------------
// Goes through the OPENJSON path and a serializer.
await Shape("filter on a child collection", "marten#5374", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>().Where(q => q.Tags.Contains("aot")).ToListAsync();
    _ = found.Count;
});

Console.WriteLine(failures.Count == 0
    ? "Polecat AOT runtime smoke OK."
    : $"Polecat AOT runtime smoke FAILED: {string.Join(", ", failures)}");

return failures.Count == 0 ? 0 : 1;

namespace Polecat.AotRuntimeSmoke
{
    internal enum Difficulty { Easy, Hard }

    internal sealed record QuestStarted(string Title);

    internal sealed class Quest
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public Difficulty Difficulty { get; set; }
        public List<string> Tags { get; set; } = ["aot"];

        public void Apply(QuestStarted e) => Title = e.Title;
    }
}
