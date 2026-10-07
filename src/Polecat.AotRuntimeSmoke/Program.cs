using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
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

// ⚠️ #733: THE CONSUMER'S OWN DOCUMENT TYPES NEED THEIR MEMBERS PRESERVED, and this is the line
// that says so. Polecat finds a document's identity by reflecting over its public properties, and
// nothing in a consumer's code statically reads Quest.Id -- the store assigns it and the store reads
// it, both reflectively -- so the trimmer removes it and the identity probe refuses:
//
//   InvalidOperationException: Document type 'Quest' must have a public property named 'Id' ...
//
// This is NOT something Polecat can fix on a consumer's behalf: it cannot name types it has never
// seen. It is the AOT contract a consumer has to meet, and it is exactly the shape of the
// DynamicDependency #734 added for DeadLetterEvent -- the one document type Polecat DOES name.
AotRoots.Keep();

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddPolecat((StoreOptions opts) =>
{
    opts.ConnectionString = connectionString;
    opts.DatabaseSchemaName = "aot_runtime_smoke";
    opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.All;

    // ⚠️ #733: THE SECOND HALF OF THE CONSUMER CONTRACT. Native AOT disables reflection-based
    // System.Text.Json, so without this every document write fails with:
    //
    //   InvalidOperationException: Reflection-based serialization has been disabled for this
    //   application. Either use the source generator APIs or explicitly configure the
    //   'JsonSerializerOptions.TypeInfoResolver' property.
    //
    // Polecat's seam for it is ConfigureSerialization(JsonSerializerOptions, ...). The context has
    // to name every type that crosses the serializer -- including JasperFx's DeadLetterEvent, which
    // Polecat registers as a document on the consumer's behalf and which no consumer would think to
    // list. That is a documentation obligation, not something a consumer can infer.
    opts.ConfigureSerialization(new JsonSerializerOptions
    {
        TypeInfoResolver = SmokeJsonContext.Default
    });
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

        // ⚠️ The first few frames, because the MESSAGE is not always enough. polecat#741: the LINQ
        // shapes fail with a bare NullReferenceException, which names nothing -- and in a native
        // image there is no debugger to attach, so whatever the lane prints is the whole diagnosis.
        // Trimmed to keep a seven-shape run readable.
        foreach (var frame in (e.StackTrace ?? string.Empty)
                 .Split('\n', StringSplitOptions.RemoveEmptyEntries).Take(6))
        {
            Console.WriteLine($"          {frame.Trim()}");
        }

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

// --- 7. a STRONG-TYPED document id -------------------------------------------
// The last shape #733 had not measured. Closes ValueTypeIdentification<,,> and
// BuildTypedProvider<,> over value-type arguments.
await Shape("write and read a strong-typed id", "polecat#733", async () =>
{
    var badgeId = new BadgeId(Guid.NewGuid());

    await using (var scope = host.Services.CreateAsyncScope())
    {
        var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
        session.Store(new Badge { Id = badgeId, Holder = "aot" });
        await session.SaveChangesAsync();
    }

    await using var read = host.Services.CreateAsyncScope();
    var reader = read.ServiceProvider.GetRequiredService<IDocumentSession>();
    var loaded = await reader.LoadAsync<Badge>(badgeId);
    if (loaded?.Holder != "aot") throw new InvalidOperationException(
        $"strong-typed id round trip gave '{loaded?.Holder}'");
});

// --- 8-10. the three shapes that reach WhereClauseParser.CompileAndInvoke ------
// polecat#743. Shape 3 above was supposed to be the CompileAndInvoke probe and is NOT: an enum
// compared to a captured local is a MemberExpression over a ConstantExpression closure, which
// ExtractValue reads with FieldInfo.GetValue and never compiles. So the fallback has been
// unmeasured this whole time, and #743 asks which shapes "genuinely need a JIT" before narrowing
// the annotation around them. These three are every branch of ExtractValue that ends in
// Expression.Lambda(...).Compile().DynamicInvoke().

await Shape("LINQ where-value from a METHOD CALL", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>().Where(q => q.Title == TitleSource.Build()).ToListAsync();
    _ = found.Count;
});

await Shape("LINQ where-value from a MEMBER CHAIN", "polecat#743", async () =>
{
    var holder = new TitleHolder { Inner = new TitleHolder { Title = "aot" } };
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>().Where(q => q.Title == holder.Inner.Title).ToListAsync();
    _ = found.Count;
});

await Shape("LINQ where-value from an ARRAY LITERAL", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>()
        .Where(q => new[] { "aot", "other" }.Contains(q.Title)).ToListAsync();
    _ = found.Count;
});

// --- 11-14. the SCALAR aggregates, whose TResult is a VALUE TYPE ---------------
// polecat#743, and the shape that matters most here. Every LINQ fact above resolves through
// IPolecatAsyncQueryProvider.ExecuteAsync<TResult> with a REFERENCE-type TResult --
// IReadOnlyList<Quest>, Quest. The scalar surface closes that same method over int, long, bool,
// decimal and double, and a runtime-closed generic with a value-type argument is the one shape
// with no canonical body to share. That is the pattern behind #733, #740 and #742, so leaving the
// whole scalar surface unmeasured while narrowing an annotation around the where-clause shapes
// would be narrowing around the wrong thing.

await Shape("LINQ CountAsync (TResult = int)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    _ = await session.Query<Quest>().CountAsync();
});

await Shape("LINQ AnyAsync (TResult = bool)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    _ = await session.Query<Quest>().AnyAsync();
});

await Shape("LINQ SumAsync (TResult = int)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    _ = await session.Query<Quest>().SumAsync(q => q.Tags.Count);
});

await Shape("LINQ MinAsync (TResult = a type ARGUMENT)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    _ = await session.Query<Quest>().MinAsync<Quest, string>(q => q.Title);
});

// --- 15-16. GROUP BY and GROUP JOIN -------------------------------------------
// polecat#743. The two LINQ execution paths with their own handlers and their own
// [RequiresDynamicCode] messages, and the two most likely to genuinely need a JIT:
//
//   GroupByListHandler<>  closed over the PROJECTED/ELEMENT type
//   JoinListHandler<,,> + Func<,,>  closed over outer/inner/result
//
// A group-by key is routinely int, Guid, DateTime or an enum, so unlike ExecuteAsync<TResult>
// (statically instantiated by its caller) this closing is genuinely at runtime over whatever the
// projection produces. Grouping by an ENUM here on purpose: a value type declared in the
// consumer's own assembly, which is the worst case.

await Shape("LINQ GroupBy with a VALUE-TYPE key, NAMED projection", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var grouped = await session.Query<Quest>()
        .GroupBy(q => q.Difficulty)
        .Select(g => new DifficultyTally { Key = g.Key, Count = g.Count() })
        .ToListAsync();
    _ = grouped.Count;
});

// ⚠️ The same query with an ANONYMOUS projection, which is the shape every GroupBy example writes.
// GroupByListHandler<> DESERIALIZES the projection through STJ, and under AOT that needs a
// JsonTypeInfo from the consumer's source-generated context -- which a consumer CANNOT supply for an
// anonymous type, because [JsonSerializable] needs a nameable type. So this is not a "root your
// types" contract the consumer can satisfy; measured here rather than assumed either way.
await Shape("LINQ GroupBy with an ANONYMOUS projection", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var grouped = await session.Query<Quest>()
        .GroupBy(q => q.Difficulty)
        .Select(g => new { g.Key, Count = g.Count() })
        .ToListAsync();
    _ = grouped.Count;
});

await Shape("LINQ GroupJoin", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var joined = await session.Query<Quest>()
        .GroupJoin(session.Query<Quest>(), o => o.Id, i => i.Id, (o, inner) => new { o, inner })
        .SelectMany(x => x.inner.DefaultIfEmpty(), (x, i) => new JoinedTitle { Outer = x.o.Title, Inner = i!.Title })
        .ToListAsync();
    _ = joined.Count;
});

Console.WriteLine(failures.Count == 0
    ? "Polecat AOT runtime smoke OK."
    : $"Polecat AOT runtime smoke FAILED: {string.Join(", ", failures)}");

return failures.Count == 0 ? 0 : 1;

namespace Polecat.AotRuntimeSmoke
{
    /// <summary>
    ///     #733: the source-generated serializer a Native AOT consumer must supply.
    /// </summary>
    /// <remarks>
    ///     ⚠️ <c>DeadLetterEvent</c> is listed because POLECAT registers it as a document type in
    ///     <c>DocumentStore</c>'s constructor, so it crosses the serializer even in an application
    ///     that never touches dead letters. A consumer cannot deduce that from their own code.
    /// </remarks>
    [JsonSerializable(typeof(Quest))]
    [JsonSerializable(typeof(QuestStarted))]
    [JsonSerializable(typeof(Badge))]
    [JsonSerializable(typeof(JasperFx.Events.Daemon.DeadLetterEvent))]
    [JsonSerializable(typeof(DifficultyTally))]
    [JsonSerializable(typeof(JoinedTitle))]
    internal sealed partial class SmokeJsonContext : JsonSerializerContext
    {
    }

    /// <summary>
    ///     #733: keeps the document types' members alive through trimming. A real consumer writes
    ///     this, or uses a source generator that writes it for them.
    /// </summary>
    internal static class AotRoots
    {
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Quest))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(QuestStarted))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Badge))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(BadgeId))]
        internal static void Keep()
        {
        }
    }

    /// <summary>
    ///     #733: a strong-typed id, so the one remaining doubt gets measured instead of asserted.
    /// </summary>
    /// <remarks>
    ///     `BuildValueTypeProvider` closes <c>ValueTypeIdentification&lt;TDoc, TOuter, TInner&gt;</c>
    ///     and <c>BuildTypedProvider&lt;TDoc, TOuter&gt;</c> reflectively, because the wrapper type is
    ///     a runtime value. Both have VALUE-type arguments here (<c>BadgeId</c> is a
    ///     <c>readonly record struct</c>, <c>TInner</c> is <see cref="Guid" />), which is the shape
    ///     that normally has no compiled code — so I expected this to fail and would rather find out
    ///     than keep saying so. Marten's AOT guide lists strong-typed ids as working.
    /// </remarks>
    internal readonly record struct BadgeId(Guid Value);

    internal sealed class Badge
    {
        public BadgeId Id { get; set; }
        public string Holder { get; set; } = string.Empty;
    }

    internal enum Difficulty { Easy, Hard }

    internal sealed record QuestStarted(string Title);

    /// <summary>
    ///     polecat#743. A STATIC METHOD rather than a local function, because an expression tree
    ///     cannot reference one (CS8110) — and the whole point is for this call to survive into the
    ///     tree as a MethodCallExpression, which is the node ExtractValue has no reflective reading
    ///     for.
    /// </summary>
    internal sealed class DifficultyTally
    {
        public Difficulty Key { get; set; }
        public int Count { get; set; }
    }

    internal sealed class JoinedTitle
    {
        public string Outer { get; set; } = string.Empty;
        public string? Inner { get; set; }
    }

    internal static class TitleSource
    {
        internal static string Build() => "aot";
    }

    internal sealed class TitleHolder
    {
        public string Title { get; set; } = string.Empty;
        public TitleHolder? Inner { get; set; }
    }

    internal sealed class Quest
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public Difficulty Difficulty { get; set; }
        public List<string> Tags { get; set; } = ["aot"];

        public void Apply(QuestStarted e) => Title = e.Title;
    }
}
