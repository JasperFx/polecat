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
using Polecat.Linq.CursorPaging;
using Polecat.Linq.Metadata;
using Polecat.Linq.SoftDeletes;
using Polecat.Pagination;

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

    // ⚠️ #751: THE THIRD PART OF THE CONSUMER CONTRACT, for a strong-typed document id. Polecat closes
    // its storage over the wrapper at runtime, and a native image has no code for that instantiation
    // unless something named it at compile time -- which is what this call does. Marten's answer too.
    // Medal is deliberately NOT registered: see the shape that expects it to fail.
    opts.RegisterValueTypeId<Badge, BadgeId, Guid>();
});

using var host = builder.Build();

// ⚠️ EVERY SHAPE DECLARES ITS EXPECTED OUTCOME, and the lane is red whenever reality disagrees in
// EITHER direction. That is what lets `continue-on-error` come off (#752).
//
// Before this, the lane printed per-shape results and returned non-zero on any failure — so with two
// shapes documented as failing (then: the strong-typed id and #743) it was PERMANENTLY red, and `continue-on-error: true`
// made it permanently ignorable. A regression in any of the sixteen WORKING shapes would have changed
// nothing a reader or a merge gate could see. A gate that cannot go green does not gate.
//
// So an expected failure is not a silenced one. It is red if it stops reproducing — if the shape
// starts PASSING (the gap closed and the docs went stale) or fails for some OTHER reason than the
// documented one (a database that is down is not evidence about AOT). The `because` substring is what
// makes the second case detectable, and it is the whole reason `Expectation` is not a bool.
var regressions = new List<string>();

// `expect` defaults to Passes on purpose: this lane's premise is that a shape SHOULD work, so
// declaring an expected failure is the exceptional act, and it has to carry a citation.
async Task Shape(string name, string issue, Func<Task> action, Expectation? expect = null)
{
    var expectation = expect ?? Expectation.Passes;

    void Frames(Exception e)
    {
        // ⚠️ The first few frames, because the MESSAGE is not always enough. polecat#741: the LINQ
        // shapes fail with a bare NullReferenceException, which names nothing -- and in a native
        // image there is no debugger to attach, so whatever the lane prints is the whole diagnosis.
        // Trimmed to keep a run of thirty-odd shapes readable.
        foreach (var frame in (e.StackTrace ?? string.Empty)
                 .Split('\n', StringSplitOptions.RemoveEmptyEntries).Take(6))
        {
            Console.WriteLine($"          {frame.Trim()}");
        }
    }

    try
    {
        await action();
    }
    catch (Exception e)
    {
        if (expectation.ShouldPass)
        {
            Console.WriteLine($"  FAIL  {name}  ({issue})");
            Console.WriteLine($"        {e.GetType().FullName}: {e.Message}");
            Frames(e);
            regressions.Add(name);
            return;
        }

        // An expected failure that reproduces. Still printed in full: this log is the only record of
        // WHY the shape is excluded, and the next reader should not have to take the citation's word.
        var message = $"{e.GetType().FullName}: {e.Message}";
        if (!message.Contains(expectation.Because!, StringComparison.Ordinal))
        {
            Console.WriteLine($"  FAIL  {name}  ({issue})");
            Console.WriteLine($"        Expected failure, but NOT the documented one — the message does");
            Console.WriteLine($"        not contain \"{expectation.Because}\". Either the limitation has");
            Console.WriteLine($"        changed shape, or something unrelated broke.");
            Console.WriteLine($"        {message}");
            Frames(e);
            regressions.Add($"{name} [failed for an undocumented reason]");
            return;
        }

        Console.WriteLine($"  xfail {name}  ({issue})");
        Console.WriteLine($"        as documented: {message}");
        return;
    }

    if (!expectation.ShouldPass)
    {
        // ⚠️ The direction that is easy to forget, and the one a plain skip would swallow. If a
        // documented AOT limitation quietly starts working, `docs/configuration/native-aot.md`, the
        // citing issue and this file are all now wrong — which is a thing to go fix, not to celebrate
        // silently.
        Console.WriteLine($"  UNEXPECTED PASS  {name}  ({issue})");
        Console.WriteLine($"        Declared an expected failure \"{expectation.Because}\", and it PASSED.");
        Console.WriteLine($"        Delete the expectation, close {issue}, and correct native-aot.md.");
        regressions.Add($"{name} [expected to fail, passed]");
        return;
    }

    Console.WriteLine($"  ok    {name}  ({issue})");
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
// #751. Registered through opts.RegisterValueTypeId<Badge, BadgeId, Guid>() above, which roots the
// ValueTypeIdentification<,,> / BuildTypedProvider<,> instantiations that BuildValueTypeProvider used to
// close reflectively over a value-type wrapper. Marten's three checks for the same fix (#5589): load by
// id, filter on the id in LINQ, and select the id.
var badgeId = new BadgeId(Guid.NewGuid());

await Shape("write and read a strong-typed id (registered)", "polecat#751", async () =>
{
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

await Shape("LINQ filter on a strong-typed id (registered)", "polecat#751", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Badge>().Where(b => b.Id == badgeId).ToListAsync();
    if (found.Count != 1) throw new InvalidOperationException($"filter on the id matched {found.Count}");
});

await Shape("LINQ select a strong-typed id (registered)", "polecat#751", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var ids = await session.Query<Badge>().Select(b => b.Id).ToListAsync();
    if (!ids.Contains(badgeId)) throw new InvalidOperationException("the stored id was not projected");
});

// ⚠️ EXPECTED FAILURE, and the reason this lane can say the registration is what does the work: the
// same shape over a document whose id was NOT registered still fails exactly as it did before #751. If
// this ever passes, either the reflective path became AOT-safe or something else rooted Medal -- and
// the docs' "you must register" would then be overstating.
await Shape("write and read a strong-typed id (NOT registered)", "polecat#751", async () =>
{
    var medalId = new MedalId(Guid.NewGuid());
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    session.Store(new Medal { Id = medalId, Holder = "aot" });
    await session.SaveChangesAsync();
}, Expectation.FailsWith("is missing native code or metadata"));

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

// ⚠️ EXPECTED FAILURE, #743, and the one shape here that CANNOT be fixed rather than merely has
// not been. The same query with an ANONYMOUS projection, which is the shape every GroupBy example
// writes. GroupByListHandler<> DESERIALIZES the projection through STJ, and under AOT that needs a
// JsonTypeInfo from the consumer's source-generated context -- which a consumer CANNOT supply for an
// anonymous type, because [JsonSerializable] needs a nameable type. So unlike every other AOT
// contract item, this is not one the consumer can satisfy; the answer is a named projection type.
// Marten has the same latent limitation (marten#5624).
await Shape("LINQ GroupBy with an ANONYMOUS projection", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var grouped = await session.Query<Quest>()
        .GroupBy(q => q.Difficulty)
        .Select(g => new { g.Key, Count = g.Count() })
        .ToListAsync();
    _ = grouped.Count;
}, Expectation.FailsWith("JsonTypeInfo metadata for type"));

// polecat#837. A bound predicate inside the projection, an ordering on an aggregate and the terminal
// operators are all new reasons to reach for a generic at runtime, so they are measured. ReadOneGroupAsync
// is closed over TResult at the call site precisely so that FirstAsync does not have to.
await Shape("LINQ GroupBy with a Count(predicate), ordered and paged", "polecat#837", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var title = "smoke-test";
    var grouped = await session.Query<Quest>()
        .GroupBy(q => q.Difficulty)
        .Select(g => new DifficultyTally { Key = g.Key, Count = g.Count(q => q.Title == title) })
        .OrderByDescending(x => x.Count)
        .Skip(0).Take(5)
        .ToListAsync();
    if (grouped.Count == 0) throw new InvalidOperationException("no groups came back");
});

await Shape("LINQ GroupBy terminals: Count, First, and First of a value type", "polecat#837", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var groups = session.Query<Quest>().GroupBy(q => q.Difficulty);

    var count = await groups.Select(g => g.Key).CountAsync();
    if (count == 0) throw new InvalidOperationException("no groups came back");

    _ = await groups.Select(g => new DifficultyTally { Key = g.Key, Count = g.Count() }).FirstAsync();
    _ = await groups.Select(g => g.Count()).OrderByDescending(x => x).FirstAsync();
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

// --- 17+. THE REST OF THE LINQ SURFACE ---------------------------------------
// polecat#743's last measurement item: "includes, metadata queries, soft-delete filters, cursor
// paging, the selectors" -- the surface the class-level [RequiresDynamicCode] covers and the shapes
// above did not reach. This is what the annotation decision is waiting on, because a warning removed
// on partial measurement is the understating suppression again with extra steps.
//
// ⚠️ Includes are NOT measured here because Polecat does not have them: marten-gaps.md lists
// Include<T>() as not planned. That is an absence, not a pass, and it is said here so nobody reads the
// missing shape as an oversight.
//
// Every shape is declared to PASS. Any that does not is a finding, and goes in with an Expectation
// citing an issue, never in silence.

// --- selectors: what a projection closes over ---------------------------------
// The selector is chosen by the projected type, so each of these closes a different generic. The
// value-type ones (an enum, an int) are the shape with no canonical body to share -- the #733 pattern.

await Shape("LINQ Select a REFERENCE-type member (string)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var titles = await session.Query<Quest>().Select(q => q.Title).ToListAsync();
    if (titles.Count == 0) throw new InvalidOperationException("no titles projected");
});

await Shape("LINQ Select a VALUE-type member (enum)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var levels = await session.Query<Quest>().Select(q => q.Difficulty).ToListAsync();
    if (levels.Count == 0) throw new InvalidOperationException("no enums projected");
});

// ⚠️ The enum shape above is what FOUND this: ScalarListHandler<Difficulty> had no native code,
// because the providers closed it over the element type with MakeGenericType. A Guid id is the case
// real code hits most, and the event provider carried its own copy of the same closing -- so both are
// measured rather than inferred from the enum.
await Shape("LINQ Select a Guid id (value type)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var ids = await session.Query<Quest>().Select(q => q.Id).ToListAsync();
    if (!ids.Contains(questId)) throw new InvalidOperationException("the stored id was not projected");
});

await Shape("event LINQ Select a value-type member (long)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var sequences = await session.Events.QueryAllRawEvents().Select(e => e.Sequence).ToListAsync();
    if (sequences.Count == 0) throw new InvalidOperationException("no event sequences projected");
});

await Shape("LINQ Select into a NAMED type, no GroupBy", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var rows = await session.Query<Quest>()
        .Select(q => new DifficultyTally { Key = q.Difficulty, Count = q.Tags.Count })
        .ToListAsync();
    if (rows.Count == 0) throw new InvalidOperationException("no rows projected");
});

// ⚠️ The GroupBy anonymous projection fails in GroupByListHandler; whether a PLAIN Select into an
// anonymous type shares that wall depends on whether its selector deserializes through STJ too.
// Measured rather than assumed in either direction.
await Shape("LINQ Select into an ANONYMOUS type, no GroupBy", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var rows = await session.Query<Quest>().Select(q => new { q.Title, q.Difficulty }).ToListAsync();
    if (rows.Count == 0) throw new InvalidOperationException("no rows projected");
});

// ⚠️ No SelectMany over a child collection. It was added, and failed on CoreCLR as well as native:
// Polecat refuses that operator outright (BadLinqExpressionException, "refused rather than ignored"),
// and supports SelectMany only after a GroupJoin, which the GroupJoin shape above covers. A shape
// that fails without AOT measures the harness, not AOT -- the trap #743 recorded for its own first run.

await Shape("LINQ Select scalar + Distinct", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var titles = await session.Query<Quest>().Select(q => q.Title).Distinct().ToListAsync();
    if (titles.Count == 0) throw new InvalidOperationException("no distinct titles");
});

// --- single-row terminals and ordering ---------------------------------------

await Shape("LINQ OrderBy + Skip + Take", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    _ = await session.Query<Quest>().OrderBy(q => q.Title).ThenBy(q => q.Id).Skip(0).Take(5).ToListAsync();
});

await Shape("LINQ FirstOrDefaultAsync / SingleOrDefaultAsync", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var first = await session.Query<Quest>().Where(q => q.Id == questId).FirstOrDefaultAsync();
    var single = await session.Query<Quest>().Where(q => q.Id == questId).SingleOrDefaultAsync();
    if (first is null || single is null) throw new InvalidOperationException("the stored quest was not found");
});

await Shape("LINQ string StartsWith / Contains", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var found = await session.Query<Quest>()
        .Where(q => q.Title.StartsWith("smoke") || q.Title.Contains("test")).ToListAsync();
    if (found.Count == 0) throw new InvalidOperationException("string predicates matched nothing");
});

// --- paging -------------------------------------------------------------------

await Shape("ToPagedListAsync", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var page = await session.Query<Quest>().OrderBy(q => q.Id).ToPagedListAsync(1, 10);
    if (page.TotalItemCount == 0) throw new InvalidOperationException("paged list is empty");
});

await Shape("ToJsonPageByCursorAsync (cursor paging)", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var page = await session.Query<Quest>().OrderBy(q => q.Id).ToJsonPageByCursorAsync(null, 10);
    _ = page;
});

// polecat#836/#838: no generic in sight, because a projected page is built server-side and never
// deserialized, which is why an anonymous type is fine here when it is not in a GroupBy.
await Shape("ToJsonPageByCursorAsync with a Select() over an enum key, page 2", "polecat#838", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();

    // A second document, so that there is a page 2 for the cursor to reach.
    session.Store(new Quest { Id = Guid.NewGuid(), Title = "smoke-page-2", Difficulty = Difficulty.Easy });
    await session.SaveChangesAsync();

    var first = await session.Query<Quest>().OrderBy(q => q.Difficulty).ThenBy(q => q.Id)
        .Select(q => new { q.Id, q.Title }).ToJsonPageByCursorAsync(null, 1);
    if (first.NextCursor is null) throw new InvalidOperationException("expected a second page");

    var second = await session.Query<Quest>().OrderBy(q => q.Difficulty).ThenBy(q => q.Id)
        .Select(q => new { q.Id, q.Title }).ToJsonPageByCursorAsync(first.NextCursor, 1);
    if (second.Count != 1) throw new InvalidOperationException($"page 2 held {second.Count} rows");
});

await Shape("ToJsonArrayAsync", "polecat#743", async () =>
{
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var json = await session.Query<Quest>().Where(q => q.Id == questId).ToJsonArrayAsync();
    if (!json.Contains("smoke-test")) throw new InvalidOperationException($"unexpected json: {json}");
});

// --- metadata filters ----------------------------------------------------------

await Shape("LINQ ModifiedSince / CreatedSince", "polecat#743", async () =>
{
    var since = DateTimeOffset.UtcNow.AddHours(-1);
    await using var scope = host.Services.CreateAsyncScope();
    var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
    var modified = await session.Query<Quest>().ModifiedSince(since).ToListAsync();
    var created = await session.Query<Quest>().CreatedSince(since).ToListAsync();
    if (modified.Count == 0 || created.Count == 0) throw new InvalidOperationException(
        $"metadata filters matched modified={modified.Count} created={created.Count}");
});

// --- soft deletes ---------------------------------------------------------------
// A [SoftDeleted] type: the default filter hides it, and each of the three operators reaches it.

await Shape("soft delete: default filter, MaybeDeleted, IsDeleted, DeletedSince", "polecat#743", async () =>
{
    var relicId = Guid.NewGuid();
    var before = DateTimeOffset.UtcNow.AddMinutes(-5);

    await using (var scope = host.Services.CreateAsyncScope())
    {
        var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();
        session.Store(new Relic { Id = relicId, Name = "aot" });
        await session.SaveChangesAsync();
        session.Delete<Relic>(relicId);
        await session.SaveChangesAsync();
    }

    await using var read = host.Services.CreateAsyncScope();
    var reader = read.ServiceProvider.GetRequiredService<IDocumentSession>();

    var visible = await reader.Query<Relic>().Where(r => r.Id == relicId).ToListAsync();
    var maybe = await reader.Query<Relic>().MaybeDeleted().Where(r => r.Id == relicId).ToListAsync();
    var deleted = await reader.Query<Relic>().IsDeleted().Where(r => r.Id == relicId).ToListAsync();
    var since = await reader.Query<Relic>().DeletedSince(before).Where(r => r.Id == relicId).ToListAsync();

    if (visible.Count != 0 || maybe.Count != 1 || deleted.Count != 1 || since.Count != 1)
        throw new InvalidOperationException(
            $"visible={visible.Count} maybe={maybe.Count} deleted={deleted.Count} since={since.Count}");
});

Console.WriteLine(regressions.Count == 0
    ? "Polecat AOT runtime smoke OK \u2014 every shape matched its declared expectation."
    : $"Polecat AOT runtime smoke FAILED: {string.Join(", ", regressions)}");

return regressions.Count == 0 ? 0 : 1;

namespace Polecat.AotRuntimeSmoke
{
    /// <summary>
    ///     What a shape is declared to do in a native image. The lane is red whenever reality
    ///     disagrees in either direction.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         ⚠️ <b>This is not a bool, and <see cref="Because" /> is the reason</b> (#751). An
    ///         expected failure that matched on nothing would turn green for a database that is down,
    ///         a trimmed-away property, or any other unrelated break — and it would do so on exactly
    ///         the shape nobody is watching, because its failure is the expected outcome. Pinning the
    ///         substring means the lane says "as documented" only when it is.
    ///     </para>
    ///     <para>
    ///         An expected failure is red three ways: the shape passes, it fails with a message that
    ///         does not contain <see cref="Because" />, or (via <see cref="ShouldPass" /> on every
    ///         other shape) something it shares a code path with regresses.
    ///     </para>
    /// </remarks>
    internal readonly record struct Expectation(bool ShouldPass, string? Because)
    {
        /// <summary>The default, and the premise of this whole lane: the shape should work.</summary>
        internal static Expectation Passes => new(true, null);

        /// <summary>
        ///     A documented limitation. <paramref name="because" /> is a substring the failure's
        ///     <c>Type: Message</c> must contain, so the lane distinguishes "still broken in the way
        ///     the docs say" from "broken for some other reason".
        /// </summary>
        internal static Expectation FailsWith(string because) => new(false, because);
    }

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
    [JsonSerializable(typeof(Medal))]
    [JsonSerializable(typeof(Relic))]
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
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Relic))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(BadgeId))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.PublicProperties, typeof(Medal))]
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(MedalId))]
        internal static void Keep()
        {
        }
    }

    /// <summary>
    ///     #733 / #751: a strong-typed id, registered through <c>RegisterValueTypeId</c>.
    /// </summary>
    /// <remarks>
    ///     <c>BuildValueTypeProvider</c> closes <c>ValueTypeIdentification&lt;TDoc, TOuter, TInner&gt;</c>
    ///     and <c>BuildTypedProvider&lt;TDoc, TOuter&gt;</c> over VALUE-type arguments here (<c>BadgeId</c>
    ///     is a <c>readonly record struct</c>, <c>TInner</c> is <see cref="Guid" />), which has no compiled
    ///     code unless the consumer's registration rooted it. <see cref="MedalId" /> is the control.
    /// </remarks>
    internal readonly record struct BadgeId(Guid Value);

    internal sealed class Badge
    {
        public BadgeId Id { get; set; }
        public string Holder { get; set; } = string.Empty;
    }

    /// <summary>
    ///     #751: a strong-typed id deliberately left UNregistered, so the lane keeps proving that
    ///     <c>RegisterValueTypeId</c> is what makes <see cref="BadgeId" /> work. Rooted exactly like
    ///     <see cref="Badge" /> in every other respect, so the registration is the only difference.
    /// </summary>
    internal readonly record struct MedalId(Guid Value);

    internal sealed class Medal
    {
        public MedalId Id { get; set; }
        public string Holder { get; set; } = string.Empty;
    }

    internal enum Difficulty { Easy, Hard }

    /// <summary>polecat#743: a soft-deleted document, for the soft-delete operators.</summary>
    [Polecat.Attributes.SoftDeleted]
    internal sealed class Relic
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

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
