// AOT smoke test (Polecat#46).
//
// This program touches a representative cross-section of the AOT-clean Polecat
// consumer surface — DI registration via AddPolecat, projection registration,
// session resolution, and one LINQ query construction. The csproj sets
// IsAotCompatible=true and promotes the AOT analyzer warning codes to errors,
// so any change that adds [RequiresDynamicCode] / [RequiresUnreferencedCode]
// to an API exercised here — or any change to this file that calls into a
// reflective Polecat surface — fails the build in CI.
//
// Crucially, this project does NOT reference JasperFx.RuntimeCompiler and does
// NOT call services.AddRuntimeCompilation(). It represents a "Static
// TypeLoadMode" consumer — the path AOT publishers take. If Polecat needs
// runtime codegen on a path we exercise here, the build will fail and we'll
// either annotate the underlying surface or narrow the smoke test.
//
// Intentionally *not* exercised here (these paths carry AOT annotations or
// depend on runtime codegen that's outside the static-mode contract):
//   - DocumentStore.LightweightSession() called from outside DI (we go through
//     ISessionFactory so the AddPolecat extension is what's gated).
//   - SaveChangesAsync / session command execution (would require a real DB and
//     runs through reflection-based DocumentMapping + the STJ serializer, which
//     carry the RUC/RDC annotations we don't exercise here).
//     ⚠️ #725: Polecat.AotRuntimeSmoke is the lane that DOES execute, natively
//     published against a real database, because no build-only gate can observe
//     a runtime AOT failure.
//   - Async daemon / projection runtime (projection dispatch is source-generated
//     by JasperFx.Events.SourceGenerator, exercised via the concrete projection
//     registration below).

using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.AotSmoke;
using Polecat.Linq;
using Polecat.Projections;

var builder = Host.CreateApplicationBuilder(args);

// --- AddPolecat: the main consumer DI surface ---------------------------
// Configures a StoreOptions and registers a single self-aggregating snapshot
// projection. Connection string is intentionally bogus — the smoke test
// never opens a connection.
//
// Lambda is explicitly typed StoreOptions to disambiguate from the
// Func<IServiceProvider, StoreOptions> overload — both are extension methods
// on IServiceCollection with the same arity.
builder.Services.AddPolecat((StoreOptions opts) =>
{
    opts.ConnectionString =
        "Server=aot-smoke;Database=aot_smoke;Integrated Security=False;User Id=sa;Password=irrelevant;TrustServerCertificate=True";
    opts.DatabaseSchemaName = "aot_smoke";

    // AOT-safe projection registration: use the new()-constrained Add<T>(...)
    // overload with a concrete SingleStreamProjection<TDoc, TId> subclass.
    //
    // This is the registration shape AOT consumers must use. The reflective
    // shortcut `opts.Projections.Snapshot<Quest>(...)` is annotated
    // [RequiresDynamicCode] + [RequiresUnreferencedCode] (it closes
    // SingleStreamProjection<,> over (T, T.Id) via Type.MakeGenericType and
    // resolves T's Id property via DocumentMapping reflection) and would
    // surface IL2026 + IL3050 here under our WarningsAsErrors. The concrete
    // subclass below has both generic arguments closed at compile time, so the
    // analyzer can prove AOT safety.
    opts.Projections.Add<QuestProjection>(ProjectionLifecycle.Inline);
});

using var host = builder.Build();

// --- IDocumentSession resolution ----------------------------------------
// Resolve a session through the DI surface so AddPolecat's scoped registration
// (ISessionFactory -> IDocumentSession) is reachable code. DocumentStore's
// ctor does not open a SQL connection; LightweightSession is lazy on first
// command. We never issue a command.
//
// CreateAsyncScope (vs CreateScope) so IDocumentSession's IAsyncDisposable
// is honored on scope teardown — LightweightSession does not implement sync
// IDisposable.
await using var scope = host.Services.CreateAsyncScope();
var session = scope.ServiceProvider.GetRequiredService<IDocumentSession>();

// --- LINQ query construction --------------------------------------------
// Construct a Where-filtered IQueryable through the LINQ provider. This
// exercises IQuerySession.Query<T> + the generic
// IQueryProvider.CreateQuery<TElement>(Expression) entry on
// PolecatLinqQueryProvider — both AOT-clean.
//
// ⚠️ #725 corrected what this comment used to claim. It said materialization
// "routes through the reflective ExecuteAsync<TResult> path, which is annotated
// [RequiresDynamicCode] + [RequiresUnreferencedCode] and would surface here
// under our WarningsAsErrors". It does NOT surface, and that is the whole
// problem: PolecatQueryableExtensions carries CLASS-LEVEL
// [UnconditionalSuppressMessage] for IL2026/IL2060/IL3050, so the annotation on
// ExecuteAsync is swallowed at the extension-method boundary. The same class's
// own remarks say "AOT-publishing apps should avoid the LINQ-async wrappers" —
// which the suppression guarantees no consumer is ever told by the analyzer.
//
// So the reads below are build-clean, and this lane gates their ANNOTATIONS
// only: if a future change replaces that suppression with a propagating
// [RequiresDynamicCode], this build is where it is noticed. What actually
// happens when an AOT-published consumer runs them is a different question, and
// a build-only lane cannot answer it — see Polecat.AotRuntimeSmoke, which
// publishes native and runs. "Writes were fine, the first read threw"
// (marten#5328) is exactly what a lane that builds but never executes reports
// as green.
var query = session.Query<Quest>().Where(q => q.Title == "smoke-test");
_ = query.Expression;

// Execution shapes, each mirroring one of Marten's four Native AOT failures.
// Never reached — the connection string is bogus and this program's job is to
// compile — so they are wrapped rather than awaited for effect. Taking their
// delegates keeps the call sites in the compiled, analyzed surface.
_ = new Func<Task>(async () => _ = await query.ToListAsync());                    // marten#5328
_ = new Func<Task>(async () => _ = await session.LoadAsync<Quest>(Guid.Empty));   // marten#5328
_ = new Func<Task>(async () =>
{
    // marten#5361: an enum compared to a VARIABLE, the shape most likely to
    // reach WhereClauseParser's CompileAndInvoke fallback, because an enum
    // operand is commonly wrapped in a Convert node that is neither a bare
    // constant nor a closure member.
    var wanted = Difficulty.Hard;
    _ = await session.Query<Quest>().Where(q => q.Difficulty == wanted).ToListAsync();
});
_ = new Func<Task>(async () => _ = await session.Events.FetchStreamAsync(Guid.Empty));       // marten#5373
_ = new Func<Task>(async () => _ = await session.Events.AggregateStreamAsync<Quest>(Guid.Empty)); // marten#5373
_ = new Func<Task>(async () =>
    _ = await session.Query<Quest>().Where(q => q.Tags.Contains("aot")).ToListAsync());      // marten#5374

// --- Polecat.AspNetCore extension surface -------------------------------
// Touch a StreamMany<T> / StreamOne<T> constructor. These IResult wrappers
// carry method-level [RequiresDynamicCode] / [RequiresUnreferencedCode] on
// ExecuteAsync (System.Text.Json reflective serialize) but the constructor
// itself is AOT-clean. Consumers building a Minimal-API endpoint will
// instantiate via `return new StreamMany<Quest>(query);` — the construction
// site stays in the AOT-clean lane; the framework's ExecuteAsync invocation
// is the part that requires consumer-side STJ source-gen.
var streamMany = new Polecat.AspNetCore.StreamMany<Quest>(query);
var streamOne = new Polecat.AspNetCore.StreamOne<Quest>(query);
_ = streamMany.ContentType;
_ = streamOne.ContentType;

// --- Polecat.EntityFrameworkCore extension surface ----------------------
// Reference the EfCoreSingleStreamProjection<TDoc, TDbContext> closed
// generic via typeof(). The base class's TDoc / TDbContext type parameters
// carry the same EF-Core-shape DAM constraints
// (PublicConstructors|NonPublicConstructors|PublicFields|...|Interfaces on
// TDoc; PublicConstructors on TDbContext) that DbContext.Find<TEntity> and
// Activator.CreateInstance(typeof(TDbContext), ...) require downstream.
// Concrete types (closed-generic instantiations) satisfy DAM implicitly,
// so this typeof() is AOT-clean.
_ = typeof(Polecat.EntityFrameworkCore.EfCoreSingleStreamProjection<,>);
_ = typeof(Polecat.EntityFrameworkCore.EfCoreEventProjection<>);

Console.WriteLine("Polecat AOT smoke OK.");
return 0;

namespace Polecat.AotSmoke
{
    /// <summary>One event type — included via ProjectionBase.IncludedEventTypes.</summary>
    internal enum Difficulty { Easy, Hard }

    internal sealed record QuestStarted(string Title);

    /// <summary>
    /// Self-aggregating aggregate. Static Create mirrors the
    /// SingleStreamProjection convention.
    /// </summary>
    internal sealed class Quest
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;

        /// <summary>#725 / marten#5361: an enum member, compared to a variable above.</summary>
        public Difficulty Difficulty { get; set; }

        /// <summary>#725 / marten#5374: a child collection, filtered above.</summary>
        public List<string> Tags { get; set; } = [];

        public static Quest Create(QuestStarted e) => new() { Title = e.Title };
    }

    /// <summary>
    /// Concrete projection with both generic args closed at compile time —
    /// AOT-safe alternative to <c>Projections.Snapshot&lt;Quest&gt;()</c>.
    ///
    /// `partial` so JasperFx.Events.SourceGenerator (wired in the csproj as
    /// an Analyzer-only PackageReference) emits the [GeneratedEvolver]
    /// dispatcher. JasperFx#276 / Phase 3 removed the FEC fallback for
    /// projection apply dispatch — without `partial` + the SG analyzer,
    /// DocumentStore construction throws InvalidProjectionException at boot.
    /// </summary>
    internal sealed partial class QuestProjection : SingleStreamProjection<Quest, Guid>
    {
    }
}
