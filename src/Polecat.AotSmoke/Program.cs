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
//     is on the codegen path — Polecat.CodeGeneration provides the static
//     manifest for AOT consumers).
//   - Async daemon / projection runtime (also codegen-backed).

using JasperFx.Events.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polecat;
using Polecat.AotSmoke;

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

    // Registering the snapshot projection pulls QuestStarted into the
    // EventGraph via ProjectionBase.IncludedEventTypes, so no separate
    // EventGraph.AddEventType call is needed here.
    opts.Projections.Snapshot<Quest>(SnapshotLifecycle.Inline);
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
// Construct (but never enumerate) a Where-filtered IQueryable through the
// LINQ provider. This exercises IQuerySession.Query<T> + the LINQ extension
// surface (PolecatLinqQueryProvider) — both class-level RUC/RDC suppressed
// today. Materialization (ToListAsync etc.) would require a DB.
var query = session.Query<Quest>().Where(q => q.Title == "smoke-test");
_ = query.Expression;

Console.WriteLine("Polecat AOT smoke OK.");
return 0;

namespace Polecat.AotSmoke
{
    /// <summary>One event type — included via ProjectionBase.IncludedEventTypes.</summary>
    internal sealed record QuestStarted(string Title);

    /// <summary>
    /// Self-aggregating aggregate so Projections.Snapshot&lt;T&gt; has a target.
    /// Static Create mirrors the SingleStreamProjection convention.
    /// </summary>
    internal sealed class Quest
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;

        public static Quest Create(QuestStarted e) => new() { Title = e.Title };
    }
}
