using System.Diagnostics.CodeAnalysis;
using JasperFx.CommandLine;
using JasperFx.CommandLine.Descriptions;
using JasperFx.Descriptors;
using JasperFx.Resources;
using Weasel.Core.CommandLine;

namespace Polecat;

/// <summary>
///     Exposes the Polecat store's database(s) to JasperFx's resource model so the
///     idiomatic <c>services.AddResourceSetupOnStartup()</c> (and the <c>resources</c>
///     CLI commands) provision the Polecat schema with no Polecat-specific call.
///     Mirrors Marten's <c>MartenSystemPart</c>: <c>FindResources()</c> wraps each
///     tenant database in a Weasel <see cref="DatabaseResource" />.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "WriteToConsole reflects over the store options for the dev-time 'describe' CLI command only; the resource-setup path (FindResources/Setup) is reflection-free. AOT consumers run resource setup, not describe.")]
[UnconditionalSuppressMessage("Trimming", "IL2046",
    Justification = "Class-level: override RUC mismatch — the base WriteToConsole does not yet carry RequiresUnreferencedCode. Suppressed locally to match Marten's MartenSystemPart.")]
internal class PolecatSystemPart : SystemPartBase
{
    public static Uri PolecatStoreUri { get; } = new("polecat://store");

    private readonly IDocumentStore _store;

    protected PolecatSystemPart(IDocumentStore store, string title, Uri subjectUri) : base(title, subjectUri)
    {
        _store = store;
    }

    public PolecatSystemPart(IDocumentStore store) : this(store, "Polecat", PolecatStoreUri)
    {
    }

    public override Task WriteToConsole()
    {
        var description = OptionsDescription.For(_store);
        OptionDescriptionWriter.Write(description);
        return Task.CompletedTask;
    }

    public override async ValueTask<IReadOnlyList<IStatefulResource>> FindResources()
    {
        // Await the tenancy build so dynamic (master-table / separate-database)
        // tenancies enumerate every tenant database, exactly like MartenSystemPart.
        var databases = await _store.Options.Tenancy!.BuildDatabasesAsync().ConfigureAwait(false);
        return databases.Select(x => new DatabaseResource(x, SubjectUri)).ToArray();
    }
}

/// <summary>
///     Marker-typed variant of <see cref="PolecatSystemPart" /> for ancillary store
///     registrations (multi-store apps where each <typeparamref name="T" /> store
///     contributes its own databases to the resource model). Mirrors
///     <c>MartenSystemPart&lt;T&gt;</c>.
/// </summary>
internal class PolecatSystemPart<T> : PolecatSystemPart where T : IDocumentStore
{
    public PolecatSystemPart(T store)
        : base(store, $"Polecat {typeof(T).Name}", new Uri("polecat://" + typeof(T).Name.ToLowerInvariant()))
    {
    }
}
