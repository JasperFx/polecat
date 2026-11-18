using JasperFx;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;
using Weasel.Core.Migrations;

namespace Polecat.Tests.Storage;

/// <summary>
///     #573 — a store's declared schema must include the document tables its <em>configuration</em>
///     implies, not only the ones some session happened to touch.
/// </summary>
/// <remarks>
///     <para>
///         Reported against <c>db-ef-migration add</c>: the same projection registered on Marten and
///         on Polecat produced a migration with the document table on Marten and without it on
///         Polecat. The cause is not the migration command — it is that Polecat materializes a
///         document provider lazily, on the first <c>GetProvider&lt;T&gt;()</c>, and the command
///         configures a store, asks it for its schema, and exits without ever opening a session. The
///         registry was empty, so <c>BuildFeatureSchemas()</c> declared no document feature at all.
///     </para>
///     <para>
///         These tests go at <c>BuildFeatureSchemas()</c> rather than at the command, because that is
///         where the decision is made and every consumer of it — migrations, the stateful resource
///         model, <c>ApplyAllConfiguredChangesToDatabaseAsync</c> — inherits the answer.
///     </para>
/// </remarks>
public class configured_document_tables_tests
{
    private static DocumentStore ConfigureOnly(Action<StoreOptions> configure) =>
        DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "configured_doc_tables";
            opts.AutoCreateSchemaObjects = AutoCreate.None;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            configure(opts);
        });

    private static IReadOnlyList<string> DeclaredTableNames(DocumentStore store) =>
        store.Database.BuildFeatureSchemas()
            .SelectMany(x => x.Objects)
            .OfType<Weasel.SqlServer.Tables.Table>()
            .Select(x => x.Identifier.Name)
            .ToList();

    /// <summary>
    ///     The reported case: a projection is registered, nothing else happens, and the store must
    ///     still declare the projection's document table.
    /// </summary>
    [Fact]
    public void a_registered_projection_declares_its_document_table_without_a_session()
    {
        using var store = ConfigureOnly(opts =>
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline));

        DeclaredTableNames(store).ShouldContain("pc_doc_questparty");
    }

    /// <summary>
    ///     The other configuration route to the same place. Schema.For&lt;T&gt;() was equally lazy.
    /// </summary>
    [Fact]
    public void an_explicit_schema_registration_declares_its_document_table_without_a_session()
    {
        using var store = ConfigureOnly(opts => opts.Schema.For<ConfiguredOnlyDoc>());

        DeclaredTableNames(store).ShouldContain("pc_doc_configuredonlydoc");
    }

    /// <summary>
    ///     A store that configures no documents of its own must not grow any, so the fix cannot be
    ///     "materialize everything" or "declare the document feature unconditionally".
    /// </summary>
    /// <remarks>
    ///     The one <c>pc_doc_*</c> table a bare store does declare is
    ///     <c>pc_doc_deadletterevent</c> — the async daemon's dead letter store, which is event-store
    ///     infrastructure rather than anything the application configured, and which predates this
    ///     change. Asserted by name rather than skipped, so that if the set of infrastructure document
    ///     tables ever changes, this test is where it is noticed.
    /// </remarks>
    [Fact]
    public void a_store_with_no_document_configuration_declares_no_document_tables_of_its_own()
    {
        using var store = ConfigureOnly(_ => { });

        DeclaredTableNames(store)
            .Where(x => x.StartsWith("pc_doc_"))
            .ShouldBe(["pc_doc_deadletterevent"]);
    }

    /// <summary>
    ///     The exclusion that makes the rest of this safe. A document whose storage has been
    ///     redirected — the EF Core projections are the shipped case — must NOT get a Polecat table,
    ///     because Polecat does not store it. Getting this wrong would add a spurious table to the
    ///     very EF Core migration #573 is about.
    /// </summary>
    [Fact]
    public void a_document_with_custom_projection_storage_declares_no_polecat_table()
    {
        using var store = ConfigureOnly(opts =>
        {
            opts.Projections.Snapshot<QuestParty>(SnapshotLifecycle.Inline);

            // Stand in for AddEfCoreProjection<TDoc, TDbContext>(): the registration that redirects
            // storage for this document type away from Polecat's tables. The factory is never
            // invoked here — only its presence in the keyed registry matters, which is exactly the
            // signal the production code reads.
            opts.CustomProjectionStorageProviders[typeof(QuestParty)] =
                (_, _) => throw new NotSupportedException("not invoked by this test");
        });

        DeclaredTableNames(store).ShouldNotContain("pc_doc_questparty");
    }
}

public class ConfiguredOnlyDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
