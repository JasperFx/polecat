using Polecat.Internal;
using Polecat.Schema.Identity.Sequences;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Weasel feature schema that yields all document tables and the HiLo sequence table.
///     Participates in ApplyAllConfiguredChangesToDatabaseAsync() for schema migration.
/// </summary>
internal class DocumentFeatureSchema : FeatureSchemaBase
{
    private readonly StoreOptions _options;

    public DocumentFeatureSchema(StoreOptions options)
        : base("Documents", new SqlServerMigrator())
    {
        _options = options;
    }

    public override Type StorageType => typeof(DocumentFeatureSchema);

    protected override IEnumerable<ISchemaObject> schemaObjects()
    {
        // HiLo table first — numeric ID document types depend on it
        if (_options.Providers.AllProviders.Any(p => p.Mapping.IsNumericId))
        {
            yield return new HiloTable(_options.DatabaseSchemaName);
        }

        // #684: a referenced table has to be yielded BEFORE the table whose foreign key points at it.
        //
        // This did not matter until the foreign keys became modeled objects: the generated creation
        // script renders each object's CREATE in the order a feature schema yields them, with no
        // migration involved and therefore none of SchemaMigration's deferral, so a table yielded first
        // carried a constraint against a table that did not exist yet -- "Foreign key '...' references
        // invalid table '...'". Provider order is dictionary order, so whether a script ran at all was
        // luck. The whole-database MIGRATION path was never affected, because there every table is in one
        // SchemaMigration and Weasel defers the constraint itself.
        foreach (var provider in InDependencyOrder(_options.Providers.AllProviders))
        {
            // #255: externally-managed partitioned tables are intentionally excluded from the bulk
            // reconciliation path. The whole-database migration applies a single (global) AutoCreate,
            // so leaving them in would reconcile their partition boundaries back to the declared
            // initial set and clobber the partitions the app/DBA manages at runtime (SPLIT/SWITCH/DROP
            // for retention). They are provisioned once, on first use, by DocumentTableEnsurer with
            // AutoCreate.CreateOnly and never reconciled by either path.
            if (provider.Mapping.Partitioning is { ExternallyManaged: true })
            {
                continue;
            }

            yield return new DocumentTable(provider.Mapping);
        }
    }

    /// <summary>
    ///     The providers, ordered so that every document type appears after the types its foreign keys
    ///     reference.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A depth-first walk with a visited set, which is a topological sort for the shape that can
    ///         occur here: foreign keys between document types form a small, usually shallow graph.
    ///     </para>
    ///     <para>
    ///         ⚠️ A <b>cycle</b> is left in whatever order it is reached rather than being an error. Two
    ///         document types referencing each other cannot be created in any order that satisfies both
    ///         constraints — that needs the deferral only a migration has — so refusing here would reject
    ///         a configuration the migration path handles. The script for such a store is the thing that
    ///         will not run, and it would not run whatever order this chose.
    ///     </para>
    ///     <para>
    ///         A referenced type with no provider of its own is skipped rather than materialized: pulling
    ///         one in from here would mutate the collection this is enumerating.
    ///     </para>
    /// </remarks>
    private IEnumerable<DocumentProvider> InDependencyOrder(IEnumerable<DocumentProvider> providers)
    {
        var byType = new Dictionary<Type, DocumentProvider>();
        foreach (var provider in providers)
        {
            byType[provider.Mapping.DocumentType] = provider;
        }

        var ordered = new List<DocumentProvider>(byType.Count);
        var visited = new HashSet<Type>();
        var onPath = new HashSet<Type>();

        foreach (var type in byType.Keys.ToList())
        {
            Visit(type);
        }

        return ordered;

        void Visit(Type type)
        {
            if (!visited.Add(type)) return;
            if (!byType.TryGetValue(type, out var provider)) return;

            // onPath is what stops a cycle from recursing forever. The type is still emitted, just in
            // the order the walk reached it -- see the remarks.
            if (onPath.Add(type))
            {
                foreach (var fk in provider.Mapping.ForeignKeys)
                {
                    Visit(fk.ReferenceDocumentType);
                }

                onPath.Remove(type);
            }

            ordered.Add(provider);
        }
    }
}
