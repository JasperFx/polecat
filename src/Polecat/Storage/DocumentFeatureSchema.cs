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

        foreach (var provider in _options.Providers.AllProviders)
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
}
