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
            yield return new DocumentTable(provider.Mapping);
        }
    }
}
