using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.Descriptors;
using Microsoft.Data.SqlClient;
using Polecat.Events;
using Polecat.Events.Schema;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Storage;

/// <summary>
///     Manages the Polecat database schema lifecycle using Weasel.
///     Handles auto-creation and migration of event store tables.
/// </summary>
public class PolecatDatabase : DatabaseBase<SqlConnection>
{
    private readonly StoreOptions _options;
    private readonly EventGraph _events;

    public PolecatDatabase(StoreOptions options)
        : base(
            new DefaultMigrationLogger(),
            options.AutoCreateSchemaObjects,
            new SqlServerMigrator(),
            "Polecat",
            options.ConnectionString)
    {
        _options = options;
        _events = new EventGraph(options);
    }

    internal EventGraph Events => _events;

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return [new EventStoreFeatureSchema(_events)];
    }

    public override DatabaseDescriptor Describe()
    {
        var builder = new SqlConnectionStringBuilder(_options.ConnectionString);
        return new DatabaseDescriptor
        {
            Engine = SqlServerProvider.EngineName,
            ServerName = builder.DataSource ?? string.Empty,
            DatabaseName = builder.InitialCatalog ?? string.Empty,
            Subject = GetType().FullNameInCode(),
            Identifier = Identifier
        };
    }
}
