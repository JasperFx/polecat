using System.IO;
using JasperFx.Descriptors;
using JasperFx.Events;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat;

public partial class DocumentStore : IDocumentStoreUsageSource
{
    Uri IDocumentStoreUsageSource.Subject => Database.DatabaseUri;

    /// <summary>
    /// Build a <see cref="DocumentStoreUsage"/> snapshot for monitoring tools
    /// (CritterWatch). Mirrors the structure of Marten's implementation —
    /// hand-built first-class properties for the operationally-interesting
    /// bits, flat OptionValues for the secondary settings, and a per-document
    /// <see cref="DocumentMappingDescriptor"/> with the SQL Server DDL each
    /// mapping will emit.
    /// </summary>
    Task<DocumentStoreUsage?> IDocumentStoreUsageSource.TryCreateUsage(CancellationToken token)
    {
        var usage = new DocumentStoreUsage
        {
            Subject = "Polecat.DocumentStore",
            SubjectUri = Database.DatabaseUri,
            Version = GetType().Assembly.GetName().Version?.ToString(),
            Database = new DatabaseUsage
            {
                Cardinality = DatabaseCardinality.Single,
                MainDatabase = Database.Describe(),
            },
            // Polecat doesn't carry a separate StoreName concept yet — fall back
            // to the database identifier so the descriptor still has a stable
            // identity.
            StoreName = Database.Identifier,
            DatabaseSchemaName = Options.DatabaseSchemaName,
            AutoCreateSchemaObjects = Options.AutoCreateSchemaObjects.ToString(),
            // Polecat's serializer-resident EnumStorage isn't exposed at the
            // store level today; default to AsInteger for parity with the
            // typical configuration.
            EnumStorage = "AsInteger",
        };

        // Polecat doesn't have a parallel set of code-generation properties on
        // StoreOptions today, so the CodeGeneration child stays null. The
        // descriptor remains forward-compatible: when Polecat grows code-gen
        // settings, they'll land here.

        // Per-document-type mappings. Polecat materializes providers
        // lazily on first GetProvider<T>() call, so Options.Providers.
        // AllProviders is empty for stores that haven't opened sessions
        // yet — which is exactly the state CritterWatch sees on a fresh
        // boot. Force materialization from two sources:
        //   1. Explicit Schema.For<T>() registrations (Schema.Expressions)
        //   2. Aggregate document types declared by registered projections
        //      (e.g. SingleStreamProjection<TDoc, TId>) — most Polecat
        //      services rely on this path rather than Schema.For<T>().
        var migrator = new SqlServerMigrator();
        var seenDocumentTypes = new HashSet<Type>();

        foreach (var expr in Options.Schema.Expressions)
        {
            var exprType = expr.GetType();
            if (!exprType.IsGenericType) continue;

            var documentType = exprType.GetGenericArguments()[0];
            if (seenDocumentTypes.Add(documentType))
            {
                // Triggers the GetOrAdd path — provider lands in the registry.
                Options.Providers.GetProvider(documentType);
            }
        }

        foreach (var aggregate in Options.Projections.All.OfType<JasperFx.Events.Aggregation.IAggregateProjection>())
        {
            if (seenDocumentTypes.Add(aggregate.AggregateType))
            {
                Options.Providers.GetProvider(aggregate.AggregateType);
            }
        }

        foreach (var provider in Options.Providers.AllProviders.OrderBy(p => p.Mapping.Alias))
        {
            usage.Documents.Add(BuildMappingDescriptor(provider.Mapping, migrator));
        }

        // Flat OptionValues — only the ones Polecat actually has equivalents
        // for. Cluster mapping vs Marten:
        //   Marten side                          Polecat side
        //   ----------------------------------   ----------------------------------
        //   TenantIdStyle                        (n/a — Polecat doesn't have it)
        //   DefaultTenantUsageEnabled            (n/a)
        //   RlsTenantSessionSetting              (n/a)
        //   NameDataLength                       (n/a — SQL Server uses 128)
        //   ApplyChangesLockId                   (n/a)
        //   CommandTimeout                       Options.CommandTimeout
        //   UseStickyConnectionLifetimes         (n/a)
        //   UpdateBatchSize                      (n/a)
        //   DuplicatedFieldEnumStorage           (n/a yet)
        //   OpenTelemetryTrackConnections        Options.OpenTelemetry.TrackConnections
        //   DisableNpgsqlLogging                 (n/a — Postgres-specific)
        //   HiloMaxLo                            Options.HiloSequenceDefaults.MaxLo
        //   ReadSessionPreference                (n/a)
        //   WriteSessionPreference               (n/a)
        //
        // Polecat-specific:
        //   UseNativeJsonType                    (json column type policy)

        usage.AddValue(nameof(Options.CommandTimeout), Options.CommandTimeout);
        usage.AddValue("OpenTelemetryTrackConnections", Options.OpenTelemetry.TrackConnections.ToString());
        usage.AddValue("HiloMaxLo", Options.HiloSequenceDefaults.MaxLo);
        usage.AddValue(
            "HiloMaxAdvanceToNextHiAttempts",
            Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts);
        usage.AddValue(nameof(Options.UseNativeJsonType), Options.UseNativeJsonType);

        return Task.FromResult<DocumentStoreUsage?>(usage);
    }

    private DocumentMappingDescriptor BuildMappingDescriptor(
        Storage.DocumentMapping mapping,
        SqlServerMigrator migrator)
    {
        var ddl = WriteSchemaCreationDdl(mapping, migrator);

        // Polecat's `mapping.Alias` is the polymorphic doc-type discriminator
        // column value (defaults to "base"), not the table-name suffix.
        // Convert the type name so the descriptor's `Alias` field carries
        // a Marten-equivalent table-name suffix the operator can correlate
        // with the actual table.
        var tableNameSuffix = mapping.DocumentType.Name.ToLowerInvariant();

        return new DocumentMappingDescriptor
        {
            DocumentType = TypeDescriptor.For(mapping.DocumentType),
            DatabaseSchemaName = mapping.DatabaseSchemaName,
            Alias = tableNameSuffix,
            // Polecat doesn't expose an IIdGeneration-style strategy; the IdType
            // alone is the most informative thing we have.
            IdStrategy = mapping.IdType.Name,
            TenancyStyle = mapping.TenancyStyle.ToString(),
            DeleteStyle = mapping.DeleteStyle.ToString(),
            UseOptimisticConcurrency = mapping.UseOptimisticConcurrency,
            UseNumericRevisions = mapping.UseNumericRevisions,
            SubClassCount = mapping.SubClasses.Count,
            // Polecat doesn't have a partition strategy today — leave null.
            PartitioningStrategy = null,
            Ddl = ddl,
        };
    }

    private static string WriteSchemaCreationDdl(
        Storage.DocumentMapping mapping,
        SqlServerMigrator migrator)
    {
        try
        {
            using var writer = new StringWriter();
            var table = new DocumentTable(mapping);
            table.WriteCreateStatement(migrator, writer);
            return writer.ToString();
        }
        catch (Exception ex)
        {
            return $"-- Failed to generate DDL: {ex.Message}";
        }
    }
}
