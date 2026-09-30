using System.Globalization;
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
    async Task<DocumentStoreUsage?> IDocumentStoreUsageSource.TryCreateUsage(CancellationToken token)
    {
        var usage = new DocumentStoreUsage
        {
            Subject = "Polecat.DocumentStore",
            SubjectUri = Database.DatabaseUri,
            Version = GetType().Assembly.GetName().Version?.ToString(),
            // #675: same defect as the event-store descriptor next door — a hard-coded
            // single-database shape on a store that may well have one database per tenant. Both read
            // the tenancy now.
            Database = await DescribeDatabasesAsync(token).ConfigureAwait(false),
            // Per-store logical name (default "Main"; the marker type name for ancillary stores) so the
            // descriptor is distinguishable across stores, mirroring Marten. See polecat#207.
            StoreName = Options.StoreName,
            DatabaseSchemaName = Options.DatabaseSchemaName,
            AutoCreateSchemaObjects = Options.AutoCreateSchemaObjects.ToString(),

            // #706: read from the serializer rather than hard-coded. This said "AsInteger" whatever
            // the store was actually configured with -- so a console reading the descriptor to
            // interpret stored JSON was told the wrong thing for every store using string enums, and
            // told it confidently. The serializer has exposed both of these all along; the descriptor
            // simply never asked. Falls back only for a custom ISerializer, which genuinely cannot be
            // interrogated.
            EnumStorage = (Options.Serializer as Serialization.Serializer)?.EnumStorage.ToString()
                          ?? "AsInteger",
            SerializerCasing = (Options.Serializer as Serialization.Serializer)?.Casing.ToString()
                               ?? "CamelCase"
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
        // #573: shared with PolecatDatabase.BuildFeatureSchemas, which needs the same set for DDL.
        // This used to be a local walk over IAggregateProjection.AggregateType, which missed every
        // other type a projection publishes and had no exclusion for documents whose storage is not
        // Polecat's — so the console could describe a pc_doc_ table the migration would never create.
        Internal.ConfiguredDocumentProviders.MaterializeConfigured(Options);

        foreach (var provider in Options.Providers.AllProviders.OrderBy(p => p.Mapping.Alias))
        {
            usage.Documents.Add(BuildMappingDescriptor(provider.Mapping, migrator));
        }

        // Flat OptionValues — only the ones Polecat actually has equivalents
        // for. Cluster mapping vs Marten:
        //   Marten side                          Polecat side
        //   ----------------------------------   ----------------------------------
        //   TenantIdStyle                        (n/a — Polecat doesn't have it)
        //   DefaultTenantUsageEnabled            Options.DefaultTenantUsageEnabled (#514)
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

        usage.AddValue(nameof(Options.DefaultTenantUsageEnabled), Options.DefaultTenantUsageEnabled);
        usage.AddValue(nameof(Options.CommandTimeout), Options.CommandTimeout);
        usage.AddValue("OpenTelemetryTrackConnections", Options.OpenTelemetry.TrackConnections.ToString());
        usage.AddValue("HiloMaxLo", Options.HiloSequenceDefaults.MaxLo);
        usage.AddValue(
            "HiloMaxAdvanceToNextHiAttempts",
            Options.HiloSequenceDefaults.MaxAdvanceToNextHiAttempts);
        usage.AddValue(nameof(Options.UseNativeJsonType), Options.UseNativeJsonType);

        // jasperfx#475 — advertise which document metadata Polecat captures so
        // store-aware consumers (CritterWatch) gate document-query facets by what is
        // actually persisted. Version / last-modified / tenant / soft-delete are
        // universal facets in Polecat and keep the descriptor's default of true; the
        // opt-in columns (correlation/causation/last-modified-by) are only queryable
        // where some document mapping has enabled them.
        var mappings = Options.Providers.AllProviders.Select(p => p.Mapping).ToList();
        usage.DocumentMetadata = new DocumentMetadataCapabilities
        {
            StoreType = "Polecat",
            CorrelationId = mappings.Any(m => m.Metadata.CorrelationId.Enabled),
            CausationId = mappings.Any(m => m.Metadata.CausationId.Enabled),
            LastModifiedBy = mappings.Any(m => m.Metadata.LastModifiedBy.Enabled)
        };

        return usage;
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
            SubClasses = mapping.SubClasses.Select(x => TypeDescriptor.For(x.DocumentType)).ToArray(),
            PartitioningStrategy = PartitioningStrategyName(mapping.Partitioning),
            Partitioning = BuildPartitioning(mapping.Partitioning),
            Ddl = ddl,

            // #706 / jasperfx#870: the indexes and duplicated columns in STRUCTURED form, so a console
            // can say whether a filter on a member can use an index without parsing Ddl. Both come
            // from the same DocumentIndex declarations the table is built from, which is what keeps
            // them from drifting from the schema they describe.
            Indexes = mapping.Indexes.Select(index => new DocumentIndexDescriptor
            {
                Name = index.GetIndexName(mapping.TableName),
                IsUnique = index.IsUnique,
                Predicate = index.Predicate,
                Members = index.JsonPaths.ToArray(),
                Columns = index.JsonPaths
                    .Select(path => Storage.DocumentIndex.ColumnNameForPath(path, index.Casing))
                    .ToArray()
            }).ToList(),

            // A Polecat index's persisted computed column IS the duplicated field: it lifts a JSON
            // member into a real column, which is exactly what a console needs to know to predict
            // whether a filter reads a column or the JSON body.
            DuplicatedFields = mapping.Indexes
                .SelectMany(index => index.JsonPaths.Select(path => new DuplicatedFieldDescriptor
                {
                    MemberPath = path,
                    ColumnName = Storage.DocumentIndex.ColumnNameForPath(path, index.Casing),
                    DbType = index.SqlTypeByPath.TryGetValue(path, out var sqlType)
                        ? sqlType
                        : string.Empty
                }))
                .GroupBy(x => x.ColumnName, StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .ToList(),
        };
    }

    /// <summary>
    ///     Project Polecat's declarative RANGE partitioning (#211) into the structured
    ///     <see cref="PartitioningDescriptor" /> CritterWatch renders. SQL Server partitions are
    ///     anonymous, so the boundary values stand in for the partition names.
    /// </summary>
    private static PartitioningDescriptor? BuildPartitioning(Storage.DocumentPartitioning? partitioning)
    {
        if (partitioning == null)
        {
            return null;
        }

        var strategy = PartitioningStrategyName(partitioning)!;

        // #386: a rolling window has no declared boundary list — the window is a function of the policy
        // and the clock, so report the boundaries it expects to exist right now.
        var names = partitioning.RollingWindow is { } rollingWindow
            ? rollingWindow.Boundaries()
            : partitioning.Boundaries
                .Select(b => Convert.ToString(b, CultureInfo.InvariantCulture) ?? string.Empty)
                .ToArray();

        return new PartitioningDescriptor { Strategy = strategy, PartitionNames = names };
    }

    private static string? PartitioningStrategyName(Storage.DocumentPartitioning? partitioning) =>
        partitioning switch
        {
            null => null,
            { RollingWindow: not null } => "RollingRange",
            _ => "Range"
        };

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
