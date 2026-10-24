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
    // #812: the store's identity, shared with IEventStore.Subject and IDocumentStoreDiagnostics.Subject
    // so every surface of one store answers with one subject. See DocumentStore.DocumentDiagnostics.cs.
    Uri IDocumentStoreUsageSource.Subject => ((IEventStore)this).Subject;

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
            SubjectUri = ((IDocumentStoreUsageSource)this).Subject,
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
            // simply never asked.
            //
            // polecat#820: a custom ISerializer genuinely cannot be interrogated, and the fallback here
            // used to report "AsInteger" / "CamelCase" for it anyway -- the same confident guess #706
            // removed, one branch over. The contract spells "unknown" as empty.
            EnumStorage = (Options.Serializer as Serialization.Serializer)?.EnumStorage.ToString()
                          ?? string.Empty,
            SerializerCasing = (Options.Serializer as Serialization.Serializer)?.Casing.ToString()
                               ?? string.Empty
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

        // jasperfx#1081: the indexes are read off the same Weasel table model the DDL is rendered from, so
        // the list is complete exactly when that model could be built. It used to be built from
        // mapping.Indexes alone and silently missed the JSON index; reporting "complete" over that list
        // would have told a console an indexed type reads every row.
        var table = TryBuildDocumentTable(mapping);

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
            Indexes = BuildIndexDescriptors(mapping, table, out var indexesReported),
            IndexesReported = indexesReported,

            // A Polecat index's persisted computed column IS the duplicated field: it lifts a JSON
            // member into a real column, which is exactly what a console needs to know to predict
            // whether a filter reads a column or the JSON body.
            DuplicatedFields = mapping.Indexes
                .SelectMany(index => index.JsonPaths.Select((path, i) => new DuplicatedFieldDescriptor
                {
                    MemberPath = MemberPathFor(path, index.MemberChains, i),
                    ColumnName = Storage.DocumentIndex.ColumnNameForPath(path, index.Casing),
                    DbType = index.SqlTypeByPath.TryGetValue(path, out var sqlType)
                        ? sqlType
                        : string.Empty
                }))
                .GroupBy(x => x.ColumnName, StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .ToList(),

            // jasperfx#1081: every duplicated column Polecat has is an index's computed column, read from
            // the same declarations the table model adds them from — complete whenever that model built.
            // (A foreign key's computed column is not reported: it holds a reference id, not a member a
            // console would filter on, and it is the same set either way for a type with no foreign key.)
            DuplicatedFieldsReported = table != null,

            // jasperfx#1077: which metadata columns THIS type's table has, so a console offers only the
            // filters this type can honour (the store-level DocumentMetadata answers "any type").
            Metadata = new DocumentMetadataCapabilities
            {
                StoreType = "Polecat",
                Version = true,
                LastModified = true,
                // #234: only a conjoined table carries tenant_id.
                TenantId = mapping.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined,
                SoftDelete = mapping.DeleteStyle == JasperFx.DeleteStyle.SoftDelete,
                CorrelationId = mapping.Metadata.CorrelationId.Enabled,
                CausationId = mapping.Metadata.CausationId.Enabled,
                LastModifiedBy = mapping.Metadata.LastModifiedBy.Enabled
            }
        };
    }

    /// <summary>
    ///     polecat#820 (jasperfx#870 §5): <see cref="DocumentIndexDescriptor.Members" /> and
    ///     <see cref="DuplicatedFieldDescriptor.MemberPath" /> are member paths "dotted and in declaration
    ///     casing" (<c>ShipTo.City</c>), which is what a console matches a predicate's members against. They
    ///     were reported as the rendered JSON path (<c>$.shipTo.city</c>), which matched nothing. An index
    ///     declared from a lambda keeps the member chain it came from (#510); a hand-written JSON path has none
    ///     and is reported as written, because its declaration casing is unknowable.
    /// </summary>
    private static string[] MemberPathsFor(string[] jsonPaths, System.Reflection.MemberInfo[][]? chains)
        => jsonPaths.Select((path, i) => MemberPathFor(path, chains, i)).ToArray();

    private static string MemberPathFor(string jsonPath, System.Reflection.MemberInfo[][]? chains, int index)
        => chains != null && index < chains.Length && chains[index].Length > 0
            ? string.Join(".", chains[index].Select(m => m.Name))
            : jsonPath;

    private static DocumentTable? TryBuildDocumentTable(Storage.DocumentMapping mapping)
    {
        try
        {
            return new DocumentTable(mapping);
        }
        catch (Exception)
        {
            // The DDL beside it reports the same failure as text; here it means "not reported".
            return null;
        }
    }

    /// <summary>
    ///     jasperfx#1081: one descriptor per secondary index the table model declares — the computed-column
    ///     indexes with their member paths, and the SQL Server 2025 JSON index over <c>data</c>.
    ///     <paramref name="reported" /> is true only when every modeled index is in the list.
    /// </summary>
    private static List<DocumentIndexDescriptor> BuildIndexDescriptors(Storage.DocumentMapping mapping,
        DocumentTable? table, out bool reported)
    {
        var descriptors = mapping.Indexes.Select(index => new DocumentIndexDescriptor
        {
            Name = index.GetIndexName(mapping.TableName),
            IsUnique = index.IsUnique,
            Predicate = index.Predicate,
            Members = MemberPathsFor(index.JsonPaths, index.MemberChains),
            Columns = index.JsonPaths
                .Select(path => Storage.DocumentIndex.ColumnNameForPath(path, index.Casing))
                .ToArray()
        }).ToList();

        if (mapping.JsonIndexes.Count == 1)
        {
            var jsonIndex = mapping.JsonIndexes[0];
            descriptors.Add(new DocumentIndexDescriptor
            {
                Name = jsonIndex.GetIndexName(mapping.TableName),
                // A JSON index with no paths indexes the whole document.
                Members = MemberPathsFor(jsonIndex.JsonPaths, jsonIndex.MemberChains),
                Columns = ["data"],
                Method = "JSON"
            });
        }

        reported = table != null
                   && table.Indexes.All(modeled =>
                       descriptors.Any(d => string.Equals(d.Name, modeled.Name, StringComparison.OrdinalIgnoreCase)));

        return descriptors;
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
