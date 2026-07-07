using JasperFx;
using Weasel.Core.Identity;
using Weasel.Storage;

namespace Polecat.Storage;

/// <summary>
///     Builds a <see cref="DocumentStorageDescriptor{TDoc,TId}" /> from a Polecat
///     <see cref="DocumentMapping" /> — the SQL Server counterpart of Marten's
///     <c>DocumentStorageDescriptorBuilder</c> (#273 phase D, slice 1). Produces the binder
///     arrays and MERGE-based SQL in lockstep: column order and <c>?</c> parameter-slot order
///     must agree exactly with the shared closed-shape write operations, which bind
///     [tenant when conjoined] → id → data → each client-side binder → trailing concurrency
///     guard (see <c>ClosedShapeUpsertOperation.BindPreOnConflictParameters</c>).
/// </summary>
/// <remarks>
///     Slice 1 scope: Off + Optimistic concurrency, no hierarchy, no partitioning, no soft
///     delete, no numeric revisions. Polecat divergences from Marten handled here:
///     <list type="bullet">
///         <item>
///             <c>tenant_id</c> is ALWAYS present in pc tables. Conjoined mappings use the
///             shared operations' leading tenant slot; single-tenant mappings instead put a
///             session-sourced <see cref="DocumentTenantIdBinder{TDoc}" /> in the write binder
///             array so the default tenant id is written through the ordinary binder loop.
///         </item>
///         <item>
///             The always-present numeric <c>version</c> column is maintained as literal SQL
///             (1 on insert, +1 on update) — it is not the concurrency vehicle. Optimistic
///             concurrency rides the separate <c>guid_version</c> column via the descriptor's
///             version binder, matching Polecat's existing operations.
///         </item>
///         <item>
///             <c>last_modified</c>/<c>created_at</c> are server-side via
///             <c>SYSDATETIMEOFFSET()</c> (weasel#336 injectable timestamp SQL).
///         </item>
///     </list>
/// </remarks>
internal static class SqlServerDocumentStorageDescriptorBuilder
{
    private const string ServerTimestamp = "SYSDATETIMEOFFSET()";

    public static DocumentStorageDescriptor<TDoc, TId> Build<TDoc, TId>(
        DocumentMapping mapping,
        IIdentification<TDoc, TId> identification,
        StoreOptions options)
        where TDoc : notnull
        where TId : notnull
    {
        var dialect = SqlServerStorageDialect<TId>.Instance;
        var writeBinders = new List<IDocumentMetadataBinder<TDoc>>();
        var readBinders = new List<IDocumentMetadataBinder<TDoc>>();

        DocumentVersionBinder<TDoc>? versionBinder = null;
        DocumentRevisionBinder<TDoc>? revisionBinder = null;
        var versionReadIndex = -1;
        var docTypeReadIndex = -1;
        Func<string, Type>? resolveDocumentType = null;
        var isConjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        // Hierarchy: doc_type discriminator, written on every save and read FIRST so the
        // hierarchical selectors can dispatch deserialization (read layout: id, data,
        // doc_type, version, ...).
        if (mapping.IsHierarchy())
        {
            resolveDocumentType = mapping.TypeFor;
            var docTypeBinder = new DocumentDocTypeBinder<TDoc>("doc_type", dialect, mapping.AliasFor);
            writeBinders.Add(docTypeBinder);
            docTypeReadIndex = readBinders.Count;
            readBinders.Add(docTypeBinder);
        }

        // tenant_id: pc tables always carry it. Conjoined uses the shared operations'
        // leading tenant parameter slot (NOT a binder); single-tenant writes the session's
        // (default) tenant id through a regular binder slot.
        // Parity with the bespoke load path: ITenanted documents get tenant_id applied on load.
        var tenantMember = mapping.Metadata.TenantId.Member
                           ?? (typeof(Polecat.Metadata.ITenanted).IsAssignableFrom(typeof(TDoc))
                               ? typeof(TDoc).GetProperty(nameof(Polecat.Metadata.ITenanted.TenantId))
                               : null);
        if (!isConjoined)
        {
            // Shared DocumentTenantIdBinder is read-only (Marten binds tenant inline);
            // Polecat's write-capable binder sources the session's tenant id.
            var tenantBinder = new PolecatTenantIdBinder<TDoc>(mapping.Metadata.TenantId.Name, tenantMember);
            writeBinders.Add(tenantBinder);
            if (tenantMember is not null)
            {
                readBinders.Add(tenantBinder);
            }
        }
        else if (tenantMember is not null)
        {
            readBinders.Add(new DocumentTenantIdBinder<TDoc>(mapping.Metadata.TenantId.Name, tenantMember));
        }

        // guid_version: the optimistic-concurrency vehicle (uniqueidentifier), separate from
        // the always-present numeric version column that MERGE maintains as literal SQL.
        if (mapping.UseOptimisticConcurrency)
        {
            // Parity with the bespoke load path: IVersioned documents get guid_version
            // applied to their Version member on every load.
            var versionMember = typeof(IVersioned).IsAssignableFrom(typeof(TDoc))
                ? typeof(TDoc).GetProperty(nameof(IVersioned.Version))
                : null;
            versionBinder = new DocumentVersionBinder<TDoc>("guid_version", dialect, versionMember);
            writeBinders.Add(versionBinder);
            versionReadIndex = readBinders.Count;
            readBinders.Add(versionBinder);
        }
        else if (mapping.UseNumericRevisions)
        {
            // Numeric revisions ride the always-present version column itself; the MERGE
            // CASE expressions handle auto-increment (Revision = 0) vs explicit revisions.
            var revisionMember = mapping.Metadata.Version.Member
                                 ?? typeof(TDoc).GetProperty("Version");
            var columnType = mapping.UseLongRevisions ? StorageColumnType.Long : StorageColumnType.Int;
            revisionBinder = new DocumentRevisionBinder<TDoc>("version", dialect, revisionMember, columnType);
            writeBinders.Add(revisionBinder);
            versionReadIndex = readBinders.Count;
            readBinders.Add(revisionBinder);
        }

        // dotnet_type: always written, never selected (matches Polecat's current SELECTs).
        writeBinders.Add(new DocumentDotNetTypeBinder<TDoc>(mapping.Metadata.DotNetType.Name, dialect));

        // last_modified: always in the table, server-side value.
        var lastModified = new DocumentLastModifiedBinder<TDoc>(
            mapping.Metadata.LastModified.Name, mapping.Metadata.LastModified.Member, ServerTimestamp);
        writeBinders.Add(lastModified);
        if (mapping.Metadata.LastModified.Member is not null)
        {
            readBinders.Add(lastModified);
        }

        // created_at: read-only projection; the INSERT branch bakes SYSDATETIMEOFFSET().
        // Parity: ICreated documents get created_at applied on load.
        var createdMember = mapping.Metadata.CreatedAt.Member
                            ?? (typeof(Polecat.Metadata.ICreated).IsAssignableFrom(typeof(TDoc))
                                ? typeof(TDoc).GetProperty(nameof(Polecat.Metadata.ICreated.CreatedAt))
                                : null);
        if (createdMember is not null)
        {
            readBinders.Add(new DocumentCreatedAtBinder<TDoc>(
                mapping.Metadata.CreatedAt.Name, createdMember, ServerTimestamp));
        }

        // Opt-in session-sourced metadata (#241): write slot when enabled, read slot only
        // with a mapped member.
        AddSessionMetadataBinder(writeBinders, readBinders, mapping.Metadata.CorrelationId,
            (name, member) => new DocumentCorrelationIdBinder<TDoc>(name, dialect, member));
        AddSessionMetadataBinder(writeBinders, readBinders, mapping.Metadata.CausationId,
            (name, member) => new DocumentCausationIdBinder<TDoc>(name, dialect, member));
        AddSessionMetadataBinder(writeBinders, readBinders, mapping.Metadata.LastModifiedBy,
            (name, member) => new DocumentLastModifiedByBinder<TDoc>(name, dialect, member));

        if (mapping.Metadata.Headers.Enabled)
        {
            // Headers: write-only — the member value comes from the JSON data column.
            writeBinders.Add(new DocumentHeadersBinder<TDoc>(
                mapping.Metadata.Headers.Name, dialect, mapping.Metadata.Headers.Member));
        }

        // Soft delete: every save writes the defaults (0, NULL) so re-saving a soft-deleted
        // document undeletes it — Marten's closed-shape semantics; the actual soft-delete
        // UPDATE stays with the delete operations.
        if (mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            var isDeleted = new DocumentSoftDeletedBinder<TDoc>(
                mapping.Metadata.IsSoftDeleted.Name, dialect, mapping.Metadata.IsSoftDeleted.Member);
            writeBinders.Add(isDeleted);
            if (mapping.Metadata.IsSoftDeleted.Member is not null)
            {
                readBinders.Add(isDeleted);
            }

            // Polecat has no member-mapping config for deleted_at (only is_deleted);
            // write-only with the column's fixed name.
            writeBinders.Add(new DocumentSoftDeletedAtBinder<TDoc>("deleted_at", dialect, member: null));
        }

        // Range-partitioned documents (#211): the duplicated partition column is written on
        // every save and participates in the MERGE ON clause so updates target the correct
        // partition row (Marten #4223 analog).
        PolecatPartitionColumnBinder<TDoc>? partitionBinder = null;
        if (mapping.HasPartitionColumn)
        {
            partitionBinder = new PolecatPartitionColumnBinder<TDoc>(mapping.Partitioning!);
            writeBinders.Add(partitionBinder);
        }

        var writeArray = writeBinders.ToArray();
        var readArray = readBinders.ToArray();
        // QueryOnly's SELECT omits the version/revision binder only when it has no mapped
        // member — mirrors Marten's #4602 rule.
        var versionHasMember = (versionBinder is not null && typeof(IVersioned).IsAssignableFrom(typeof(TDoc)))
                               || revisionBinder is not null;
        var queryOnlyReadArray = versionReadIndex >= 0 && !versionHasMember
            ? readArray.Where((_, i) => i != versionReadIndex).ToArray()
            : readArray;
        var clientSide = writeArray.Where(b => !b.IsServerSide).ToArray();

        var concurrencyMode = mapping.UseOptimisticConcurrency
            ? ConcurrencyMode.Optimistic
            : mapping.UseNumericRevisions
                ? ConcurrencyMode.Numeric
                : ConcurrencyMode.Off;

        var partitionColumn = partitionBinder?.ColumnName;
        var upsertSql = BuildMergeSql(mapping, writeArray, revisionBinder, partitionColumn, isConjoined, concurrencyMode, guarded: concurrencyMode != ConcurrencyMode.Off, insertOnly: false);
        var insertSql = BuildMergeSql(mapping, writeArray, revisionBinder, partitionColumn, isConjoined, concurrencyMode, guarded: false, insertOnly: true);
        var updateSql = BuildUpdateSql(mapping, writeArray, revisionBinder, partitionColumn, isConjoined, concurrencyMode);
        var overwriteSql = BuildMergeSql(mapping, writeArray, revisionBinder, partitionColumn, isConjoined, concurrencyMode, guarded: false, insertOnly: false);

        return new DocumentStorageDescriptor<TDoc, TId>(
            identification,
            serializer: Serialization.StorageSerializerAdapter.For(options.Serializer),
            dialect: SqlServerStorageDialect<TId>.Instance,
            clientSideWriteBinders: clientSide,
            writeBinders: writeArray,
            readBinders: readArray,
            queryOnlyReadBinders: queryOnlyReadArray,
            upsertSql: upsertSql,
            insertSql: insertSql,
            updateSql: updateSql,
            overwriteSql: overwriteSql,
            isConjoined: isConjoined,
            concurrencyMode: concurrencyMode,
            versionBinder: versionBinder,
            revisionBinder: revisionBinder,
            versionReadIndex: versionReadIndex,
            resolveDocumentType: resolveDocumentType,
            docTypeReadIndex: docTypeReadIndex,
            tableName: mapping.QualifiedTableName,
            partitionPkBinders: partitionBinder is null
                ? null
                : new IDocumentMetadataBinder<TDoc>[] { partitionBinder });
    }

    private static void AddSessionMetadataBinder<TDoc>(
        List<IDocumentMetadataBinder<TDoc>> writeBinders,
        List<IDocumentMetadataBinder<TDoc>> readBinders,
        Metadata.MetadataColumn column,
        Func<string, System.Reflection.MemberInfo?, IDocumentMetadataBinder<TDoc>> create)
        where TDoc : notnull
    {
        if (!column.Enabled)
        {
            return;
        }

        var binder = create(column.Name, column.Member);
        writeBinders.Add(binder);
        if (column.Member is not null)
        {
            readBinders.Add(binder);
        }
    }

    /// <summary>
    ///     The MERGE statement serving upsert (guarded or not) and insert-only. Client-side
    ///     binder values travel in the USING row so both branches reference <c>s.{col}</c>;
    ///     server-side values are baked as literals per branch. Off/Optimistic modes maintain
    ///     the always-present numeric <c>version</c> column as literal SQL (1 on insert,
    ///     <c>t.version + 1</c> on update); Numeric mode instead routes the revision binder's
    ///     TWO parameter slots through source columns <c>rev0</c>/<c>rev1</c> (the shared
    ///     operations bind the raw revision to both) and adds the four trailing slots the
    ///     numeric upsert binds (guard pair + SET-CASE pair — all four get the same value, so
    ///     MERGE's textual reordering versus Postgres is safe). Zero rows from OUTPUT signals
    ///     a concurrency violation (guarded upsert) or an id collision (insert-only).
    /// </summary>
    private static string BuildMergeSql<TDoc>(
        DocumentMapping mapping,
        IReadOnlyList<IDocumentMetadataBinder<TDoc>> binders,
        IDocumentMetadataBinder<TDoc>? revisionBinder,
        string? partitionColumn,
        bool isConjoined,
        ConcurrencyMode mode,
        bool guarded,
        bool insertOnly)
        where TDoc : notnull
    {
        var table = mapping.QualifiedTableName;
        var numeric = mode == ConcurrencyMode.Numeric;

        // USING row: [tenant] id, data, then client-side binder columns in binder-array order —
        // this IS the parameter slot order. The numeric revision binder occupies two slots.
        var usingColumns = new List<string>();
        if (isConjoined)
        {
            usingColumns.Add("tenant_id");
        }

        usingColumns.Add("id");
        usingColumns.Add("data");
        foreach (var binder in binders.Where(b => !b.IsServerSide))
        {
            if (numeric && ReferenceEquals(binder, revisionBinder))
            {
                usingColumns.Add("rev0");
                usingColumns.Add("rev1");
            }
            else
            {
                usingColumns.Add(binder.ColumnName);
            }
        }

        var usingValues = string.Join(", ", usingColumns.Select(_ => "?"));
        var sourceColumns = string.Join(", ", usingColumns);

        var onClause = isConjoined
            ? "t.id = s.id AND t.tenant_id = s.tenant_id"
            : "t.id = s.id";
        if (partitionColumn is not null)
        {
            // Partition column is in the PK; the predicate keeps MERGE on the right partition row.
            onClause += $" AND t.{partitionColumn} = s.{partitionColumn}";
        }

        // INSERT branch. Off/Optimistic: version literal 1 + created_at. Numeric: the version
        // value is the revision CASE over the source columns (auto -> initial revision 1).
        var insertColumns = new List<string>();
        var insertValues = new List<string>();
        foreach (var column in usingColumns)
        {
            if (column == "rev0")
            {
                // Bespoke parity: INSERT always starts at revision 1 regardless of the
                // doc-carried value (the expectation only applies to existing rows).
                insertColumns.Add("version");
                insertValues.Add("1");
            }
            else if (column != "rev1")
            {
                insertColumns.Add(column);
                insertValues.Add($"s.{column}");
            }
        }

        if (!numeric)
        {
            insertColumns.Add("version");
            insertValues.Add("1");
        }

        insertColumns.Add("created_at");
        insertValues.Add(ServerTimestamp);
        foreach (var binder in binders.Where(b => b.IsServerSide))
        {
            insertColumns.Add(binder.ColumnName);
            insertValues.Add(binder.ValueSql);
        }

        var insertClause =
            $"WHEN NOT MATCHED THEN INSERT ({string.Join(", ", insertColumns)}) VALUES ({string.Join(", ", insertValues)})";

        if (insertOnly)
        {
            return $"MERGE {table} WITH (HOLDLOCK) AS t " +
                   $"USING (VALUES ({usingValues})) AS s ({sourceColumns}) ON {onClause} " +
                   $"{insertClause} " +
                   $"OUTPUT {OutputColumn(mode)};";
        }

        // UPDATE branch. Numeric: version SET-CASE + guard use the four trailing ? slots the
        // shared numeric upsert binds after the client loop (all four carry the raw revision).
        var updateAssignments = new List<string> { "data = s.data" };
        if (numeric)
        {
            // POLECAT numeric semantics (deliberate divergence from Marten's explicit-greater
            // rule): the doc-carried revision is an EQUALITY expectation and version always
            // auto-increments. Explicit path sets ? + 1 to match the bespoke pipeline.
            updateAssignments.Add("version = CASE WHEN ? = 0 THEN t.version + 1 ELSE ? + 1 END");
        }
        else
        {
            updateAssignments.Add("version = t.version + 1");
        }

        updateAssignments.AddRange(binders
            .Where(b => !b.IsServerSide && b.ColumnName != "tenant_id" && !ReferenceEquals(b, revisionBinder))
            .Select(b => $"{b.ColumnName} = s.{b.ColumnName}"));
        updateAssignments.AddRange(binders.Where(b => b.IsServerSide)
            .Select(b => $"{b.ColumnName} = {b.ValueSql}"));

        var guard = string.Empty;
        if (guarded && mode == ConcurrencyMode.Optimistic)
        {
            // Trailing expected-version slot; strict equality — updating an existing row
            // without having loaded it is a concurrency violation by design.
            guard = " AND t.guid_version = ?";
        }
        else if (numeric)
        {
            // Equality expectation (Polecat parity): auto (? = 0) always wins; an explicit
            // revision must MATCH the current version. The shared numeric ops always bind the
            // four trailing slots, so the shape is constant whether guarded or not.
            guard = " AND (? = 0 OR t.version = ?)";
        }

        return $"MERGE {table} WITH (HOLDLOCK) AS t " +
               $"USING (VALUES ({usingValues})) AS s ({sourceColumns}) ON {onClause} " +
               $"WHEN MATCHED{guard} THEN UPDATE SET {string.Join(", ", updateAssignments)} " +
               $"{insertClause} " +
               $"OUTPUT {OutputColumn(mode)};";
    }

    /// <summary>
    ///     Update-only SQL. The shared update operations bind a DIFFERENT slot order than the
    ///     upserts (<c>ClosedShapeUpdateOperation.BindPreConcurrencyParameters</c>): data →
    ///     client-side binders → id → tenant (when conjoined) → partition PK binders →
    ///     trailing concurrency guard. The MERGE USING tuple is laid out in exactly that
    ///     order; the partition PK travels as a second source column (<c>{col}_pk</c>) since
    ///     the ordinary binder slot already carries it for the SET list. Zero rows = missing
    ///     document or concurrency violation.
    /// </summary>
    private static string BuildUpdateSql<TDoc>(
        DocumentMapping mapping,
        IReadOnlyList<IDocumentMetadataBinder<TDoc>> binders,
        IDocumentMetadataBinder<TDoc>? revisionBinder,
        string? partitionColumn,
        bool isConjoined,
        ConcurrencyMode mode)
        where TDoc : notnull
    {
        var table = mapping.QualifiedTableName;
        var numeric = mode == ConcurrencyMode.Numeric;

        // USING tuple in the update ops' binding order: data, client binders, id, [tenant],
        // [partition pk].
        var usingColumns = new List<string> { "data" };
        foreach (var binder in binders.Where(b => !b.IsServerSide))
        {
            if (numeric && ReferenceEquals(binder, revisionBinder))
            {
                usingColumns.Add("rev0");
                usingColumns.Add("rev1");
            }
            else
            {
                usingColumns.Add(binder.ColumnName);
            }
        }

        usingColumns.Add("id");
        if (isConjoined)
        {
            usingColumns.Add("tenant_id_pk");
        }

        if (partitionColumn is not null)
        {
            usingColumns.Add($"{partitionColumn}_pk");
        }

        var usingValues = string.Join(", ", usingColumns.Select(_ => "?"));
        var sourceColumns = string.Join(", ", usingColumns);

        var onClause = "t.id = s.id";
        if (isConjoined)
        {
            onClause += " AND t.tenant_id = s.tenant_id_pk";
        }

        if (partitionColumn is not null)
        {
            onClause += $" AND t.{partitionColumn} = s.{partitionColumn}_pk";
        }

        var updateAssignments = new List<string> { "data = s.data" };
        updateAssignments.Add(numeric
            ? "version = CASE WHEN s.rev0 = 0 THEN t.version + 1 ELSE s.rev1 + 1 END"
            : "version = t.version + 1");
        updateAssignments.AddRange(binders
            .Where(b => !b.IsServerSide && b.ColumnName != "tenant_id" && !ReferenceEquals(b, revisionBinder))
            .Select(b => $"{b.ColumnName} = s.{b.ColumnName}"));
        updateAssignments.AddRange(binders.Where(b => b.IsServerSide)
            .Select(b => $"{b.ColumnName} = {b.ValueSql}"));

        var guard = mode switch
        {
            ConcurrencyMode.Optimistic => " AND t.guid_version = ?",
            ConcurrencyMode.Numeric => " AND (? = 0 OR t.version = ?)",
            _ => string.Empty
        };

        return $"MERGE {table} WITH (HOLDLOCK) AS t " +
               $"USING (VALUES ({usingValues})) AS s ({sourceColumns}) ON {onClause} " +
               $"WHEN MATCHED{guard} THEN UPDATE SET {string.Join(", ", updateAssignments)} " +
               $"OUTPUT {OutputColumn(mode)};";
    }

    private static string OutputColumn(ConcurrencyMode mode)
        => mode switch
        {
            ConcurrencyMode.Optimistic => "inserted.guid_version",
            ConcurrencyMode.Numeric => "inserted.version",
            _ => "inserted.id"
        };
}
