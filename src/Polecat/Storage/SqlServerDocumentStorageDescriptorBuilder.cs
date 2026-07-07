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
        if (mapping.IsHierarchy())
        {
            throw new NotSupportedException(
                "Closed-shape descriptors for hierarchical documents land in a later #273 increment.");
        }

        if (mapping.UseNumericRevisions || mapping.DeleteStyle == DeleteStyle.SoftDelete ||
            mapping.HasPartitionColumn)
        {
            throw new NotSupportedException(
                "Closed-shape descriptors for numeric revisions / soft deletes / partitioned documents land in a later #273 increment.");
        }

        var dialect = SqlServerStorageDialect<TId>.Instance;
        var writeBinders = new List<IDocumentMetadataBinder<TDoc>>();
        var readBinders = new List<IDocumentMetadataBinder<TDoc>>();

        DocumentVersionBinder<TDoc>? versionBinder = null;
        var versionReadIndex = -1;
        var isConjoined = mapping.TenancyStyle == TenancyStyle.Conjoined;

        // tenant_id: pc tables always carry it. Conjoined uses the shared operations'
        // leading tenant parameter slot (NOT a binder); single-tenant writes the session's
        // (default) tenant id through a regular binder slot.
        if (!isConjoined)
        {
            // Shared DocumentTenantIdBinder is read-only (Marten binds tenant inline);
            // Polecat's write-capable binder sources the session's tenant id.
            writeBinders.Add(new PolecatTenantIdBinder<TDoc>(
                mapping.Metadata.TenantId.Name, mapping.Metadata.TenantId.Member));
        }
        else if (mapping.Metadata.TenantId.Member is not null)
        {
            readBinders.Add(new DocumentTenantIdBinder<TDoc>(
                mapping.Metadata.TenantId.Name, mapping.Metadata.TenantId.Member));
        }

        // guid_version: the optimistic-concurrency vehicle (uniqueidentifier), separate from
        // the always-present numeric version column that MERGE maintains as literal SQL.
        if (mapping.UseOptimisticConcurrency)
        {
            versionBinder = new DocumentVersionBinder<TDoc>("guid_version", dialect, versionMember: null);
            writeBinders.Add(versionBinder);
            versionReadIndex = readBinders.Count;
            readBinders.Add(versionBinder);
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
        if (mapping.Metadata.CreatedAt.Member is not null)
        {
            readBinders.Add(new DocumentCreatedAtBinder<TDoc>(
                mapping.Metadata.CreatedAt.Name, mapping.Metadata.CreatedAt.Member, ServerTimestamp));
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

        var writeArray = writeBinders.ToArray();
        var readArray = readBinders.ToArray();
        // QueryOnly's SELECT omits guid_version when it has no mapped member (always the case
        // in slice 1) so its read set drops that binder — mirrors Marten's #4602 rule.
        var queryOnlyReadArray = versionReadIndex >= 0
            ? readArray.Where((_, i) => i != versionReadIndex).ToArray()
            : readArray;
        var clientSide = writeArray.Where(b => !b.IsServerSide).ToArray();

        var concurrencyMode = mapping.UseOptimisticConcurrency
            ? ConcurrencyMode.Optimistic
            : ConcurrencyMode.Off;

        var upsertSql = BuildMergeSql(mapping, writeArray, isConjoined, concurrencyMode, guarded: concurrencyMode != ConcurrencyMode.Off, insertOnly: false);
        var insertSql = BuildMergeSql(mapping, writeArray, isConjoined, concurrencyMode, guarded: false, insertOnly: true);
        var updateSql = BuildUpdateSql(mapping, writeArray, isConjoined, concurrencyMode);
        var overwriteSql = BuildMergeSql(mapping, writeArray, isConjoined, concurrencyMode, guarded: false, insertOnly: false);

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
            revisionBinder: null,
            versionReadIndex: versionReadIndex,
            resolveDocumentType: null,
            docTypeReadIndex: -1,
            tableName: mapping.QualifiedTableName);
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
    ///     server-side values are baked as literals per branch. The always-present numeric
    ///     <c>version</c> column is 1 on insert and <c>t.version + 1</c> on update. Zero rows
    ///     from OUTPUT signals a concurrency violation (guarded upsert) or an id collision
    ///     (insert-only) to the shared operations.
    /// </summary>
    private static string BuildMergeSql<TDoc>(
        DocumentMapping mapping,
        IReadOnlyList<IDocumentMetadataBinder<TDoc>> binders,
        bool isConjoined,
        ConcurrencyMode mode,
        bool guarded,
        bool insertOnly)
        where TDoc : notnull
    {
        var table = mapping.QualifiedTableName;

        // USING row: [tenant] id, data, then client-side binder columns — parameter slot order.
        var usingColumns = new List<string>();
        if (isConjoined)
        {
            usingColumns.Add("tenant_id");
        }

        usingColumns.Add("id");
        usingColumns.Add("data");
        usingColumns.AddRange(binders.Where(b => !b.IsServerSide).Select(b => b.ColumnName));

        var usingValues = string.Join(", ", usingColumns.Select(_ => "?"));
        var sourceColumns = string.Join(", ", usingColumns);

        var onClause = isConjoined
            ? "t.id = s.id AND t.tenant_id = s.tenant_id"
            : "t.id = s.id";

        // INSERT branch: USING columns + version/created_at/last modified server-side extras.
        var insertColumns = new List<string>(usingColumns) { "version", "created_at" };
        var insertValues = new List<string>(usingColumns.Select(c => $"s.{c}")) { "1", ServerTimestamp };
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

        // UPDATE branch: data + client-side binder columns from s, server-side as literals,
        // numeric version incremented. created_at is never touched on update.
        var updateAssignments = new List<string> { "data = s.data", "version = t.version + 1" };
        updateAssignments.AddRange(binders.Where(b => !b.IsServerSide && b.ColumnName != "tenant_id")
            .Select(b => $"{b.ColumnName} = s.{b.ColumnName}"));
        updateAssignments.AddRange(binders.Where(b => b.IsServerSide)
            .Select(b => $"{b.ColumnName} = {b.ValueSql}"));

        // Optimistic guard: expected guid_version is the trailing parameter slot; a NULL
        // expectation (document never loaded in this session) skips the check, matching the
        // shared operation's DBNull binding.
        var guard = guarded && mode == ConcurrencyMode.Optimistic
            ? " AND t.guid_version = ?"
            : string.Empty;

        return $"MERGE {table} WITH (HOLDLOCK) AS t " +
               $"USING (VALUES ({usingValues})) AS s ({sourceColumns}) ON {onClause} " +
               $"WHEN MATCHED{guard} THEN UPDATE SET {string.Join(", ", updateAssignments)} " +
               $"{insertClause} " +
               $"OUTPUT {OutputColumn(mode)};";
    }

    /// <summary>
    ///     Update-only SQL. Parameter order per the shared update operations: [tenant] id,
    ///     data, client binders, then the optimistic guard pair. Zero rows = missing document
    ///     or concurrency violation.
    /// </summary>
    private static string BuildUpdateSql<TDoc>(
        DocumentMapping mapping,
        IReadOnlyList<IDocumentMetadataBinder<TDoc>> binders,
        bool isConjoined,
        ConcurrencyMode mode)
        where TDoc : notnull
    {
        var table = mapping.QualifiedTableName;

        // Reuse the MERGE shape without the NOT MATCHED branch so the slot order stays
        // identical to upsert — the shared ClosedShapeUpdateOperations bind exactly like
        // the upserts.
        var usingColumns = new List<string>();
        if (isConjoined)
        {
            usingColumns.Add("tenant_id");
        }

        usingColumns.Add("id");
        usingColumns.Add("data");
        usingColumns.AddRange(binders.Where(b => !b.IsServerSide).Select(b => b.ColumnName));

        var usingValues = string.Join(", ", usingColumns.Select(_ => "?"));
        var sourceColumns = string.Join(", ", usingColumns);
        var onClause = isConjoined
            ? "t.id = s.id AND t.tenant_id = s.tenant_id"
            : "t.id = s.id";

        var updateAssignments = new List<string> { "data = s.data", "version = t.version + 1" };
        updateAssignments.AddRange(binders.Where(b => !b.IsServerSide && b.ColumnName != "tenant_id")
            .Select(b => $"{b.ColumnName} = s.{b.ColumnName}"));
        updateAssignments.AddRange(binders.Where(b => b.IsServerSide)
            .Select(b => $"{b.ColumnName} = {b.ValueSql}"));

        var guard = mode == ConcurrencyMode.Optimistic
            ? " AND t.guid_version = ?"
            : string.Empty;

        return $"MERGE {table} WITH (HOLDLOCK) AS t " +
               $"USING (VALUES ({usingValues})) AS s ({sourceColumns}) ON {onClause} " +
               $"WHEN MATCHED{guard} THEN UPDATE SET {string.Join(", ", updateAssignments)} " +
               $"OUTPUT {OutputColumn(mode)};";
    }

    private static string OutputColumn(ConcurrencyMode mode)
        => mode == ConcurrencyMode.Optimistic ? "inserted.guid_version" : "inserted.id";
}
