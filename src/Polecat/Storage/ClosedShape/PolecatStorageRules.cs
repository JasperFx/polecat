using JasperFx;
using JasperFx.Core.Reflection;
using JasperFx.MultiTenancy;
using Weasel.Core;
using Weasel.Core.SqlGeneration;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     #959: everything about a Polecat document's storage that is Polecat's rather than Weasel's — the SQL
///     Server read and delete SQL, the tenancy and soft-delete filters, identity conversion, the mapped
///     version member (#720), and which descriptor a write runs over (#854, #916) — as the
///     <see cref="IClosedShapeStorageRules{TDoc,TId}" /> seam Weasel's public closed-shape bases take
///     (weasel#811). One instance is shared by a document type's QueryOnly, Lightweight and IdentityMap
///     storages, so the lazily built Snapshot descriptor is built once.
/// </summary>
/// <remarks>
///     The Polecat-only storage interfaces (<see cref="IPolecatObjectStorage{TDoc}" />,
///     <see cref="IPolecatBatchLoadStorage{TDoc}" />, <see cref="IPolecatBulkVersionCheckStorage{TDoc}" />,
///     <see cref="IPolecatDeletionStorage" />) have no counterpart in Weasel, so their SQL lives here too and
///     <see cref="IPolecatClosedShapeStorage{TDoc,TId}" /> forwards to it.
/// </remarks>
internal sealed class PolecatStorageRules<TDoc, TId> : IClosedShapeStorageRules<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    private readonly DocumentMapping _mapping;
    private readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    private readonly ClosedShapeReadSql _writeableReadSql;
    private readonly ClosedShapeReadSql _queryOnlyReadSql;
    private readonly IOperationFragment _deleteFragment;
    private readonly IOperationFragment _hardDeleteFragment;
    private readonly IOperationFragment _undeleteFragment;

    /// <summary>#720: readers for a <c>Metadata(m =&gt; m.Version.MapTo(...))</c> member; null when there is none.</summary>
    private readonly Func<TDoc, Guid>? _mappedVersion;

    private readonly Func<TDoc, long>? _mappedRevision;

    /// <summary>#720: the write-back half, for UpdateExpectedVersion / UpdateRevision.</summary>
    private readonly Action<TDoc, Guid>? _setMappedVersion;

    private readonly Action<TDoc, long>? _setMappedRevision;

    private DocumentStorageDescriptor<TDoc, TId>? _snapshotDescriptor;
    private string? _bulkVersionCheckedSql;

    public PolecatStorageRules(DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
    {
        _mapping = mapping;
        _descriptor = descriptor;

        TableName = DbObjectName.Parse(SqlServerProvider.Instance, mapping.QualifiedTableName);

        // #720: the mapped version member, read back off the document so the concurrency guard can be
        // seeded from the instance. Only one of the two can exist -- the member's type is what chose
        // the mode in DocumentMapping.ResolveMappedConcurrencyMode -- and a marker interface wins the
        // mode, so a mapped member on an IVersioned/IRevisioned type is still only a relocation of
        // where the value lives, which is what the session's interface-first ordering already says.
        var mappedVersionMember = mapping.Metadata.Version.Member;
        if (mappedVersionMember is not null)
        {
            var memberType = mappedVersionMember.GetRawMemberType();
            if (memberType == typeof(Guid) && mapping.UseOptimisticConcurrency)
            {
                _mappedVersion = LambdaBuilder.Getter<TDoc, Guid>(mappedVersionMember);
                _setMappedVersion = LambdaBuilder.Setter<TDoc, Guid>(mappedVersionMember);
            }
            else if (memberType == typeof(int) && mapping.UseNumericRevisions)
            {
                var intGetter = LambdaBuilder.Getter<TDoc, int>(mappedVersionMember);
                var intSetter = LambdaBuilder.Setter<TDoc, int>(mappedVersionMember);
                _mappedRevision = doc => intGetter!(doc);
                _setMappedRevision = (doc, revision) => intSetter!(doc, (int)revision);
            }
            else if (memberType == typeof(long) && mapping.UseNumericRevisions)
            {
                _mappedRevision = LambdaBuilder.Getter<TDoc, long>(mappedVersionMember);
                _setMappedRevision = LambdaBuilder.Setter<TDoc, long>(mappedVersionMember);
            }
        }

        // Read layout must match the shared selectors: writeable flavors read id=0, data=1,
        // metadata from 2; the QueryOnly selectors EXCLUDE id (data=0, metadata from 1) and
        // narrow the binder set via QueryOnlyReadBinders.
        _writeableReadSql = BuildReadSql(includeId: true, descriptor.ReadBinders);
        _queryOnlyReadSql = BuildReadSql(includeId: false, descriptor.QueryOnlyReadBinders);

        _deleteFragment = IsSoftDeleted
            ? new HardCodedOperationFragment(
                $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET()",
                OperationRole.Deletion)
            : new HardCodedOperationFragment(
                $"DELETE FROM {_mapping.QualifiedTableName}", OperationRole.Deletion);
        _hardDeleteFragment = new HardCodedOperationFragment(
            $"DELETE FROM {_mapping.QualifiedTableName}", OperationRole.Deletion);
        _undeleteFragment = new HardCodedOperationFragment(
            $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL", OperationRole.Update);
    }

    private ClosedShapeReadSql BuildReadSql(bool includeId, IDocumentMetadataBinder<TDoc>[] binders)
    {
        var readColumns = new List<string>();
        if (includeId)
        {
            readColumns.Add("id");
        }

        readColumns.Add("data");
        readColumns.AddRange(binders.Select(b => b.ColumnName));
        var fields = readColumns.ToArray();

        var select = $"SELECT {string.Join(", ", fields)} FROM {_mapping.QualifiedTableName}";

        // #234: only conjoined tables carry a tenant_id column, so single-tenant loads omit the
        // tenant predicate entirely (the harmless @tenant_id parameter the dialect still binds is
        // simply unreferenced by the SQL).
        var tenantFilter = IsConjoined ? " AND tenant_id = @tenant_id" : string.Empty;
        var loader = $"{select} WHERE id = @id{tenantFilter}{SoftDeleteFilterSql}";

        // #363: OPENJSON's value column is nvarchar(4000); against a varchar(250) id column that
        // forces CONVERT_IMPLICIT on the column side and a full scan, so string ids cast the
        // extracted values back to varchar before the IN comparison.
        var idsSelect = _mapping.InnerIdType == typeof(string)
            ? $"SELECT CAST(value AS {_mapping.IdSqlType}) FROM OPENJSON(@ids)"
            : "SELECT value FROM OPENJSON(@ids)";
        var loadMany = $"{select} WHERE id IN ({idsSelect}){tenantFilter}{SoftDeleteFilterSql}";

        return new ClosedShapeReadSql(fields, select, loader, loadMany);
    }

    private string SoftDeleteFilterSql => IsSoftDeleted ? " AND is_deleted = 0" : string.Empty;

    /// <summary>#234: conjoined tables carry a <c>tenant_id</c> column to scope the WHERE by.</summary>
    public bool IsConjoined => _mapping.TenancyStyle == TenancyStyle.Conjoined;

    public bool IsSoftDeleted => _mapping.DeleteStyle == DeleteStyle.SoftDelete;

    // ---- IClosedShapeStorageRules ----

    public DbObjectName TableName { get; }
    public TenancyStyle TenancyStyle => _mapping.TenancyStyle;
    public IReadOnlyList<IDuplicatedField> DuplicatedFields => Array.Empty<IDuplicatedField>();

    public ClosedShapeReadSql ReadSqlFor(ClosedShapeTrackingMode mode)
        => mode == ClosedShapeTrackingMode.QueryOnly ? _queryOnlyReadSql : _writeableReadSql;

    public IOperationFragment BuildDeleteFragment(IDocumentStorage<TDoc, TId> storage) => _deleteFragment;
    public IOperationFragment BuildHardDeleteFragment(IDocumentStorage<TDoc, TId> storage) => _hardDeleteFragment;

    /// <summary>
    ///     Always a fragment, soft-deleted type or not, as before: callers decide by
    ///     <see cref="IsSoftDeleted" /> (gh-558), not by whether there is one.
    /// </summary>
    public IOperationFragment? BuildUndeleteFragment(IDocumentStorage<TDoc, TId> storage) => _undeleteFragment;

    public IDeletion BuildDeletion(IDocumentStorage<TDoc, TId> storage, IOperationFragment fragment, TId id,
        ISqlFragment byIdFilter, string tenantId)
    {
        var hard = ReferenceEquals(fragment, _hardDeleteFragment) || !IsSoftDeleted;

        // #234: single-tenant tables have no tenant_id column, so the tenant predicate (and its
        // bound parameter) are conjoined-only. #419: a soft delete skips rows already deleted, so
        // it does not re-stamp deleted_at.
        var tenantClause = IsConjoined ? " AND tenant_id = ?" : string.Empty;
        var sql = hard
            ? $"DELETE FROM {_mapping.QualifiedTableName} WHERE id = ?{tenantClause}"
            : $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET() WHERE id = ?{tenantClause} AND is_deleted = 0";

        return new ClosedShapeDeletion<TDoc, TId>(sql, default, id, _descriptor.Identification.ToRawSqlValue(id),
            tenantId, _descriptor, typeof(TDoc), bindTenant: IsConjoined);
    }

    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
    {
        var filters = new List<ISqlFragment> { query };

        // #234: single-tenant tables have no tenant_id column to filter on.
        if (IsConjoined)
        {
            filters.Add(new TenantFilter(session.TenantId, _mapping.UnicodeKeys));
        }

        if (IsSoftDeleted)
        {
            filters.Add(new HardCodedFilter("d.is_deleted = 0"));
        }

        return filters.Count == 1 ? query : new AllOfFilter(filters);
    }

    public ISqlFragment? DefaultWhereFragment()
        => IsSoftDeleted ? new HardCodedFilter("d.is_deleted = 0") : null;

    public string TruncateSql => $"DELETE FROM {_mapping.QualifiedTableName}";

    public void SetIdentity(TDoc document, TId identity) => _mapping.SetRawId(document, identity);

    public void SetIdentityFromString(TDoc document, string identityString)
        => SetIdentity(document, ConvertIdentity(identityString));

    public void SetIdentityFromGuid(TDoc document, Guid identityGuid)
        => SetIdentity(document, typeof(TId) == typeof(Guid)
            ? (TId)(object)identityGuid
            : ConvertIdentity(identityGuid.ToString()));

    private TId ConvertIdentity(string raw)
        => (TId)_mapping.WrapId(ConvertRawIdentity(raw, _mapping.InnerIdType));

    private static object ConvertRawIdentity(string raw, Type idType)
    {
        if (idType == typeof(string)) return raw;
        if (idType == typeof(Guid)) return Guid.Parse(raw);
        if (idType == typeof(int)) return int.Parse(raw);
        if (idType == typeof(long)) return long.Parse(raw);
        throw new NotSupportedException($"Cannot convert identity string to {idType.FullName}.");
    }

    public Guid? MappedVersionFor(TDoc document) => _mappedVersion?.Invoke(document);
    public long? MappedRevisionFor(TDoc document) => _mappedRevision?.Invoke(document);

    public bool TryApplyMappedVersion(TDoc document, Guid version)
    {
        if (_setMappedVersion is null) return false;
        _setMappedVersion(document, version);
        return true;
    }

    public bool TryApplyMappedRevision(TDoc document, long revision)
    {
        if (_setMappedRevision is null) return false;
        _setMappedRevision(document, revision);
        return true;
    }

    /// <summary>
    ///     Which descriptor a write runs over. The default is the storage's own; two writes differ.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         #854: an unguarded write — an unversioned Update or Upsert — in a <c>Snapshot</c> session gets
    ///         its own SQL, so a write conflict raises 3960 as every other write in that session does.
    ///     </para>
    ///     <para>
    ///         #916: a numeric Update or Upsert at revision 0 asks for the next revision, so its guard always
    ///         passes and nothing but the conflict can stop it writing over a concurrent commit, which is the
    ///         case the Snapshot descriptor is for. With a revision named, the guard is what answers, and it
    ///         has to see the current row to do that.
    ///     </para>
    ///     <para>
    ///         A write with no session — the <c>*Projected</c> family the daemon and bulk insert use — always
    ///         runs over the storage's own descriptor, as do Insert, Overwrite and every optimistic write.
    ///     </para>
    /// </remarks>
    public DocumentStorageDescriptor<TDoc, TId> WriteDescriptorFor(OperationRole role, TDoc document,
        IStorageSession? session, DocumentStorageDescriptor<TDoc, TId> descriptor)
    {
        if (session is null || role is not (OperationRole.Update or OperationRole.Upsert))
        {
            return descriptor;
        }

        return descriptor.ConcurrencyMode switch
        {
            ConcurrencyMode.Off => UnguardedWriteDescriptorFor(session, descriptor),
            ConcurrencyMode.Numeric =>
                Internal.DocumentSessionBase.ExpectedRevisionOf(document, MappedRevisionFor(document)) == 0
                    ? UnguardedWriteDescriptorFor(session, descriptor)
                    : descriptor,
            _ => descriptor
        };
    }

    private DocumentStorageDescriptor<TDoc, TId> UnguardedWriteDescriptorFor(IStorageSession session,
        DocumentStorageDescriptor<TDoc, TId> descriptor)
        => RunsAtSnapshot(session)
            ? _snapshotDescriptor ??= SqlServerDocumentStorageDescriptorBuilder.Build(
                _mapping, descriptor.Identification, _mapping.StoreOptions, forSnapshotSession: true)
            : descriptor;

    private static bool RunsAtSnapshot(IStorageSession session) => session switch
    {
        Internal.DocumentSessionBase documents => documents.RunsAtSnapshotIsolation,
        Internal.TenantScopedStorageSession scoped => RunsAtSnapshot(scoped.Inner),
        _ => false
    };

    // ---- Polecat-only seams, forwarded by IPolecatClosedShapeStorage ----

    public TId NormalizeId(object id) => id is TId typed ? typed : (TId)_mapping.WrapId(id);

    /// <summary>#273: the batched-query read seam — by id.</summary>
    public void WriteLoadByIdSql(Weasel.SqlServer.ICommandBuilder builder, string selectClause, object id,
        string tenantId)
    {
        builder.Append(selectClause);
        builder.Append(" WHERE id = ");
        builder.AppendParameter(id, Polecat.Internal.SqlParameterCollectionExtensions.BindingForKey(id, _mapping.UnicodeKeys));
        AppendBatchLoadFilters(builder, tenantId);
    }

    /// <summary>#273: the batched-query read seam — many ids.</summary>
    public void WriteLoadManySql(Weasel.SqlServer.ICommandBuilder builder, string selectClause,
        IReadOnlyList<object> ids, string tenantId)
    {
        builder.Append(selectClause);

        if (ids.Count == 0)
        {
            builder.Append(" WHERE 1 = 0");
            return;
        }

        builder.Append(" WHERE ");

        // #834: the batched flavour of a load that LoadManyAsync has bound as one JSON array since
        // #363. This copy bound one parameter per id, so it alone was refused past 2098 ids.
        if (Polecat.Linq.SqlGeneration.JsonValueList.ShouldBindAsJsonArray(builder, ids.Count))
        {
            // #919: typed as the column, the list would turn 東京 into ?? on the server. Left as what
            // OPENJSON gives it, the comparison is exact, for the price SqlParameterCollectionExtensions
            // .BindingForKey describes.
            // #935: an nvarchar column is exact either way.
            var exact = !_mapping.UnicodeKeys && ids.Any(x => x is string text && !System.Text.Ascii.IsValid(text));
            Polecat.Linq.SqlGeneration.JsonValueList.AppendInClause(builder, "id", ids.ToArray(),
                exact ? null : _mapping.IdSqlType, value => value);
        }
        else
        {
            builder.Append("id IN (");
            for (var i = 0; i < ids.Count; i++)
            {
                if (i > 0) builder.Append(", ");
                builder.AppendParameter(ids[i], Polecat.Internal.SqlParameterCollectionExtensions.BindingForKey(ids[i], _mapping.UnicodeKeys));
            }

            builder.Append(")");
        }

        AppendBatchLoadFilters(builder, tenantId);
    }

    private void AppendBatchLoadFilters(Weasel.SqlServer.ICommandBuilder builder, string tenantId)
    {
        if (IsConjoined)
        {
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(tenantId, _mapping.StoreOptions.KeyDbType);
        }

        builder.Append(SoftDeleteFilterSql);
    }

    // ---- bulk version-checked upsert seam (#273 doc-side convergence) ----

    /// <summary>
    ///     The table-variable column type for the bulk OUTPUT stream — the INNER type of a
    ///     strongly-typed id, matching the document table's own id column (#296/#302).
    /// </summary>
    private string BulkOutputIdType() => _mapping.IdSqlType;

    private IEnumerable<IDocumentMetadataBinder<TDoc>> BulkClientBinders
        => _descriptor.ClientSideWriteBinders.Where(b => b.ColumnName != "version");

    private string BulkVersionCheckedSql()
    {
        if (_bulkVersionCheckedSql is not null) return _bulkVersionCheckedSql;

        var table = _mapping.QualifiedTableName;
        var conjoined = _descriptor.IsConjoined;
        var clientCols = BulkClientBinders.Select(b => b.ColumnName).ToArray();
        var serverBinders = _descriptor.WriteBinders.Where(b => b.IsServerSide && b.ColumnName != "version").ToArray();

        // USING tuple = parameter order: [tenant_id,] id, data, expected_version, {client cols}.
        var usingCols = new List<string>();
        if (conjoined) usingCols.Add("tenant_id");
        usingCols.Add("id");
        usingCols.Add("data");
        usingCols.Add("expected_version");
        usingCols.AddRange(clientCols);
        var usingValues = string.Join(", ", usingCols.Select(_ => "?"));
        var sourceCols = string.Join(", ", usingCols);

        // Managed tenant partitioning (#335): resolve tenant_ordinal server-side by joining the
        // pc_tenant_partitions registry into the MERGE source — parameter slots unchanged.
        var usingClause = _mapping.TenantPartitioned
            ? $"(SELECT {string.Join(", ", usingCols.Select(c => $"v.{c}"))}, tp.ordinal AS tenant_ordinal " +
              $"FROM (VALUES ({usingValues})) AS v ({sourceCols}) " +
              $"LEFT JOIN {_mapping.StoreOptions.EventGraph.TenantPartitionsTableName} tp " +
              "ON tp.tenant_id = v.tenant_id) AS s"
            : $"(VALUES ({usingValues})) AS s ({sourceCols})";

        var on = conjoined ? "t.id = s.id AND t.tenant_id = s.tenant_id" : "t.id = s.id";
        if (_mapping.TenantPartitioned)
        {
            on += " AND t.tenant_ordinal = s.tenant_ordinal";
        }

        // UPDATE (matched AND version == expected): data, version+1, client cols, server literals.
        var setList = new List<string> { "data = s.data", "version = t.version + 1" };
        setList.AddRange(clientCols.Select(c => $"{c} = s.{c}"));
        setList.AddRange(serverBinders.Select(b => $"{b.ColumnName} = {b.ValueSql}"));

        // INSERT (not matched): id, data, version=1, created_at, {client cols}, {server literals}[, tenant].
        var insertCols = new List<string> { "id", "data", "version", "created_at" };
        var insertVals = new List<string> { "s.id", "s.data", "1", "SYSDATETIMEOFFSET()" };
        foreach (var c in clientCols)
        {
            insertCols.Add(c);
            insertVals.Add($"s.{c}");
        }

        foreach (var b in serverBinders)
        {
            insertCols.Add(b.ColumnName);
            insertVals.Add(b.ValueSql);
        }

        if (conjoined)
        {
            insertCols.Add("tenant_id");
            insertVals.Add("s.tenant_id");
        }

        if (_mapping.TenantPartitioned)
        {
            insertCols.Add("tenant_ordinal");
            insertVals.Add("s.tenant_ordinal");
        }

        _bulkVersionCheckedSql =
            $"MERGE {table} WITH (HOLDLOCK) AS t " +
            $"USING {usingClause} ON {on} " +
            $"WHEN MATCHED AND t.version = s.expected_version THEN UPDATE SET {string.Join(", ", setList)} " +
            $"WHEN NOT MATCHED THEN INSERT ({string.Join(", ", insertCols)}) VALUES ({string.Join(", ", insertVals)}) " +
            $"OUTPUT inserted.id{(_mapping.FullTextIndexes.Count == 0 ? ";" : " INTO @pc_output;")}";

        // gh-611: a trigger on the table forbids a bare OUTPUT, so a full-text-indexed type routes
        // the id stream through a table variable. Same result shape, so the caller's
        // absent-row-means-conflict reading is unchanged.
        if (_mapping.FullTextIndexes.Count > 0)
        {
            _bulkVersionCheckedSql =
                $"DECLARE @pc_output TABLE (value {BulkOutputIdType()}); " + _bulkVersionCheckedSql
                + " SELECT value FROM @pc_output;";
        }

        return _bulkVersionCheckedSql;
    }

    public void ConfigureVersionCheckedUpsert(Weasel.SqlServer.ICommandBuilder builder, TDoc document, TId id,
        long expectedVersion, string tenantId)
    {
        var parameters = builder.AppendWithParameters(BulkVersionCheckedSql(), '?');
        var slot = 0;

        if (_descriptor.IsConjoined)
        {
            parameters[slot].Value = tenantId;
            _descriptor.Dialect.SetParameterType(parameters[slot], StorageColumnType.String);
            slot++;
        }

        parameters[slot].Value = _descriptor.Identification.ToRawSqlValue(id);
        _descriptor.Dialect.SetIdParameterType(parameters[slot], _descriptor.Identification.RawSqlType);
        slot++;

        _descriptor.Serializer.WriteToParameter(parameters[slot], document);
        slot++;

        parameters[slot].Value = expectedVersion;
        _descriptor.Dialect.SetParameterType(parameters[slot], StorageColumnType.Long);
        slot++;

        foreach (var binder in BulkClientBinders)
        {
            var bulk = binder.GetBulkValue(document);
            if (bulk.Value is null)
            {
                parameters[slot].Value = DBNull.Value;
            }
            else
            {
                parameters[slot].Value = bulk.Value;
                _descriptor.Dialect.SetParameterType(parameters[slot], bulk.Type);
            }

            slot++;
        }
    }
}
