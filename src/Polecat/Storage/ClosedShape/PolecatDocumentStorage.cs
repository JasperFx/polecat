using System.Data.Common;
using JasperFx;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.Core.SqlGeneration;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Storage.ClosedShape;

/// <summary>
///     Polecat's implementation of the shared closed-shape <see cref="IDocumentStorage{T,TId}" />
///     contract (#273 phase E1), composed from the released Weasel.Storage pieces: the SQL Server
///     descriptor (#286–#288), <see cref="SqlServerStorageDialect{TId}" /> (#283), and the shared
///     selectors/write operations (Weasel.Storage 9.13.0). The per-store layer mirrors Marten's:
///     thin flavor/mode compositions over a common base — Marten's 559-line
///     <c>DocumentStorage</c> stays Marten-local because this layer is deliberately per-store.
/// </summary>
/// <remarks>
///     E1 scope: the storage layer is reachable through the shared seams
///     (<c>IStorageSession.StorageFor</c> / <c>IStorageDatabase.Providers</c>) while Polecat's
///     bespoke pipeline still drives sessions. LINQ-facing members (<c>FilterDocuments</c>,
///     <c>SelectFields</c>, fragments) are minimal-but-correct; the full LINQ retarget is E2+.
///     Subclass (hierarchy child) storage is also E2.
/// </remarks>
/// <summary>
///     Non-generic-id load bridge for the session pipeline (#273 E2a): QuerySession's load
///     internals carry ids as <c>object</c>, so this closes the gap without the session
///     needing the TId generic argument.
/// </summary>
internal interface IPolecatObjectStorage<TDoc> where TDoc : notnull
{
    Task<TDoc?> LoadByObjectIdAsync(object id, IStorageSession session, CancellationToken token);
    Task<IReadOnlyList<TDoc>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session, CancellationToken token);
    IDeletion DeletionForObjectId(object id, string tenantId);
}

internal abstract class PolecatDocumentStorage<TDoc, TId> : IDocumentStorage<TDoc, TId>, IPolecatObjectStorage<TDoc>
    where TDoc : notnull
    where TId : notnull
{
    protected readonly DocumentMapping _mapping;
    protected readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    private readonly string _loaderSql;
    private readonly string _loadManySql;
    private readonly string[] _selectFields;
    private readonly IOperationFragment _deleteFragment;
    private readonly IOperationFragment _hardDeleteFragment;

    protected PolecatDocumentStorage(DocumentMapping mapping, DocumentStorageDescriptor<TDoc, TId> descriptor)
    {
        _mapping = mapping;
        _descriptor = descriptor;

        // Read layout must match the shared selectors: writeable flavors read id=0, data=1,
        // metadata from 2; the QueryOnly selectors EXCLUDE id (data=0, metadata from 1) and
        // narrow the binder set via QueryOnlyReadBinders.
        var readColumns = new List<string>();
        if (IncludeIdInSelect)
        {
            readColumns.Add("id");
        }

        readColumns.Add("data");
        readColumns.AddRange(ReadBinders().Select(b => b.ColumnName));
        _selectFields = readColumns.ToArray();

        var select = $"SELECT {string.Join(", ", _selectFields)} FROM {_mapping.QualifiedTableName}";
        // Polecat filters tenant_id on every load (the column is always present; default
        // tenant id for single-tenant stores), matching the bespoke pipeline.
        _loaderSql = $"{select} WHERE id = @id AND tenant_id = @tenant_id{SoftDeleteFilterSql()}";
        _loadManySql =
            $"{select} WHERE id IN (SELECT value FROM OPENJSON(@ids)) AND tenant_id = @tenant_id{SoftDeleteFilterSql()}";

        _deleteFragment = _mapping.DeleteStyle == DeleteStyle.SoftDelete
            ? new HardCodedOperationFragment(
                $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET()",
                OperationRole.Deletion)
            : new HardCodedOperationFragment(
                $"DELETE FROM {_mapping.QualifiedTableName}", OperationRole.Deletion);
        _hardDeleteFragment = new HardCodedOperationFragment(
            $"DELETE FROM {_mapping.QualifiedTableName}", OperationRole.Deletion);
    }

    /// <summary>Read binders backing this flavor's SELECT (QueryOnly overrides).</summary>
    protected virtual IDocumentMetadataBinder<TDoc>[] ReadBinders() => _descriptor.ReadBinders;

    /// <summary>The QueryOnly selectors read data at column 0 with no id column.</summary>
    protected virtual bool IncludeIdInSelect => true;

    private string SoftDeleteFilterSql()
        => _mapping.DeleteStyle == DeleteStyle.SoftDelete ? " AND is_deleted = 0" : string.Empty;

    // ---- identity ----

    public Type SourceType => typeof(TDoc);
    public Type IdType => typeof(TId);
    public Type DocumentType => typeof(TDoc);
    public Type SelectedType => typeof(TDoc);

    public TId Identity(TDoc document) => _descriptor.Identification.Identity(document);

    public object IdentityFor(TDoc document) => Identity(document);

    public TId AssignIdentity(TDoc document, string tenantId, IStorageDatabase database)
        => _descriptor.Identification.AssignIfMissing(document, database);

    public void SetIdentity(TDoc document, TId identity) => _mapping.SetRawId(document, identity);

    public object RawIdentityValue(object id) => _descriptor.Identification.ToRawSqlValue((TId)id);

    public object RawIdentityValue(TId id) => _descriptor.Identification.ToRawSqlValue(id);

    public void SetIdentityFromString(TDoc document, string identityString)
        => SetIdentity(document, ConvertIdentity(identityString));

    public void SetIdentityFromGuid(TDoc document, Guid identityGuid)
        => SetIdentity(document, typeof(TId) == typeof(Guid)
            ? (TId)(object)identityGuid
            : ConvertIdentity(identityGuid.ToString()));

    /// <summary>
    ///     Converts an identity string to TId. Strongly-typed wrapper ids (#273 E2e) convert
    ///     the string to the INNER value shape first, then wrap via the mapping's compiled
    ///     wrapper (WrapId is a pass-through for plain ids, where InnerIdType == TId).
    /// </summary>
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

    // ---- storage facts ----

    public bool UseOptimisticConcurrency => _descriptor.ConcurrencyMode == ConcurrencyMode.Optimistic;
    public bool UseNumericRevisions => _descriptor.ConcurrencyMode == ConcurrencyMode.Numeric;
    public TenancyStyle TenancyStyle => _mapping.TenancyStyle;
    public DbObjectName TableName => DbObjectName.Parse(SqlServerProvider.Instance, _mapping.QualifiedTableName);
    public IOperationFragment DeleteFragment => _deleteFragment;
    public IOperationFragment HardDeleteFragment => _hardDeleteFragment;
    public IReadOnlyList<IDuplicatedField> DuplicatedFields => Array.Empty<IDuplicatedField>();

    // ---- select clause (E1-minimal; full LINQ retarget is E2) ----

    public string FromObject => _mapping.QualifiedTableName;
    public string[] SelectFields() => _selectFields;

    public void Apply(Weasel.Core.ICommandBuilder builder)
    {
        builder.Append("SELECT ");
        builder.Append(string.Join(", ", _selectFields));
        builder.Append(" FROM ");
        builder.Append(_mapping.QualifiedTableName);
    }

    public abstract ISelector BuildSelector(IStorageSession session);

    public ISqlFragment FilterDocuments(ISqlFragment query, IStorageSession session)
    {
        var filters = new List<ISqlFragment> { query };
        filters.Add(new HardCodedFilter($"d.tenant_id = '{session.TenantId.Replace("'", "''")}'"));
        if (_mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            filters.Add(new HardCodedFilter("d.is_deleted = 0"));
        }

        return filters.Count == 1 ? query : new AllOfFilter(filters);
    }

    public ISqlFragment? DefaultWhereFragment()
        => _mapping.DeleteStyle == DeleteStyle.SoftDelete ? new HardCodedFilter("d.is_deleted = 0") : null;

    public ISqlFragment ByIdFilter(TId id) => _descriptor.Dialect.ByIdFilter(RawIdentityValue(id));

    // ---- load paths ----

    public DbCommand BuildLoadCommand(TId id, string tenantId)
        => _descriptor.Dialect.BuildLoadCommand(_loaderSql, RawIdentityValue(id), tenantId);

    public DbCommand BuildLoadManyCommand(TId[] ids, string tenantId)
        => _descriptor.Dialect.BuildLoadManyCommand(_loadManySql, BuildManyIdParameter(ids), tenantId);

    protected DbParameter BuildManyIdParameter(TId[] ids)
        => _descriptor.Dialect.CreateIdArrayParameter(
            Array.ConvertAll(ids, id => RawIdentityValue(id)),
            _descriptor.Identification.RawSqlType);

    public abstract Task<TDoc?> LoadAsync(TId id, IStorageSession session, CancellationToken token);

    public abstract Task<IReadOnlyList<TDoc>> LoadManyAsync(TId[] ids, IStorageSession session, CancellationToken token);

    protected async Task<TDoc?> QueryOneAsync(TId id, IStorageSession session, CancellationToken token)
    {
        var command = BuildLoadCommand(id, session.TenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);
        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return default;
        }

        return await selector.ResolveAsync(reader, token).ConfigureAwait(false);
    }

    protected async Task<IReadOnlyList<TDoc>> QueryManyAsync(TId[] ids, IStorageSession session,
        CancellationToken token)
    {
        var command = BuildLoadManyCommand(ids, session.TenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);
        var list = new List<TDoc>();
        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            list.Add(await selector.ResolveAsync(reader, token).ConfigureAwait(false));
        }

        return list;
    }

    public Task<TDoc?> LoadProjectedAsync(TId id, IStorageDatabase database, string tenantId, CancellationToken token)
        => ClosedShapeProjectionLoader<TDoc, TId>.LoadAsync(
            BuildLoadCommand(id, tenantId), _descriptor, _descriptor.Serializer, database, token);

    public Task<IReadOnlyList<TDoc>> LoadManyProjectedAsync(TId[] ids, IStorageDatabase database, string tenantId,
        CancellationToken token)
        => ClosedShapeProjectionLoader<TDoc, TId>.LoadManyAsync(
            BuildLoadManyCommand(ids, tenantId), _descriptor, _descriptor.Serializer, database, token);

    // ---- session bookkeeping ----

    public virtual void Store(IStorageSession session, TDoc document)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.MarkAsAddedForStorage(id, document);
    }

    public virtual void Store(IStorageSession session, TDoc document, Guid? version)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        if (version.HasValue)
        {
            session.Versions.StoreVersion<TDoc, TId>(id, version.Value);
        }
        else
        {
            session.Versions.ClearVersion<TDoc, TId>(id);
        }

        session.MarkAsAddedForStorage(id, document);
    }

    public virtual void Store(IStorageSession session, TDoc document, long revision)
    {
        var id = AssignIdentity(document, session.TenantId, session.Database);
        session.Versions.StoreRevision<TDoc, TId>(id, revision);
        session.MarkAsAddedForStorage(id, document);
    }

    public Guid? VersionFor(TDoc document, IStorageSession session)
        => session.Versions.VersionFor<TDoc, TId>(Identity(document));

    public virtual void Eject(IStorageSession session, TDoc document)
    {
        var id = Identity(document);
        EjectById(session, id);
    }

    public virtual void EjectById(IStorageSession session, object id)
    {
        if (session.ItemMap.TryGetValue(typeof(TDoc), out var raw) && raw is Dictionary<TId, TDoc> map)
        {
            map.Remove((TId)id);
        }

        session.Versions.ClearVersion<TDoc, TId>((TId)id);
        session.Versions.ClearRevision<TDoc, TId>((TId)id);
    }

    public void RemoveDirtyTracker(IStorageSession session, object id)
    {
        // Polecat has no dirty tracking by design — nothing registered, nothing to remove.
    }

    // ---- write operations (mode concretions supply these) ----

    public abstract Weasel.Storage.IStorageOperation Insert(TDoc document, IStorageSession session, string tenantId);
    public abstract Weasel.Storage.IStorageOperation Update(TDoc document, IStorageSession session, string tenantId);
    public abstract Weasel.Storage.IStorageOperation Upsert(TDoc document, IStorageSession session, string tenantId);
    public abstract Weasel.Storage.IStorageOperation Overwrite(TDoc document, IStorageSession session, string tenantId);
    public abstract Weasel.Storage.IStorageOperation OverwriteProjected(TDoc document, string tenantId);
    public abstract Weasel.Storage.IStorageOperation UpsertProjected(TDoc document, string tenantId);
    public abstract Weasel.Storage.IStorageOperation InsertProjected(TDoc document, string tenantId);
    public abstract Weasel.Storage.IStorageOperation UpdateProjected(TDoc document, string tenantId);

    // ---- deletions ----

    public IDeletion DeleteForDocument(TDoc document, string tenantId)
        => BuildDeletion(document, Identity(document), tenantId, hard: _mapping.DeleteStyle != DeleteStyle.SoftDelete);

    public IDeletion HardDeleteForDocument(TDoc document, string tenantId)
        => BuildDeletion(document, Identity(document), tenantId, hard: true);

    public IDeletion DeleteForId(TId id, string tenantId)
        => BuildDeletion(default, id, tenantId, hard: _mapping.DeleteStyle != DeleteStyle.SoftDelete);

    public IDeletion HardDeleteForId(TId id, string tenantId)
        => BuildDeletion(default, id, tenantId, hard: true);

    private IDeletion BuildDeletion(TDoc? document, TId id, string tenantId, bool hard)
    {
        var sql = hard
            ? $"DELETE FROM {_mapping.QualifiedTableName} WHERE id = ? AND tenant_id = ?"
            : $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET() WHERE id = ? AND tenant_id = ? AND is_deleted = 0";
        return new ClosedShapeDeletion<TDoc, TId>(sql, document, id, RawIdentityValue(id), tenantId,
            _descriptor, typeof(TDoc));
    }

    public async Task TruncateDocumentStorageAsync(IStorageDatabase database, CancellationToken ct = default)
    {
        await database.RunSqlAsync($"DELETE FROM {_mapping.QualifiedTableName}", ct).ConfigureAwait(false);
    }

    public bool Contains(string sqlText) => false;

    // ---- IPolecatObjectStorage bridge (#273 E2a) ----

    public Task<TDoc?> LoadByObjectIdAsync(object id, IStorageSession session, CancellationToken token)
        => LoadAsync(NormalizeId(id), session, token);

    public Task<IReadOnlyList<TDoc>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session,
        CancellationToken token)
    {
        var typed = new TId[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            typed[i] = NormalizeId(ids[i]);
        }

        return LoadManyAsync(typed, session, token);
    }

    /// <summary>
    ///     Session load internals carry raw inner values (Guid/string/int/long) even for
    ///     strongly-typed-id documents; wrap when TId is the wrapper type.
    /// </summary>
    private TId NormalizeId(object id) => id is TId typed ? typed : (TId)_mapping.WrapId(id);

    public IDeletion DeletionForObjectId(object id, string tenantId) => DeleteForId(NormalizeId(id), tenantId);
}

/// <summary>Fixed-SQL operation fragment (delete fragments on the shared contract).</summary>
internal sealed class HardCodedOperationFragment : IOperationFragment
{
    private readonly string _sql;
    private readonly OperationRole _role;

    public HardCodedOperationFragment(string sql, OperationRole role)
    {
        _sql = sql;
        _role = role;
    }

    public void Apply(Weasel.Core.ICommandBuilder builder) => builder.Append(_sql);

    public OperationRole Role() => _role;
}

/// <summary>Fixed-SQL WHERE filter over the shared SQL-generation contract.</summary>
internal sealed class HardCodedFilter : ISqlFragment
{
    private readonly string _sql;

    public HardCodedFilter(string sql) => _sql = sql;

    public void Apply(Weasel.Core.ICommandBuilder builder) => builder.Append(_sql);
}

/// <summary>AND-combination of filters.</summary>
internal sealed class AllOfFilter : ICompoundFragment
{
    private readonly IReadOnlyList<ISqlFragment> _filters;

    public AllOfFilter(IReadOnlyList<ISqlFragment> filters) => _filters = filters;

    public IEnumerable<ISqlFragment> Children => _filters;

    public void Apply(Weasel.Core.ICommandBuilder builder)
    {
        builder.Append("(");
        for (var i = 0; i < _filters.Count; i++)
        {
            if (i > 0) builder.Append(" AND ");
            _filters[i].Apply(builder);
        }

        builder.Append(")");
    }
}

/// <summary>
///     Closed-shape deletion (hard DELETE or soft UPDATE) over the shared operation contract.
///     Binds id then tenant via <c>?</c> slots; no result set (<see cref="NoDataReturnedCall" />).
/// </summary>
internal sealed class ClosedShapeDeletion<TDoc, TId> : IDeletion, NoDataReturnedCall
    where TDoc : notnull
    where TId : notnull
{
    private readonly string _sql;
    private readonly object _rawId;
    private readonly string _tenantId;
    private readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    private readonly Type _documentType;

    public ClosedShapeDeletion(string sql, TDoc? document, TId id, object rawId, string tenantId,
        DocumentStorageDescriptor<TDoc, TId> descriptor, Type documentType)
    {
        _sql = sql;
        _rawId = rawId;
        _tenantId = tenantId;
        _descriptor = descriptor;
        _documentType = documentType;
        Document = document!;
        Id = id;
    }

    public object Document { get; set; }
    public object Id { get; set; }
    public Type DocumentType => _documentType;

    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(Weasel.Core.ICommandBuilder builder, IStorageSession session)
    {
        var parameters = builder.AppendWithDbParameters(_sql, '?');
        parameters[0].Value = _rawId;
        _descriptor.Dialect.SetIdParameterType(parameters[0], _descriptor.Identification.RawSqlType);
        parameters[1].Value = _tenantId;
        _descriptor.Dialect.SetParameterType(parameters[1], StorageColumnType.String);
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
