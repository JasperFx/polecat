using System.Data;
using System.Data.Common;
using JasperFx;
using JasperFx.Core.Reflection;
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
///     selectors/write operations (Weasel.Storage 9.13.0). Since #959 the tracking-mode classes
///     themselves are Weasel's public closed-shape bases (weasel#811), with Polecat's SQL and policies
///     supplied through <see cref="PolecatStorageRules{TDoc,TId}" />.
/// </summary>
/// <remarks>
///     E1 scope: the storage layer is reachable through the shared seams
///     (<c>IStorageSession.StorageFor</c> / <c>IStorageDatabase.Providers</c>) while Polecat's
///     bespoke pipeline still drives sessions. LINQ-facing members (<c>FilterDocuments</c>,
///     <c>SelectFields</c>, fragments) are minimal-but-correct; the full LINQ retarget is E2+.
///     Subclass (hierarchy child) storage is also E2.
/// </remarks>
/// <summary>
///     Non-generic object bridge for callers that only have a runtime <see cref="Type" />
///     (#273 E2e): <c>StoreObjects</c> and the projection storage dispatch writes without a
///     TDoc/TId generic context.
/// </summary>
internal interface IPolecatObjectWriteStorage
{
    /// <summary>Id assignment + session bookkeeping (identity-map add for that flavor).</summary>
    void StoreObject(IStorageSession session, object document);

    /// <summary>
    ///     #592 — the same, seeding the Guid concurrency guard with the caller's expected version.
    ///     The object-typed twin of <c>IDocumentStorage&lt;T&gt;.Store(session, document, version)</c>,
    ///     so a versioned document written through <c>StoreObjects</c> is guarded the same way one
    ///     written through <c>Store&lt;T&gt;</c> is.
    /// </summary>
    void StoreObject(IStorageSession session, object document, Guid? version);

    /// <summary>
    ///     #720 — the object-typed twin of <c>IDocumentStorage&lt;T&gt;.MappedVersionFor</c> /
    ///     <c>MappedRevisionFor</c>, so <c>StoreObjects</c> seeds a mapped concurrency member the same
    ///     way <c>Store&lt;T&gt;</c> does. Null means "no mapped member of that kind".
    /// </summary>
    Guid? MappedVersionForObject(object document);

    long? MappedRevisionForObject(object document);

    Weasel.Storage.IStorageOperation UpsertObject(object document, IStorageSession session, string tenantId);

    /// <summary>Session-free upsert for the projection paths (no version-tracker reads).</summary>
    Weasel.Storage.IStorageOperation UpsertObjectProjected(object document, string tenantId);

    IDeletion HardDeletionForObjectId(object id, string tenantId);
    IDeletion HardDeletionForDocument(object document, string tenantId);

    /// <summary>
    ///     #959: forget the version or revision the session tracked for this id. The session's
    ///     <c>Eject</c> calls it after Weasel's <c>EjectById</c>, which drops the identity-map entry
    ///     but not the tracked version.
    /// </summary>
    void ClearTrackedVersions(IStorageSession session, object id);
}

/// <summary>
///     Non-generic-id load bridge for the session pipeline (#273 E2a): QuerySession's load
///     internals carry ids as <c>object</c>, so this closes the gap without the session
///     needing the TId generic argument.
/// </summary>
internal interface IPolecatObjectStorage<TDoc> : IPolecatObjectWriteStorage where TDoc : notnull
{
    Task<TDoc?> LoadByObjectIdAsync(object id, IStorageSession session, CancellationToken token);
    Task<IReadOnlyList<TDoc>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session, CancellationToken token);

    /// <summary>
    ///     Explicit-tenant load for the projection storage (#273 E2e), which may serve a
    ///     different tenant than the driving session. Bypasses identity-map flavor logic,
    ///     matching the bespoke projection reads.
    /// </summary>
    Task<TDoc?> LoadByObjectIdAsync(object id, IStorageSession session, string tenantId, CancellationToken token);

    Task<IReadOnlyList<TDoc>> LoadManyByObjectIdsAsync(IReadOnlyList<object> ids, IStorageSession session,
        string tenantId, CancellationToken token);

    IDeletion DeletionForObjectId(object id, string tenantId);
}

/// <summary>
///     Read-side seam for the batched-query load items (#273 doc-side convergence): composes the
///     closed-shape whole-document SELECT with positional parameters into a shared
///     <c>SqlBatch</c> command, and materializes rows through the closed-shape QueryOnly selector.
///     This retires the bespoke <c>DocumentProvider.SelectSql</c>/<c>LoadSql</c> +
///     <c>FromJson</c>/<c>SyncVersionProperties</c> load path from <c>Internal/Batching</c>.
/// </summary>
internal interface IPolecatBatchLoadStorage<TDoc> where TDoc : notnull
{
    /// <summary>Appends <c>SELECT … FROM … WHERE id = @p [AND tenant] [AND soft-delete]</c> (no trailing <c>;</c>).</summary>
    void WriteLoadByIdSql(Weasel.SqlServer.ICommandBuilder builder, object id, string tenantId);

    /// <summary>Appends <c>SELECT … FROM … WHERE id IN (…) [AND tenant] [AND soft-delete]</c> (no trailing <c>;</c>).</summary>
    void WriteLoadManySql(Weasel.SqlServer.ICommandBuilder builder, IReadOnlyList<object> ids, string tenantId);

    /// <summary>The closed-shape QueryOnly selector whose ordinals match the composed SELECT.</summary>
    ISelector<TDoc> BuildLoadSelector(IStorageSession session);
}

/// <summary>
///     Session-free bulk version-checked upsert seam (#273 doc-side convergence). Sources the
///     full write-column set from the closed-shape descriptor's write binders — including
///     <c>doc_type</c>, <c>guid_version</c>, the partition column and soft-delete defaults that
///     the bespoke <c>BuildVersionCheckedBatchCommand</c> silently omitted — while keeping the
///     bulk-specific strict version guard (<c>WHEN MATCHED AND version = expected</c>).
/// </summary>
internal interface IPolecatBulkVersionCheckStorage<TDoc> where TDoc : notnull
{
    /// <summary>
    ///     Appends one <c>MERGE … OUTPUT inserted.id</c> for the document + its expected version
    ///     (positional params), session-free. A row absent from the OUTPUT stream is a version
    ///     mismatch (neither updated nor inserted).
    /// </summary>
    void ConfigureVersionCheckedUpsert(Weasel.SqlServer.ICommandBuilder builder, TDoc document,
        long expectedVersion, string tenantId);

    /// <summary>The raw id value as it appears in the <c>OUTPUT inserted.id</c> stream.</summary>
    object BulkRawId(TDoc document);
}

/// <summary>
///     Set-based delete / undelete seam (#273 doc-side convergence). Exposes the closed-shape
///     operation fragments (the <c>DELETE FROM …</c> / <c>UPDATE … SET is_deleted = …</c>
///     prefixes) so the criteria-based delete/undelete operations compose off the shared storage
///     instead of hand-writing SQL from <c>DocumentMapping</c>. <see cref="DeleteFragment" /> and
///     <see cref="HardDeleteFragment" /> already live on <c>IDocumentStorage</c>; this adds the
///     undelete prefix and the tenancy flag the operations need.
/// </summary>
internal interface IPolecatDeletionStorage
{
    /// <summary>Soft-or-hard delete prefix (an <c>UPDATE … is_deleted = 1</c> for soft-delete
    /// types, a <c>DELETE FROM …</c> otherwise).</summary>
    IOperationFragment DeleteFragment { get; }

    /// <summary>Always a hard <c>DELETE FROM …</c>.</summary>
    IOperationFragment HardDeleteFragment { get; }

    /// <summary>
    ///     <c>UPDATE … SET is_deleted = 0, deleted_at = NULL</c>. Nullable because Weasel's storage
    ///     declares it so; Polecat's rules always supply one, and callers decide by
    ///     <see cref="IsSoftDeleted" /> (gh-558).
    /// </summary>
    IOperationFragment? UndeleteFragment { get; }

    /// <summary>#234: conjoined tables carry a <c>tenant_id</c> column to scope the WHERE by.</summary>
    bool IsConjoined { get; }

    /// <summary>
    ///     #419: true when <see cref="DeleteFragment" /> is the soft-delete <c>UPDATE</c> rather than
    ///     a hard <c>DELETE</c>. Criteria-based soft deletes have to exclude rows that are already
    ///     deleted, or they re-stamp <c>deleted_at</c> and destroy the real deletion instant.
    /// </summary>
    bool IsSoftDeleted { get; }
}

/// <summary>
///     #959: what every Polecat closed-shape storage adds to Weasel's — the four Polecat-only seams —
///     implemented once here as default interface members over <see cref="PolecatRules" /> and the Weasel
///     <see cref="IClosedShapeDocumentStorage{TDoc,TId}" /> members, so each tracking-mode class below is a
///     constructor and nothing else.
/// </summary>
/// <remarks>
///     Callers reach these through the Polecat interfaces they already cast to, which is what lets
///     default interface members carry the implementation.
/// </remarks>
internal interface IPolecatClosedShapeStorage<TDoc, TId>
    : IClosedShapeDocumentStorage<TDoc, TId>, IPolecatObjectStorage<TDoc>, IPolecatBatchLoadStorage<TDoc>,
        IPolecatBulkVersionCheckStorage<TDoc>, IPolecatDeletionStorage
    where TDoc : notnull
    where TId : notnull
{
    PolecatStorageRules<TDoc, TId> PolecatRules { get; }

    // ---- IPolecatDeletionStorage ----

    bool IPolecatDeletionStorage.IsConjoined => PolecatRules.IsConjoined;
    bool IPolecatDeletionStorage.IsSoftDeleted => PolecatRules.IsSoftDeleted;

    // ---- IPolecatObjectStorage (#273 E2a) ----

    Task<TDoc?> IPolecatObjectStorage<TDoc>.LoadByObjectIdAsync(object id, IStorageSession session,
        CancellationToken token)
        => LoadAsync(PolecatRules.NormalizeId(id), session, token);

    Task<IReadOnlyList<TDoc>> IPolecatObjectStorage<TDoc>.LoadManyByObjectIdsAsync(IReadOnlyList<object> ids,
        IStorageSession session, CancellationToken token)
        => LoadManyAsync(NormalizeIds(ids), session, token);

    /// <summary>
    ///     #273 E2e: the explicit-tenant load for projection storage, which may serve a different tenant
    ///     from the driving session. Weasel's own load reads only the session's tenant, so this builds the
    ///     command for the tenant it is given and resolves through this storage's selector.
    /// </summary>
    async Task<TDoc?> IPolecatObjectStorage<TDoc>.LoadByObjectIdAsync(object id, IStorageSession session,
        string tenantId, CancellationToken token)
    {
        var command = BuildLoadCommand(PolecatRules.NormalizeId(id), tenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);

        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return default;
        }

        return await selector.ResolveAsync(reader, token).ConfigureAwait(false);
    }

    async Task<IReadOnlyList<TDoc>> IPolecatObjectStorage<TDoc>.LoadManyByObjectIdsAsync(IReadOnlyList<object> ids,
        IStorageSession session, string tenantId, CancellationToken token)
    {
        var command = BuildLoadManyCommand(NormalizeIds(ids), tenantId);
        var selector = (ISelector<TDoc>)BuildSelector(session);
        var list = new List<TDoc>();

        await using var reader = await session.ExecuteReaderAsync(command, token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            list.Add(await selector.ResolveAsync(reader, token).ConfigureAwait(false));
        }

        return list;
    }

    IDeletion IPolecatObjectStorage<TDoc>.DeletionForObjectId(object id, string tenantId)
        => DeleteForId(PolecatRules.NormalizeId(id), tenantId);

    private TId[] NormalizeIds(IReadOnlyList<object> ids)
    {
        var typed = new TId[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            typed[i] = PolecatRules.NormalizeId(ids[i]);
        }

        return typed;
    }

    // ---- IPolecatObjectWriteStorage (#273 E2e) ----

    void IPolecatObjectWriteStorage.StoreObject(IStorageSession session, object document)
        => Store(session, (TDoc)document);

    void IPolecatObjectWriteStorage.StoreObject(IStorageSession session, object document, Guid? version)
        => Store(session, (TDoc)document, version);

    Guid? IPolecatObjectWriteStorage.MappedVersionForObject(object document) => MappedVersionFor((TDoc)document);
    long? IPolecatObjectWriteStorage.MappedRevisionForObject(object document) => MappedRevisionFor((TDoc)document);

    Weasel.Storage.IStorageOperation IPolecatObjectWriteStorage.UpsertObject(object document,
        IStorageSession session, string tenantId)
        => Upsert((TDoc)document, session, tenantId);

    Weasel.Storage.IStorageOperation IPolecatObjectWriteStorage.UpsertObjectProjected(object document,
        string tenantId)
        => UpsertProjected((TDoc)document, tenantId);

    IDeletion IPolecatObjectWriteStorage.HardDeletionForObjectId(object id, string tenantId)
        => HardDeleteForId(PolecatRules.NormalizeId(id), tenantId);

    IDeletion IPolecatObjectWriteStorage.HardDeletionForDocument(object document, string tenantId)
        => HardDeleteForDocument((TDoc)document, tenantId);

    void IPolecatObjectWriteStorage.ClearTrackedVersions(IStorageSession session, object id)
    {
        var typed = PolecatRules.NormalizeId(id);
        session.Versions.ClearVersion<TDoc, TId>(typed);
        session.Versions.ClearRevision<TDoc, TId>(typed);
    }

    // ---- IPolecatBatchLoadStorage (#273 doc-side convergence) ----

    void IPolecatBatchLoadStorage<TDoc>.WriteLoadByIdSql(Weasel.SqlServer.ICommandBuilder builder, object id,
        string tenantId)
        => PolecatRules.WriteLoadByIdSql(builder, PolecatRules.ReadSqlFor(TrackingMode).SelectClause, id, tenantId);

    void IPolecatBatchLoadStorage<TDoc>.WriteLoadManySql(Weasel.SqlServer.ICommandBuilder builder,
        IReadOnlyList<object> ids, string tenantId)
        => PolecatRules.WriteLoadManySql(builder, PolecatRules.ReadSqlFor(TrackingMode).SelectClause, ids, tenantId);

    ISelector<TDoc> IPolecatBatchLoadStorage<TDoc>.BuildLoadSelector(IStorageSession session)
        => (ISelector<TDoc>)BuildSelector(session);

    // ---- IPolecatBulkVersionCheckStorage (#273 doc-side convergence) ----

    void IPolecatBulkVersionCheckStorage<TDoc>.ConfigureVersionCheckedUpsert(Weasel.SqlServer.ICommandBuilder builder,
        TDoc document, long expectedVersion, string tenantId)
        => PolecatRules.ConfigureVersionCheckedUpsert(builder, document, Identity(document), expectedVersion, tenantId);

    object IPolecatBulkVersionCheckStorage<TDoc>.BulkRawId(TDoc document) => RawIdentityValue(Identity(document));
}

/// <summary>
///     #959: the query session's storage. Weasel's query-only base refuses writes; nothing in Polecat writes
///     through it (projection and bulk-insert writes take <see cref="PolecatLightweightStorage{TDoc,TId}" />).
/// </summary>
internal sealed class PolecatQueryOnlyStorage<TDoc, TId>
    : QueryOnlyClosedShapeDocumentStorage<TDoc, TId>, IPolecatClosedShapeStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public PolecatQueryOnlyStorage(DocumentStorageDescriptor<TDoc, TId> descriptor, PolecatStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
        PolecatRules = rules;
    }

    public PolecatStorageRules<TDoc, TId> PolecatRules { get; }
}

/// <summary>#959: the lightweight document session's storage.</summary>
internal sealed class PolecatLightweightStorage<TDoc, TId>
    : LightweightClosedShapeDocumentStorage<TDoc, TId>, IPolecatClosedShapeStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public PolecatLightweightStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        PolecatStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
        PolecatRules = rules;
    }

    public PolecatStorageRules<TDoc, TId> PolecatRules { get; }
}

/// <summary>
///     #959: the identity-map session's storage, which also fills the DirtyTracking slot: Polecat has no
///     dirty tracking, and a real dirty-tracking storage with nothing sweeping it would only pile up trackers.
/// </summary>
/// <remarks>
///     Storing a <em>different</em> instance under an id already in the identity map throws, as Marten does
///     (weasel#740), unless the type implements <see cref="IEquatable{T}" />. Polecat's own storage used to
///     replace the mapped instance silently, which is a lost update waiting to happen.
/// </remarks>
internal sealed class PolecatIdentityMapStorage<TDoc, TId>
    : IdentityMapClosedShapeDocumentStorage<TDoc, TId>, IPolecatClosedShapeStorage<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public PolecatIdentityMapStorage(DocumentStorageDescriptor<TDoc, TId> descriptor,
        PolecatStorageRules<TDoc, TId> rules)
        : base(descriptor, rules)
    {
        PolecatRules = rules;
    }

    public PolecatStorageRules<TDoc, TId> PolecatRules { get; }
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

/// <summary>
///     <c>d.tenant_id = @p0</c>, with the tenant id BOUND rather than interpolated.
/// </summary>
/// <remarks>
///     <para>
///     #665: this was a <see cref="HardCodedFilter" /> built by hand-doubling quotes —
///     <c>$"d.tenant_id = '{session.TenantId.Replace("'", "''")}'"</c>. The escaping was correct, and
///     doubling <c>'</c> is complete for a T-SQL literal, so this was hardening rather than a
///     vulnerability. It was also the last place in the repo where a value genuinely reachable from
///     a public API — <c>SessionOptions.TenantId</c>, <c>LightweightSession(tenantId)</c>, commonly a
///     header or a claim — landed in a quoted literal instead of a parameter, on convention rather
///     than through <see cref="SqlEscaping" />. One refactor away from being a finding.
///     </para>
///     <para>
///     ⚠️ Bound as <see cref="DbType.AnsiString" />, not the default unicode string. <c>tenant_id</c>
///     is <c>varchar</c>, and an nvarchar parameter against a varchar column forces a
///     CONVERT_IMPLICIT and loses the index seek — that is #363, and switching from a varchar
///     LITERAL to an nvarchar parameter here would have reintroduced it while looking like a pure
///     safety improvement.
///     </para>
/// </remarks>
internal sealed class TenantFilter : ISqlFragment
{
    private readonly string _tenantId;

    public TenantFilter(string tenantId) => _tenantId = tenantId;

    public void Apply(Weasel.Core.ICommandBuilder builder)
    {
        builder.Append("d.tenant_id = ");
        builder.AppendParameter(_tenantId).DbType = DbType.AnsiString;
    }
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
    private readonly bool _bindTenant;
    private readonly DocumentStorageDescriptor<TDoc, TId> _descriptor;
    private readonly Type _documentType;

    public ClosedShapeDeletion(string sql, TDoc? document, TId id, object rawId, string tenantId,
        DocumentStorageDescriptor<TDoc, TId> descriptor, Type documentType, bool bindTenant = true)
    {
        _sql = sql;
        _rawId = rawId;
        _tenantId = tenantId;
        _bindTenant = bindTenant;
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
        if (_bindTenant)
        {
            parameters[1].Value = _tenantId;
            _descriptor.Dialect.SetParameterType(parameters[1], StorageColumnType.String);
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
