using System.Data.Common;
using System.Reflection;
using JasperFx.Core.Reflection;
using Weasel.Storage;

namespace Polecat.Storage;

/// <summary>
///     Write-capable tenant-id binder for Polecat's always-present <c>tenant_id</c> column
///     (#273 phase D). The shared <see cref="DocumentTenantIdBinder{TDoc}" /> is read-only
///     because Marten binds the tenant inline via the conjoined parameter slot; Polecat's
///     single-tenant tables still carry <c>tenant_id</c> (default tenant id), so this binder
///     writes the session's tenant through the ordinary client-side binder loop instead.
/// </summary>
internal sealed class PolecatTenantIdBinder<TDoc> : IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly Action<TDoc, string>? _setter;

    public PolecatTenantIdBinder(string columnName, MemberInfo? tenantIdMember)
    {
        ColumnName = columnName;
        if (tenantIdMember is not null)
        {
            _setter = LambdaBuilder.Setter<TDoc, string>(tenantIdMember);
        }
    }

    public string ColumnName { get; }

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        parameter.Value = session.TenantId;
        parameter.DbType = System.Data.DbType.String;
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        if (_setter is null || reader.IsDBNull(columnOrdinal)) return;
        _setter(document, reader.GetString(columnOrdinal));
    }

    /// <summary>Bulk insert binds tenant per batch, not per document.</summary>
    public BulkColumnValue GetBulkValue(TDoc document) => BulkColumnValue.TypedNull(StorageColumnType.String);
}
