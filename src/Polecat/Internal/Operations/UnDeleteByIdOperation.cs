using System.Data.Common;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Undelete a single soft-deleted document by id. Applies the closed-shape undelete fragment
///     (<c>UPDATE … SET is_deleted = 0, deleted_at = NULL</c>) then <c>WHERE id = ?</c> (+ tenant
///     when conjoined). #273 doc-side convergence — the SQL prefix + tenancy come from the shared
///     closed-shape storage, not from <c>DocumentMapping</c>.
/// </summary>
internal class UnDeleteByIdOperation : IStorageOperation
{
    private readonly IOperationFragment _undeleteFragment;
    private readonly bool _conjoined;
    private readonly object _id;
    private readonly string _tenantId;
    private readonly Type _documentType;

    public UnDeleteByIdOperation(IOperationFragment undeleteFragment, bool conjoined, object id, string tenantId,
        Type documentType)
    {
        _undeleteFragment = undeleteFragment;
        _conjoined = conjoined;
        _id = id;
        _tenantId = tenantId;
        _documentType = documentType;
    }

    public Type DocumentType => _documentType;
    public OperationRole Role() => OperationRole.Update;
    public object? DocumentId => _id;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        _undeleteFragment.Apply(builder);
        builder.Append(" WHERE id = ");
        builder.AppendParameter(_id, _id is string ? System.Data.SqlDbType.VarChar : null);
        // #234: single-tenant tables have no tenant_id column.
        if (_conjoined)
        {
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(_tenantId, System.Data.SqlDbType.VarChar);
        }

        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
