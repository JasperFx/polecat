using System.Data.Common;
using Polecat.Linq.SqlGeneration;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Criteria-based undelete: applies the closed-shape undelete fragment
///     (<c>UPDATE … SET is_deleted = 0, deleted_at = NULL</c>), scoped to currently-deleted rows
///     matching the tenant + LINQ predicate. #273 doc-side convergence — the SQL prefix + tenancy
///     come from the shared closed-shape storage, not from <c>DocumentMapping</c>.
/// </summary>
internal class UndoDeleteWhereOperation : IStorageOperation
{
    private readonly IOperationFragment _undeleteFragment;
    private readonly bool _conjoined;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;
    private readonly Type _documentType;

    public UndoDeleteWhereOperation(IOperationFragment undeleteFragment, bool conjoined, string tenantId,
        ISqlFragment whereFragment, Type documentType)
    {
        _undeleteFragment = undeleteFragment;
        _conjoined = conjoined;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
        _documentType = documentType;
    }

    public Type DocumentType => _documentType;
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        _undeleteFragment.Apply(builder);
        builder.Append(" WHERE ");
        if (_conjoined) // #234
        {
            builder.Append("tenant_id = ");
            builder.AppendParameter(_tenantId, System.Data.SqlDbType.VarChar);
            builder.Append(" AND ");
        }

        builder.Append("is_deleted = 1 AND ");
        _whereFragment.Apply(builder);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
