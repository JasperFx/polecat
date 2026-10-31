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
internal class UndoDeleteWhereOperation : IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly IOperationFragment _undeleteFragment;
    private readonly bool _conjoined;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;
    private readonly Type _documentType;
    private readonly bool _movesVersion;

    /// <param name="movesVersion">
    ///     #813: also move <c>version</c> and <c>last_modified</c> on, as every other write does. Only
    ///     the diagnostics writer's undelete asks for it: the jasperfx#1059 contract returns the
    ///     restored document "live, with a new version", so a console holding the deleted row's
    ///     version can tell the restore happened. <c>UndoDeleteWhere</c> itself keeps its long-standing
    ///     shape, which leaves both columns alone — changing it would move every revisioned
    ///     document's revision under an application that never asked.
    /// </param>
    public UndoDeleteWhereOperation(IOperationFragment undeleteFragment, bool conjoined, string tenantId,
        ISqlFragment whereFragment, Type documentType, bool movesVersion = false)
    {
        _undeleteFragment = undeleteFragment;
        _conjoined = conjoined;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
        _documentType = documentType;
        _movesVersion = movesVersion;
    }

    public Type DocumentType => _documentType;
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        _undeleteFragment.Apply(builder);
        if (_movesVersion)
        {
            builder.Append(", version = version + 1, last_modified = SYSDATETIMEOFFSET()");
        }

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
