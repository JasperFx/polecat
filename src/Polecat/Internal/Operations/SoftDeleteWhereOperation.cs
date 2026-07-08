using System.Data.Common;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

/// <summary>
///     Bulk UPDATE ... SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET() WHERE predicate.
///     Used for soft-deleted types when DeleteWhere is called.
/// </summary>
internal class SoftDeleteWhereOperation : IStorageOperation
{
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;

    public SoftDeleteWhereOperation(DocumentMapping mapping, string tenantId, ISqlFragment whereFragment)
    {
        _mapping = mapping;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append(
            $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 1, deleted_at = SYSDATETIMEOFFSET() WHERE ");
        if (_mapping.TenancyStyle == TenancyStyle.Conjoined) // #234
        {
            builder.Append("tenant_id = ");
            builder.AppendParameter(_tenantId);
            builder.Append(" AND ");
        }

        _whereFragment.Apply(builder);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
