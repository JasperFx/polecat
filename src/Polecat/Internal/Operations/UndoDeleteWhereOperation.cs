using System.Data.Common;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class UndoDeleteWhereOperation : IStorageOperation
{
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;

    public UndoDeleteWhereOperation(DocumentMapping mapping, string tenantId, ISqlFragment whereFragment)
    {
        _mapping = mapping;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role() => OperationRole.Update;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append(
            $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL WHERE ");
        if (_mapping.TenancyStyle == TenancyStyle.Conjoined) // #234
        {
            builder.Append("tenant_id = ");
            builder.AppendParameter(_tenantId);
            builder.Append(" AND ");
        }

        builder.Append("is_deleted = 1 AND ");
        _whereFragment.Apply(builder);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
