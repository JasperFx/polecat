using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;

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
    public OperationRole Role => OperationRole.Update;

    public void ConfigureCommand(SqlCommand command)
    {
        var builder = new CommandBuilder();
        builder.Append($"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL WHERE tenant_id = ");
        builder.AppendParameter(_tenantId);
        builder.Append(" AND is_deleted = 1 AND ");
        _whereFragment.Apply(builder);
        builder.Append(";");
        builder.ApplyTo(command);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
