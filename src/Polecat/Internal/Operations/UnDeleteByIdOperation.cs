using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class UnDeleteByIdOperation : IStorageOperation
{
    private readonly object _id;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;

    public UnDeleteByIdOperation(object id, DocumentMapping mapping, string tenantId)
    {
        _id = id;
        _mapping = mapping;
        _tenantId = tenantId;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Update;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText =
            $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL WHERE id = @id AND tenant_id = @tenant_id;";
        command.Parameters.AddWithValue("@id", _id);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
