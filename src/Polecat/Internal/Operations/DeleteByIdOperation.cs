using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class DeleteByIdOperation : IStorageOperation
{
    private readonly object _id;
    private readonly DocumentMapping _mapping;

    public DeleteByIdOperation(object id, DocumentMapping mapping)
    {
        _id = id;
        _mapping = mapping;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Delete;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"DELETE FROM {_mapping.QualifiedTableName} WHERE id = @id;";
        command.Parameters.AddWithValue("@id", _id);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
