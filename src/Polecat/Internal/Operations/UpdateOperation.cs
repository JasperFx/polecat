using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class UpdateOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;

    public UpdateOperation(object document, object id, string json, DocumentMapping mapping, string tenantId)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Update;
    public object Document => _document;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_mapping.QualifiedTableName}
            SET data = @data, version = version + 1,
                last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
            OUTPUT inserted.version
            WHERE id = @id AND tenant_id = @tenant_id;
            """;

        command.Parameters.AddWithValue("@id", _id);
        command.Parameters.AddWithValue("@data", _json);
        command.Parameters.AddWithValue("@dotnet_type", _mapping.DotNetTypeName);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await using var reader = await command.ExecuteReaderAsync(token);
        // If no rows affected, the document doesn't exist
        // For now, Update is a best-effort operation
    }
}
