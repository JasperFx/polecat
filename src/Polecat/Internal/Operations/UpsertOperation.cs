using Microsoft.Data.SqlClient;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class UpsertOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;

    public UpsertOperation(object document, object id, string json, DocumentMapping mapping, string tenantId)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Upsert;
    public object Document => _document;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
            USING (SELECT @id AS id) AS source ON target.id = source.id
            WHEN MATCHED THEN
              UPDATE SET data = @data, version = target.version + 1,
                last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
            WHEN NOT MATCHED THEN
              INSERT (id, data, version, last_modified, dotnet_type, tenant_id)
              VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id)
            OUTPUT inserted.version;
            """;

        AddParameters(command);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await using var reader = await command.ExecuteReaderAsync(token);
        // Read the OUTPUT version if needed in the future
        if (await reader.ReadAsync(token))
        {
            // version is available at reader.GetInt32(0) if needed
        }
    }

    private void AddParameters(SqlCommand command)
    {
        command.Parameters.AddWithValue("@id", _id);
        command.Parameters.AddWithValue("@data", _json);
        command.Parameters.AddWithValue("@dotnet_type", _mapping.DotNetTypeName);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }
}
