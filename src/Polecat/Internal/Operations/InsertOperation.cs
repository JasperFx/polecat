using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class InsertOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;

    public InsertOperation(object document, object id, string json, DocumentMapping mapping, string tenantId)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Insert;
    public object Document => _document;

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = $"""
            INSERT INTO {_mapping.QualifiedTableName} (id, data, version, last_modified, dotnet_type, tenant_id)
            OUTPUT inserted.version
            VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id);
            """;

        command.Parameters.AddWithValue("@id", _id);
        command.Parameters.AddWithValue("@data", _json);
        command.Parameters.AddWithValue("@dotnet_type", _mapping.DotNetTypeName);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        try
        {
            await using var reader = await command.ExecuteReaderAsync(token);
            if (await reader.ReadAsync(token))
            {
                // version is available at reader.GetInt32(0)
            }
        }
        catch (SqlException ex) when (ex.Number == 2627) // PK violation
        {
            throw new DocumentAlreadyExistsException(_mapping.DocumentType, _id);
        }
    }
}
