using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class InsertOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly Guid _newGuidVersion;

    public InsertOperation(object document, object id, string json, DocumentMapping mapping, string tenantId)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
        _newGuidVersion = mapping.UseOptimisticConcurrency ? Guid.NewGuid() : Guid.Empty;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Insert;
    public object Document => _document;

    public void ConfigureCommand(SqlCommand command)
    {
        if (_mapping.UseOptimisticConcurrency)
        {
            command.CommandText = $"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, guid_version, last_modified, dotnet_type, tenant_id)
                OUTPUT inserted.version, inserted.guid_version
                VALUES (@id, @data, 1, @new_guid_version, SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id);
                """;

            AddBaseParameters(command);
            command.Parameters.AddWithValue("@new_guid_version", _newGuidVersion);
        }
        else
        {
            command.CommandText = $"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, last_modified, dotnet_type, tenant_id)
                OUTPUT inserted.version
                VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id);
                """;

            AddBaseParameters(command);
        }
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        try
        {
            await using var reader = await command.ExecuteReaderAsync(token);
            if (await reader.ReadAsync(token))
            {
                var newVersion = reader.GetInt32(0);

                if (_mapping.UseNumericRevisions && _document is IRevisioned revisioned)
                {
                    revisioned.Version = newVersion;
                }

                if (_mapping.UseOptimisticConcurrency && _document is IVersioned versioned)
                {
                    versioned.Version = reader.GetGuid(1);
                }
            }
        }
        catch (SqlException ex) when (ex.Number == 2627) // PK violation
        {
            throw new DocumentAlreadyExistsException(_mapping.DocumentType, _id);
        }
    }

    private void AddBaseParameters(SqlCommand command)
    {
        command.Parameters.AddWithValue("@id", _id);
        command.Parameters.AddWithValue("@data", _json);
        command.Parameters.AddWithValue("@dotnet_type", _mapping.DotNetTypeName);
        command.Parameters.AddWithValue("@tenant_id", _tenantId);
    }
}
