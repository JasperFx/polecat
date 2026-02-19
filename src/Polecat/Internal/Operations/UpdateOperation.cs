using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Metadata;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

internal class UpdateOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly int _expectedRevision;
    private readonly Guid? _expectedGuidVersion;
    private readonly Guid _newGuidVersion;

    public UpdateOperation(object document, object id, string json, DocumentMapping mapping, string tenantId,
        int expectedRevision = 0, Guid? expectedGuidVersion = null)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
        _expectedRevision = expectedRevision;
        _expectedGuidVersion = expectedGuidVersion;
        _newGuidVersion = mapping.UseOptimisticConcurrency ? Guid.NewGuid() : Guid.Empty;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Update;
    public object? DocumentId => _id;
    public object Document => _document;

    public void ConfigureCommand(SqlCommand command)
    {
        if (_mapping.UseOptimisticConcurrency)
        {
            ConfigureGuidVersionCommand(command);
        }
        else if (_mapping.UseNumericRevisions)
        {
            ConfigureRevisionCommand(command);
        }
        else
        {
            ConfigureStandardCommand(command);
        }
    }

    private void ConfigureStandardCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_mapping.QualifiedTableName}
            SET data = @data, version = version + 1,
                last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
            OUTPUT inserted.version
            WHERE id = @id AND tenant_id = @tenant_id;
            """;

        AddBaseParameters(command);
    }

    private void ConfigureRevisionCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_mapping.QualifiedTableName}
            SET data = @data, version = version + 1,
                last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
            OUTPUT inserted.version
            WHERE id = @id AND tenant_id = @tenant_id
                AND (@expected_version = 0 OR version = @expected_version);
            """;

        AddBaseParameters(command);
        command.Parameters.AddWithValue("@expected_version", _expectedRevision);
    }

    private void ConfigureGuidVersionCommand(SqlCommand command)
    {
        command.CommandText = $"""
            UPDATE {_mapping.QualifiedTableName}
            SET data = @data, version = version + 1, guid_version = @new_guid_version,
                last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
            OUTPUT inserted.version, inserted.guid_version
            WHERE id = @id AND tenant_id = @tenant_id
                AND (@expected_guid_version IS NULL OR guid_version = @expected_guid_version);
            """;

        AddBaseParameters(command);
        command.Parameters.AddWithValue("@expected_guid_version",
            _expectedGuidVersion.HasValue && _expectedGuidVersion.Value != Guid.Empty
                ? _expectedGuidVersion.Value
                : DBNull.Value);
        command.Parameters.AddWithValue("@new_guid_version", _newGuidVersion);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
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
        else if (_mapping.UseNumericRevisions && _expectedRevision > 0)
        {
            throw new ConcurrencyException(_mapping.DocumentType, _id);
        }
        else if (_mapping.UseOptimisticConcurrency &&
                 _expectedGuidVersion.HasValue && _expectedGuidVersion.Value != Guid.Empty)
        {
            throw new ConcurrencyException(_mapping.DocumentType, _id);
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
