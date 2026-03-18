using System.Data.Common;
using JasperFx;
using Polecat.Metadata;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class UpsertOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly int _expectedRevision;
    private readonly Guid? _expectedGuidVersion;
    private readonly Guid _newGuidVersion;

    public UpsertOperation(object document, object id, string json, DocumentMapping mapping, string tenantId,
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
    public OperationRole Role() => OperationRole.Upsert;
    public object? DocumentId => _id;
    public object Document => _document;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        if (_mapping.UseOptimisticConcurrency)
        {
            ConfigureGuidVersionCommand(builder);
        }
        else if (_mapping.UseNumericRevisions)
        {
            ConfigureRevisionCommand(builder);
        }
        else
        {
            ConfigureStandardCommand(builder);
        }
    }

    private void ConfigureStandardCommand(ICommandBuilder builder)
    {
        if (_mapping.IsHierarchy())
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED THEN
                  UPDATE SET data = @data, version = target.version + 1,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type, doc_type = @doc_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id, doc_type)
                  VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id, @doc_type)
                OUTPUT inserted.version;
                """);
        }
        else
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED THEN
                  UPDATE SET data = @data, version = target.version + 1,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id)
                  VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id)
                OUTPUT inserted.version;
                """);
        }

        AddBaseParameters(builder);
    }

    private void ConfigureRevisionCommand(ICommandBuilder builder)
    {
        if (_mapping.IsHierarchy())
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED AND (@expected_version = 0 OR target.version = @expected_version) THEN
                  UPDATE SET data = @data, version = target.version + 1,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type, doc_type = @doc_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id, doc_type)
                  VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id, @doc_type)
                OUTPUT inserted.version;
                """);
        }
        else
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED AND (@expected_version = 0 OR target.version = @expected_version) THEN
                  UPDATE SET data = @data, version = target.version + 1,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, last_modified, created_at, dotnet_type, tenant_id)
                  VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id)
                OUTPUT inserted.version;
                """);
        }

        AddBaseParameters(builder);
        builder.AddParameters(new Dictionary<string, object?> { ["expected_version"] = _expectedRevision });
    }

    private void ConfigureGuidVersionCommand(ICommandBuilder builder)
    {
        if (_mapping.IsHierarchy())
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED AND (@expected_guid_version IS NULL OR target.guid_version = @expected_guid_version) THEN
                  UPDATE SET data = @data, version = target.version + 1, guid_version = @new_guid_version,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type, doc_type = @doc_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, guid_version, last_modified, created_at, dotnet_type, tenant_id, doc_type)
                  VALUES (@id, @data, 1, @new_guid_version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id, @doc_type)
                OUTPUT inserted.version, inserted.guid_version;
                """);
        }
        else
        {
            builder.Append($"""
                MERGE INTO {_mapping.QualifiedTableName} WITH (HOLDLOCK) AS target
                USING (SELECT @id AS id, @tenant_id AS tenant_id) AS source
                    ON target.id = source.id AND target.tenant_id = source.tenant_id
                WHEN MATCHED AND (@expected_guid_version IS NULL OR target.guid_version = @expected_guid_version) THEN
                  UPDATE SET data = @data, version = target.version + 1, guid_version = @new_guid_version,
                    last_modified = SYSDATETIMEOFFSET(), dotnet_type = @dotnet_type
                WHEN NOT MATCHED THEN
                  INSERT (id, data, version, guid_version, last_modified, created_at, dotnet_type, tenant_id)
                  VALUES (@id, @data, 1, @new_guid_version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type, @tenant_id)
                OUTPUT inserted.version, inserted.guid_version;
                """);
        }

        AddBaseParameters(builder);
        builder.AddParameters(new Dictionary<string, object?>
        {
            ["expected_guid_version"] = _expectedGuidVersion.HasValue && _expectedGuidVersion.Value != Guid.Empty
                ? _expectedGuidVersion.Value
                : DBNull.Value,
            ["new_guid_version"] = _newGuidVersion
        });
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
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

    private void AddBaseParameters(ICommandBuilder builder)
    {
        builder.AddParameters(new Dictionary<string, object?>
        {
            ["id"] = _id, ["data"] = _json,
            ["dotnet_type"] = _mapping.DotNetTypeName, ["tenant_id"] = _tenantId
        });

        if (_mapping.IsHierarchy())
        {
            builder.AddParameters(new Dictionary<string, object?>
            {
                ["doc_type"] = _mapping.AliasFor(_document.GetType())
            });
        }
    }
}
