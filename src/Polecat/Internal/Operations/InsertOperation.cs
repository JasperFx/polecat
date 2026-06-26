using System.Data.Common;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Metadata;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class InsertOperation : IStorageOperation
{
    private readonly object _document;
    private readonly object _id;
    private readonly string _json;
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly Guid _newGuidVersion;
    private readonly DocumentMetadataValues _metadata;

    public InsertOperation(object document, object id, string json, DocumentMapping mapping, string tenantId,
        DocumentMetadataValues metadata = default)
    {
        _document = document;
        _id = id;
        _json = json;
        _mapping = mapping;
        _tenantId = tenantId;
        _newGuidVersion = mapping.UseOptimisticConcurrency ? Guid.NewGuid() : Guid.Empty;
        _metadata = metadata;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role() => OperationRole.Insert;
    public object? DocumentId => _id;
    public object Document => _document;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var docTypeCol = _mapping.IsHierarchy() ? ", doc_type" : "";
        var docTypeVal = _mapping.IsHierarchy() ? ", @doc_type" : "";
        // #241: append the opt-in metadata columns after the partition columns.
        var pCols = _mapping.PartitionInsertColumns + _mapping.MetadataInsertColumns;
        var pVals = _mapping.PartitionInsertValues + _mapping.MetadataInsertValues;

        if (_mapping.UseOptimisticConcurrency)
        {
            builder.Append($"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, guid_version, last_modified, created_at, dotnet_type{docTypeCol}, tenant_id{pCols})
                OUTPUT inserted.version, inserted.guid_version
                VALUES (@id, @data, 1, @new_guid_version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type{docTypeVal}, @tenant_id{pVals});
                """);

            var parameters = new Dictionary<string, object?>
            {
                ["id"] = _id, ["data"] = _json, ["new_guid_version"] = _newGuidVersion,
                ["dotnet_type"] = _mapping.DotNetTypeName, ["tenant_id"] = _tenantId
            };
            if (_mapping.IsHierarchy()) parameters["doc_type"] = _mapping.AliasFor(_document.GetType());
            if (_mapping.HasPartitionColumn) parameters["partition_value"] = _mapping.Partitioning!.GetValue(_document);
            builder.AddParameters(parameters);
            _metadata.AddParameters(builder, _mapping);
        }
        else
        {
            builder.Append($"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, last_modified, created_at, dotnet_type{docTypeCol}, tenant_id{pCols})
                OUTPUT inserted.version
                VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type{docTypeVal}, @tenant_id{pVals});
                """);

            var parameters = new Dictionary<string, object?>
            {
                ["id"] = _id, ["data"] = _json,
                ["dotnet_type"] = _mapping.DotNetTypeName, ["tenant_id"] = _tenantId
            };
            if (_mapping.IsHierarchy()) parameters["doc_type"] = _mapping.AliasFor(_document.GetType());
            if (_mapping.HasPartitionColumn) parameters["partition_value"] = _mapping.Partitioning!.GetValue(_document);
            builder.AddParameters(parameters);
            _metadata.AddParameters(builder, _mapping);
        }
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        if (await reader.ReadAsync(token))
        {
            var newVersion = reader.GetInt64(0);

            if (_mapping.UseNumericRevisions)
            {
                if (_document is ILongVersioned longVersioned)
                {
                    longVersioned.Version = newVersion;
                }
                else if (_document is IRevisioned revisioned)
                {
                    revisioned.Version = (int)newVersion;
                }
            }

            if (_mapping.UseOptimisticConcurrency && _document is IVersioned versioned)
            {
                versioned.Version = reader.GetGuid(1);
            }
        }
    }
}
