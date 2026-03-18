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
    public OperationRole Role() => OperationRole.Insert;
    public object? DocumentId => _id;
    public object Document => _document;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var docTypeCol = _mapping.IsHierarchy() ? ", doc_type" : "";
        var docTypeVal = _mapping.IsHierarchy() ? ", @doc_type" : "";

        if (_mapping.UseOptimisticConcurrency)
        {
            builder.Append($"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, guid_version, last_modified, created_at, dotnet_type{docTypeCol}, tenant_id)
                OUTPUT inserted.version, inserted.guid_version
                VALUES (@id, @data, 1, @new_guid_version, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type{docTypeVal}, @tenant_id);
                """);

            var parameters = new Dictionary<string, object?>
            {
                ["id"] = _id, ["data"] = _json, ["new_guid_version"] = _newGuidVersion,
                ["dotnet_type"] = _mapping.DotNetTypeName, ["tenant_id"] = _tenantId
            };
            if (_mapping.IsHierarchy()) parameters["doc_type"] = _mapping.AliasFor(_document.GetType());
            builder.AddParameters(parameters);
        }
        else
        {
            builder.Append($"""
                INSERT INTO {_mapping.QualifiedTableName} (id, data, version, last_modified, created_at, dotnet_type{docTypeCol}, tenant_id)
                OUTPUT inserted.version
                VALUES (@id, @data, 1, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET(), @dotnet_type{docTypeVal}, @tenant_id);
                """);

            var parameters = new Dictionary<string, object?>
            {
                ["id"] = _id, ["data"] = _json,
                ["dotnet_type"] = _mapping.DotNetTypeName, ["tenant_id"] = _tenantId
            };
            if (_mapping.IsHierarchy()) parameters["doc_type"] = _mapping.AliasFor(_document.GetType());
            builder.AddParameters(parameters);
        }
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
    }
}
