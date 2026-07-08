using System.Data.Common;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

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
    public OperationRole Role() => OperationRole.Update;
    public object? DocumentId => _id;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // #234: single-tenant tables have no tenant_id column.
        if (_mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            builder.Append(
                $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL WHERE id = @id AND tenant_id = @tenant_id;");
            builder.AddParameters(new Dictionary<string, object?> { ["id"] = _id, ["tenant_id"] = _tenantId });
        }
        else
        {
            builder.Append(
                $"UPDATE {_mapping.QualifiedTableName} SET is_deleted = 0, deleted_at = NULL WHERE id = @id;");
            builder.AddParameters(new Dictionary<string, object?> { ["id"] = _id });
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
