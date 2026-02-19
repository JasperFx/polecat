using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Bulk DELETE FROM ... WHERE predicate. Used for hard deletes and
///     for non-soft-deleted types.
/// </summary>
internal class DeleteWhereOperation : IStorageOperation
{
    private readonly DocumentMapping _mapping;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;

    public DeleteWhereOperation(DocumentMapping mapping, string tenantId, ISqlFragment whereFragment)
    {
        _mapping = mapping;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
    }

    public Type DocumentType => _mapping.DocumentType;
    public OperationRole Role => OperationRole.Delete;

    public void ConfigureCommand(SqlCommand command)
    {
        var builder = new CommandBuilder();
        builder.Append($"DELETE FROM {_mapping.QualifiedTableName} WHERE tenant_id = ");
        builder.AppendParameter(_tenantId);
        builder.Append(" AND ");
        _whereFragment.Apply(builder);
        builder.Append(";");
        builder.ApplyTo(command);
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
