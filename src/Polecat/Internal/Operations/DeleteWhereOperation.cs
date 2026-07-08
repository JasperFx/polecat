using System.Data.Common;
using Polecat.Linq.SqlGeneration;
using Polecat.Storage;
using Weasel.Core;
using Weasel.SqlServer;

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
    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        // #234: single-tenant tables have no tenant_id column to scope the delete by.
        builder.Append($"DELETE FROM {_mapping.QualifiedTableName} WHERE ");
        if (_mapping.TenancyStyle == TenancyStyle.Conjoined)
        {
            builder.Append("tenant_id = ");
            builder.AppendParameter(_tenantId);
            builder.Append(" AND ");
        }

        _whereFragment.Apply(builder);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
