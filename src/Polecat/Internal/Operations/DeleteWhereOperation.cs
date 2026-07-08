using System.Data.Common;
using Polecat.Linq.SqlGeneration;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Internal.Operations;

/// <summary>
///     Criteria-based delete. Applies the closed-shape delete fragment — a soft-delete
///     <c>UPDATE … SET is_deleted = 1</c> or a hard <c>DELETE FROM …</c>, depending on which
///     fragment the session passed — then the tenant scope and the LINQ predicate. #273 doc-side
///     convergence: the SQL prefix + tenancy come from the shared closed-shape storage, not from
///     a hand-written <c>DocumentMapping</c> template. Serves both <c>DeleteWhere</c>
///     (soft-or-hard per the type) and <c>HardDeleteWhere</c> (the hard fragment).
/// </summary>
internal class DeleteWhereOperation : IStorageOperation
{
    private readonly IOperationFragment _fragment;
    private readonly bool _conjoined;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;
    private readonly Type _documentType;

    public DeleteWhereOperation(IOperationFragment fragment, bool conjoined, string tenantId,
        ISqlFragment whereFragment, Type documentType)
    {
        _fragment = fragment;
        _conjoined = conjoined;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
        _documentType = documentType;
    }

    public Type DocumentType => _documentType;
    public OperationRole Role() => OperationRole.Deletion;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        _fragment.Apply(builder);
        builder.Append(" WHERE ");
        // #234: single-tenant tables have no tenant_id column to scope the delete by.
        if (_conjoined)
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
