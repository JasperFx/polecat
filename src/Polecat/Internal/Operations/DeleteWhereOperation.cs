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
    private readonly bool _excludeAlreadyDeleted;
    private readonly string _tenantId;
    private readonly ISqlFragment _whereFragment;
    private readonly Type _documentType;

    public DeleteWhereOperation(IOperationFragment fragment, bool conjoined, string tenantId,
        ISqlFragment whereFragment, Type documentType, bool excludeAlreadyDeleted = false)
    {
        _fragment = fragment;
        _conjoined = conjoined;
        _tenantId = tenantId;
        _whereFragment = whereFragment;
        _documentType = documentType;
        _excludeAlreadyDeleted = excludeAlreadyDeleted;
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
            builder.AppendParameter(_tenantId, System.Data.SqlDbType.VarChar);
            builder.Append(" AND ");
        }

        // #419: a soft DeleteWhere is an UPDATE that stamps deleted_at, so it has to skip rows that
        // are already deleted -- otherwise a later bulk delete whose predicate still matches them
        // moves their deleted_at forward and the real deletion instant is gone for good, taking
        // DeletedSince / DeletedBefore and any retention sweep with it. This matches the guard the
        // by-id soft delete already carries (PolecatDocumentStorage.BuildDeletion) and the one
        // UndoDeleteWhereOperation carries. HardDeleteWhere never sets this: a hard delete of an
        // already-soft-deleted row is a real delete and must proceed. Appended ahead of the caller's
        // predicate for the same reason the tenant scope is -- a compound user predicate arrives
        // parenthesized and cannot swallow it.
        if (_excludeAlreadyDeleted)
        {
            builder.Append("is_deleted = 0 AND ");
        }

        _whereFragment.Apply(builder);
        builder.Append(";");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
