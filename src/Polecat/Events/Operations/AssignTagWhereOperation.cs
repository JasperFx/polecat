using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Linq.SqlGeneration;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Operations;

/// <summary>
/// Retroactively assigns a tag to all events matching a WHERE clause.
/// For non-conjoined tenancy generates:
///   INSERT INTO [schema].[pc_event_tag_{suffix}] (value, seq_id)
///   SELECT @value, e.seq_id FROM [schema].[pc_events] e
///   WHERE {where}
///   AND NOT EXISTS (SELECT 1 FROM [schema].[pc_event_tag_{suffix}] x WHERE x.value = @value AND x.seq_id = e.seq_id)
/// For conjoined tenancy, includes tenant_id in INSERT and NOT EXISTS check, and tags only the session
/// tenant's events: under per-tenant sequences the same seq_id exists in every tenant (#842).
/// </summary>
internal class AssignTagWhereOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
{
    private readonly string _schemaName;
    private readonly ITagTypeRegistration _registration;
    private readonly object _value;
    private readonly ISqlFragment _whereFragment;
    private readonly bool _isConjoined;
    private readonly bool _useArchivedPartitioning;
    private readonly string? _tenantOrdinalColumn;
    private readonly string? _tenantId;

    /// <param name="tenantOrdinalColumn">
    ///     The partition column under per-tenant partitioning, which a tag row needs for its foreign key
    ///     to <c>pc_events</c>. Null otherwise.
    /// </param>
    public AssignTagWhereOperation(string schemaName, ITagTypeRegistration registration, object value,
        ISqlFragment whereFragment, bool isConjoined = false, string? tenantId = null,
        bool useArchivedPartitioning = false, string? tenantOrdinalColumn = null)
    {
        _schemaName = schemaName;
        _registration = registration;
        _value = value;
        _whereFragment = whereFragment;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
        _useArchivedPartitioning = useArchivedPartitioning;
        _tenantOrdinalColumn = tenantOrdinalColumn;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var tagTable = Polecat.Internal.SqlEscaping.QualifiedName(
            _schemaName, EventGraph.TagTableNameFor(_registration));
        var eventsTable = Polecat.Internal.SqlEscaping.QualifiedName(_schemaName, "pc_events");

        builder.Append($"INSERT INTO {tagTable} (value");
        if (_isConjoined) builder.Append(", tenant_id");
        builder.Append(", seq_id");
        if (_tenantOrdinalColumn != null) builder.Append($", {_tenantOrdinalColumn}");

        // When pc_events is partitioned by is_archived the tag table also carries is_archived, in its
        // primary key -- see EventTagTable. Carry the source row's flag through so an event that's
        // already been archived doesn't lose its tag rows.
        if (_useArchivedPartitioning) builder.Append(", is_archived");

        builder.Append(") SELECT ");
        builder.AppendParameter(_value);
        if (_isConjoined)
        {
            builder.Append(", ");
            builder.AppendParameter(_tenantId!);
        }

        builder.Append(", e.seq_id");
        if (_tenantOrdinalColumn != null) builder.Append($", e.{_tenantOrdinalColumn}");
        if (_useArchivedPartitioning) builder.Append(", e.is_archived");

        builder.Append($" FROM {eventsTable} e WHERE ");
        _whereFragment.Apply(builder);

        if (_isConjoined)
        {
            builder.Append(" AND e.tenant_id = ");
            builder.AppendParameter(_tenantId!, System.Data.SqlDbType.VarChar);
        }

        // is_archived is deliberately left out of this check even though it is in the tag table's key.
        // The flag on a tag row goes stale when its stream is archived, so matching on it would write a
        // second row for an event that already carries the tag.
        builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
        builder.AppendParameter(_value);
        if (_isConjoined)
        {
            builder.Append(" AND x.tenant_id = ");
            builder.AppendParameter(_tenantId!);
        }

        builder.Append(" AND x.seq_id = e.seq_id);");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
