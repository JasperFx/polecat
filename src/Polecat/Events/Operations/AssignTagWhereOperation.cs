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
/// For conjoined tenancy, includes tenant_id in INSERT and NOT EXISTS check.
/// </summary>
internal class AssignTagWhereOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _schemaName;
    private readonly ITagTypeRegistration _registration;
    private readonly object _value;
    private readonly ISqlFragment _whereFragment;
    private readonly bool _isConjoined;
    private readonly bool _useArchivedPartitioning;
    private readonly string? _tenantId;

    public AssignTagWhereOperation(string schemaName, ITagTypeRegistration registration, object value,
        ISqlFragment whereFragment, bool isConjoined = false, string? tenantId = null,
        bool useArchivedPartitioning = false)
    {
        _schemaName = schemaName;
        _registration = registration;
        _value = value;
        _whereFragment = whereFragment;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
        _useArchivedPartitioning = useArchivedPartitioning;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var tagTable = $"[{_schemaName}].[pc_event_tag_{_registration.TableSuffix}]";
        var eventsTable = $"[{_schemaName}].[pc_events]";

        // When pc_events is partitioned by is_archived the tag table also carries
        // is_archived (PK + FK columns) — see EventTagTable. Carry the source row's
        // is_archived through into the tag insert so the FK matches and an event
        // that's already been archived doesn't lose its tag rows.
        if (_isConjoined && _useArchivedPartitioning)
        {
            builder.Append($"INSERT INTO {tagTable} (value, tenant_id, seq_id, is_archived) SELECT ");
            builder.AppendParameter(_value);
            builder.Append(", ");
            builder.AppendParameter(_tenantId!);
            builder.Append($", e.seq_id, e.is_archived FROM {eventsTable} e WHERE ");
            _whereFragment.Apply(builder);
            builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
            builder.AppendParameter(_value);
            builder.Append(" AND x.tenant_id = ");
            builder.AppendParameter(_tenantId!);
            builder.Append(" AND x.seq_id = e.seq_id AND x.is_archived = e.is_archived);");
        }
        else if (_isConjoined)
        {
            builder.Append($"INSERT INTO {tagTable} (value, tenant_id, seq_id) SELECT ");
            builder.AppendParameter(_value);
            builder.Append(", ");
            builder.AppendParameter(_tenantId!);
            builder.Append($", e.seq_id FROM {eventsTable} e WHERE ");
            _whereFragment.Apply(builder);
            builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
            builder.AppendParameter(_value);
            builder.Append(" AND x.tenant_id = ");
            builder.AppendParameter(_tenantId!);
            builder.Append(" AND x.seq_id = e.seq_id);");
        }
        else if (_useArchivedPartitioning)
        {
            builder.Append($"INSERT INTO {tagTable} (value, seq_id, is_archived) SELECT ");
            builder.AppendParameter(_value);
            builder.Append($", e.seq_id, e.is_archived FROM {eventsTable} e WHERE ");
            _whereFragment.Apply(builder);
            builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
            builder.AppendParameter(_value);
            builder.Append(" AND x.seq_id = e.seq_id AND x.is_archived = e.is_archived);");
        }
        else
        {
            builder.Append($"INSERT INTO {tagTable} (value, seq_id) SELECT ");
            builder.AppendParameter(_value);
            builder.Append($", e.seq_id FROM {eventsTable} e WHERE ");
            _whereFragment.Apply(builder);
            builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
            builder.AppendParameter(_value);
            builder.Append(" AND x.seq_id = e.seq_id);");
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
