using System.Data.Common;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Linq.SqlGeneration;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Operations;

/// <summary>
/// Retroactively assigns a tag to all events matching a WHERE clause.
/// Generates:
///   INSERT INTO [schema].[pc_event_tag_{suffix}] (value, seq_id)
///   SELECT @value, e.seq_id FROM [schema].[pc_events] e
///   WHERE {where}
///   AND NOT EXISTS (SELECT 1 FROM [schema].[pc_event_tag_{suffix}] x WHERE x.value = @value AND x.seq_id = e.seq_id)
/// </summary>
internal class AssignTagWhereOperation : Internal.IStorageOperation
{
    private readonly string _schemaName;
    private readonly ITagTypeRegistration _registration;
    private readonly object _value;
    private readonly ISqlFragment _whereFragment;

    public AssignTagWhereOperation(string schemaName, ITagTypeRegistration registration, object value,
        ISqlFragment whereFragment)
    {
        _schemaName = schemaName;
        _registration = registration;
        _value = value;
        _whereFragment = whereFragment;
    }

    public Type DocumentType => typeof(IEvent);
    public OperationRole Role() => OperationRole.Events;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var tagTable = $"[{_schemaName}].[pc_event_tag_{_registration.TableSuffix}]";
        var eventsTable = $"[{_schemaName}].[pc_events]";

        builder.Append($"INSERT INTO {tagTable} (value, seq_id) SELECT ");
        builder.AppendParameter(_value);
        builder.Append($", e.seq_id FROM {eventsTable} e WHERE ");
        _whereFragment.Apply(builder);
        builder.Append($" AND NOT EXISTS (SELECT 1 FROM {tagTable} x WHERE x.value = ");
        builder.AppendParameter(_value);
        builder.Append(" AND x.seq_id = e.seq_id);");
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
