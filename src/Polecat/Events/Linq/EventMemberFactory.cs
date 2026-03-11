using System.Linq.Expressions;
using System.Reflection;
using Polecat.Linq.Members;

namespace Polecat.Events.Linq;

/// <summary>
///     Resolves IEvent property expressions to SQL column references on the pc_events table.
/// </summary>
internal class EventMemberFactory : IMemberResolver
{
    private static readonly Dictionary<string, (string column, Type clrType)> EventColumns = new()
    {
        { "Id", ("id", typeof(Guid)) },
        { "Sequence", ("seq_id", typeof(long)) },
        { "StreamId", ("stream_id", typeof(Guid)) },
        { "StreamKey", ("stream_id", typeof(string)) },
        { "Version", ("version", typeof(long)) },
        { "Timestamp", ("timestamp", typeof(DateTimeOffset)) },
        { "EventTypeName", ("type", typeof(string)) },
        { "DotNetTypeName", ("dotnet_type", typeof(string)) },
        { "IsArchived", ("is_archived", typeof(bool)) },
        { "CorrelationId", ("correlation_id", typeof(string)) },
        { "CausationId", ("causation_id", typeof(string)) },
        { "TenantId", ("tenant_id", typeof(string)) }
    };

    public IQueryableMember ResolveMember(MemberExpression expression)
    {
        var propName = expression.Member.Name;

        if (!EventColumns.TryGetValue(propName, out var mapping))
        {
            throw new NotSupportedException(
                $"IEvent property '{propName}' is not supported in event LINQ queries.");
        }

        var (column, clrType) = mapping;
        var isBoolean = clrType == typeof(bool);
        return new QueryableMember(column, column, clrType, isBoolean);
    }
}
