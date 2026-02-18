using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;
using Polecat.Internal;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Handles DELETE operations for a flat table when a specific event type is received.
/// </summary>
internal class EventDeleter<TEvent> : IFlatTableEventHandler
{
    private readonly FlatTableProjection _parent;
    private readonly MemberInfo[]? _pkMembers;
    private string? _compiledSql;
    private IParameterSetter? _primaryKeySetter;

    public EventDeleter(FlatTableProjection parent, MemberInfo[]? pkMembers)
    {
        _parent = parent;
        _pkMembers = pkMembers;
    }

    public void Compile(Events.EventGraph events)
    {
        if (_pkMembers != null)
        {
            _primaryKeySetter = BuildSetterForMembers(_pkMembers);
        }
        else
        {
            _primaryKeySetter = events.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
                ? new StreamIdParameterSetter()
                : new StreamKeyParameterSetter();
        }

        var table = _parent.Table;
        var pkColumn = table.PrimaryKeyColumns.FirstOrDefault()
                       ?? throw new InvalidOperationException(
                           $"Table {table.Identifier} must have a primary key column.");

        _compiledSql =
            $"DELETE FROM [{table.Identifier.Schema}].[{table.Identifier.Name}] WHERE [{pkColumn}] = @p0;";
    }

    public FlatTableSqlOperation CreateOperation(IEvent e)
    {
        if (_compiledSql == null || _primaryKeySetter == null)
            throw new InvalidOperationException("EventDeleter has not been compiled.");

        return new FlatTableSqlOperation(_compiledSql, e, [_primaryKeySetter], OperationRole.Delete);
    }

    private static IParameterSetter BuildSetterForMembers(MemberInfo[] members)
    {
        var param = Expression.Parameter(typeof(TEvent), "x");
        Expression body = param;
        foreach (var member in members)
        {
            body = Expression.MakeMemberAccess(body, member);
        }

        var lambda = Expression.Lambda(body, param);
        var compiled = lambda.Compile();

        var valueType = body.Type;
        var setterType = typeof(EventDataParameterSetter<,>).MakeGenericType(typeof(TEvent), valueType);
        return (IParameterSetter)Activator.CreateInstance(setterType, compiled)!;
    }
}
