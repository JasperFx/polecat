using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Handles DELETE operations for a flat table when a specific event type is received.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: builds typed property accessor delegates via Expression.Lambda + Compile against TEvent's primary-key properties — runtime code generation. TEvent flows in from FlatTableProjection.Delete<TEvent>() registration on the caller side and is preserved per the AOT publishing guide.")]
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

        return new FlatTableSqlOperation(_compiledSql, e, [_primaryKeySetter], OperationRole.Deletion);
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
