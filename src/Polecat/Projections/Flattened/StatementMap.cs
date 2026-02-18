using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.SqlServer.Tables;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Configures column mappings for a single event type in a FlatTableProjection.
///     At compile time, generates a MERGE SQL statement and parameter setter array.
/// </summary>
public class StatementMap<TEvent> : IFlatTableEventHandler
{
    private readonly FlatTableProjection _parent;
    private readonly List<IColumnMap> _columnMaps = new();
    private readonly List<IParameterSetter> _parameterSetters = new();
    private IParameterSetter? _primaryKeySetter;
    private string? _compiledSql;
    private IParameterSetter[]? _compiledSetters;

    public StatementMap(FlatTableProjection parent, MemberInfo[]? pkMembers)
    {
        _parent = parent;

        if (pkMembers != null)
        {
            _primaryKeySetter = BuildSetterForMembers<TEvent>(pkMembers);
        }
    }

    /// <summary>
    ///     Map an event property to a table column (direct value assignment).
    /// </summary>
    public Table.ColumnExpression Map<TValue>(Expression<Func<TEvent, TValue>> members,
        string? columnName = null)
    {
        var memberInfo = GetMemberInfo(members);
        var name = columnName ?? ToSnakeCase(memberInfo.Name);
        _columnMaps.Add(new MemberMap(name));
        _parameterSetters.Add(BuildSetter(members));
        return ResolveColumn(name, typeof(TValue));
    }

    /// <summary>
    ///     Increment a column by the event property value.
    /// </summary>
    public Table.ColumnExpression Increment<TValue>(Expression<Func<TEvent, TValue>> members,
        string? columnName = null)
    {
        var memberInfo = GetMemberInfo(members);
        var name = columnName ?? ToSnakeCase(memberInfo.Name);
        _columnMaps.Add(new IncrementMemberMap(name));
        _parameterSetters.Add(BuildSetter(members));
        return ResolveColumn(name, typeof(TValue));
    }

    /// <summary>
    ///     Increment a column by 1 (no event parameter needed).
    /// </summary>
    public Table.ColumnExpression Increment(string columnName)
    {
        _columnMaps.Add(new IncrementMap(columnName));
        return ResolveColumn(columnName, typeof(int));
    }

    /// <summary>
    ///     Decrement a column by the event property value.
    /// </summary>
    public Table.ColumnExpression Decrement<TValue>(Expression<Func<TEvent, TValue>> members,
        string? columnName = null)
    {
        var memberInfo = GetMemberInfo(members);
        var name = columnName ?? ToSnakeCase(memberInfo.Name);
        _columnMaps.Add(new DecrementMemberMap(name));
        _parameterSetters.Add(BuildSetter(members));
        return ResolveColumn(name, typeof(TValue));
    }

    /// <summary>
    ///     Decrement a column by 1 (no event parameter needed).
    /// </summary>
    public Table.ColumnExpression Decrement(string columnName)
    {
        _columnMaps.Add(new DecrementMap(columnName));
        return ResolveColumn(columnName, typeof(int));
    }

    /// <summary>
    ///     Set a column to a literal string value.
    /// </summary>
    public Table.ColumnExpression SetValue(string columnName, string value)
    {
        _columnMaps.Add(new SetStringValueMap(columnName, value));
        return ResolveColumn(columnName, typeof(string));
    }

    /// <summary>
    ///     Set a column to a literal integer value.
    /// </summary>
    public Table.ColumnExpression SetValue(string columnName, int value)
    {
        _columnMaps.Add(new SetIntValueMap(columnName, value));
        return ResolveColumn(columnName, typeof(int));
    }

    void IFlatTableEventHandler.Compile(Events.EventGraph events)
    {
        Compile(events);
    }

    FlatTableSqlOperation IFlatTableEventHandler.CreateOperation(IEvent e)
    {
        return CreateOperation(e);
    }

    internal void Compile(Events.EventGraph events)
    {
        // If no explicit PK setter, default to stream ID
        _primaryKeySetter ??= events.StreamIdentity == JasperFx.Events.StreamIdentity.AsGuid
            ? new StreamIdParameterSetter()
            : new StreamKeyParameterSetter();

        var table = _parent.Table;
        var pkColumn = table.PrimaryKeyColumns.FirstOrDefault()
                       ?? throw new InvalidOperationException(
                           $"Table {table.Identifier} must have a primary key column.");

        // Build parameter list: [0] = PK, [1..N] = column map values (only those requiring input)
        var allSetters = new List<IParameterSetter> { _primaryKeySetter };
        var paramIndex = 1;

        // Build MERGE SQL
        var updateClauses = new List<string>();
        var insertColumns = new List<string> { $"[{pkColumn}]" };
        var insertValues = new List<string> { "@p0" };

        for (var i = 0; i < _columnMaps.Count; i++)
        {
            var map = _columnMaps[i];
            string paramName;

            if (map.RequiresInput)
            {
                paramName = $"@p{paramIndex}";
                allSetters.Add(_parameterSetters[GetInputIndex(i)]);
                paramIndex++;
            }
            else
            {
                paramName = ""; // Not used for non-input maps
            }

            updateClauses.Add(map.UpdateExpression(paramName));
            insertColumns.Add($"[{map.ColumnName}]");
            insertValues.Add(map.InsertExpression(paramName));
        }

        var qualifiedTable = table.Identifier.QualifiedName;
        var updateSet = string.Join(", ", updateClauses);
        var insertCols = string.Join(", ", insertColumns);
        var insertVals = string.Join(", ", insertValues);

        _compiledSql = $"""
            MERGE [{table.Identifier.Schema}].[{table.Identifier.Name}] AS target
            USING (SELECT @p0 AS [{pkColumn}]) AS source ON target.[{pkColumn}] = source.[{pkColumn}]
            WHEN MATCHED THEN UPDATE SET {updateSet}
            WHEN NOT MATCHED THEN INSERT ({insertCols}) VALUES ({insertVals});
            """;

        _compiledSetters = allSetters.ToArray();
    }

    internal FlatTableSqlOperation CreateOperation(IEvent e)
    {
        if (_compiledSql == null || _compiledSetters == null)
            throw new InvalidOperationException("StatementMap has not been compiled.");

        return new FlatTableSqlOperation(_compiledSql, e, _compiledSetters, OperationRole.Upsert);
    }

    private int GetInputIndex(int columnMapIndex)
    {
        // Maps with RequiresInput have a corresponding entry in _parameterSetters
        // Non-input maps don't, so we need to count only the input entries up to this index
        var inputIndex = 0;
        for (var i = 0; i < columnMapIndex; i++)
        {
            if (_columnMaps[i].RequiresInput) inputIndex++;
        }

        return inputIndex;
    }

    private Table.ColumnExpression ResolveColumn(string columnName, Type dotnetType)
    {
        var table = _parent.Table;
        // Check if column already exists on the table — don't add duplicates
        var existing = table.Columns.FirstOrDefault(c => c.Name == columnName);
        if (existing != null)
        {
            return new Table.ColumnExpression(table, existing);
        }

        // Add the column with an inferred SQL type
        return table.AddColumn(columnName, MapToSqlType(dotnetType));
    }

    private static IParameterSetter BuildSetter<TValue>(Expression<Func<TEvent, TValue>> expression)
    {
        var compiled = expression.Compile();
        return new EventDataParameterSetter<TEvent, TValue>(compiled);
    }

    private static IParameterSetter BuildSetterForMembers<TSource>(MemberInfo[] members)
    {
        // Build a lambda: source => source.Member1.Member2...
        var param = Expression.Parameter(typeof(TSource), "x");
        Expression body = param;
        foreach (var member in members)
        {
            body = Expression.MakeMemberAccess(body, member);
        }

        var lambda = Expression.Lambda(body, param);
        var compiled = lambda.Compile();

        // Create an EventDataParameterSetter with the appropriate types
        var valueType = body.Type;
        var setterType = typeof(EventDataParameterSetter<,>).MakeGenericType(typeof(TSource), valueType);
        return (IParameterSetter)Activator.CreateInstance(setterType, compiled)!;
    }

    private static MemberInfo GetMemberInfo<TValue>(Expression<Func<TEvent, TValue>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression unary) body = unary.Operand;
        if (body is MemberExpression member) return member.Member;
        throw new ArgumentException("Expression must be a member access expression.");
    }

    private static string ToSnakeCase(string name)
    {
        return string.Concat(name.Select((c, i) =>
            i > 0 && char.IsUpper(c) ? "_" + char.ToLower(c) : char.ToLower(c).ToString()));
    }

    private static string MapToSqlType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying switch
        {
            _ when underlying == typeof(Guid) => "uniqueidentifier",
            _ when underlying == typeof(int) => "int",
            _ when underlying == typeof(long) => "bigint",
            _ when underlying == typeof(short) => "smallint",
            _ when underlying == typeof(decimal) => "decimal(18,4)",
            _ when underlying == typeof(double) => "float",
            _ when underlying == typeof(float) => "real",
            _ when underlying == typeof(bool) => "bit",
            _ when underlying == typeof(string) => "nvarchar(max)",
            _ when underlying == typeof(DateTime) => "datetime2",
            _ when underlying == typeof(DateTimeOffset) => "datetimeoffset",
            _ when underlying == typeof(byte[]) => "varbinary(max)",
            _ => "nvarchar(max)"
        };
    }
}
