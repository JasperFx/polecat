namespace Polecat.Projections.Flattened;

/// <summary>
///     Describes how a single column participates in a flat table MERGE statement.
/// </summary>
internal interface IColumnMap
{
    string ColumnName { get; }

    /// <summary>
    ///     True if this column requires a parameter value from the event.
    ///     False for Increment(columnName), Decrement(columnName), and SetValue.
    /// </summary>
    bool RequiresInput { get; }

    /// <summary>
    ///     The SET expression for the WHEN MATCHED UPDATE clause.
    ///     e.g., "[col] = @p1" or "[col] = target.[col] + 1"
    /// </summary>
    string UpdateExpression(string paramName);

    /// <summary>
    ///     The value expression for the WHEN NOT MATCHED INSERT VALUES clause.
    ///     e.g., "@p1" or "0" or "'value'"
    /// </summary>
    string InsertExpression(string paramName);
}

/// <summary>
///     Maps an event property to a column value (direct assignment).
/// </summary>
internal class MemberMap : IColumnMap
{
    public MemberMap(string columnName)
    {
        ColumnName = columnName;
    }

    public string ColumnName { get; }
    public bool RequiresInput => true;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = {paramName}";
    public string InsertExpression(string paramName) => paramName;
}

/// <summary>
///     Maps an event property to a column that is incremented by the value.
/// </summary>
internal class IncrementMemberMap : IColumnMap
{
    public IncrementMemberMap(string columnName)
    {
        ColumnName = columnName;
    }

    public string ColumnName { get; }
    public bool RequiresInput => true;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = target.[{ColumnName}] + {paramName}";
    public string InsertExpression(string paramName) => paramName;
}

/// <summary>
///     Maps an event property to a column that is decremented by the value.
/// </summary>
internal class DecrementMemberMap : IColumnMap
{
    public DecrementMemberMap(string columnName)
    {
        ColumnName = columnName;
    }

    public string ColumnName { get; }
    public bool RequiresInput => true;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = target.[{ColumnName}] - {paramName}";
    public string InsertExpression(string paramName) => paramName;
}

/// <summary>
///     Increments a column by 1 (no event parameter needed).
/// </summary>
internal class IncrementMap : IColumnMap
{
    public IncrementMap(string columnName)
    {
        ColumnName = columnName;
    }

    public string ColumnName { get; }
    public bool RequiresInput => false;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = target.[{ColumnName}] + 1";
    public string InsertExpression(string paramName) => "1";
}

/// <summary>
///     Decrements a column by 1 (no event parameter needed).
/// </summary>
internal class DecrementMap : IColumnMap
{
    public DecrementMap(string columnName)
    {
        ColumnName = columnName;
    }

    public string ColumnName { get; }
    public bool RequiresInput => false;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = target.[{ColumnName}] - 1";
    public string InsertExpression(string paramName) => "0";
}

/// <summary>
///     Sets a column to a literal string value.
/// </summary>
internal class SetStringValueMap : IColumnMap
{
    private readonly string _value;

    public SetStringValueMap(string columnName, string value)
    {
        ColumnName = columnName;
        _value = value;
    }

    public string ColumnName { get; }
    public bool RequiresInput => false;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = '{_value}'";
    public string InsertExpression(string paramName) => $"'{_value}'";
}

/// <summary>
///     Sets a column to a literal integer value.
/// </summary>
internal class SetIntValueMap : IColumnMap
{
    private readonly int _value;

    public SetIntValueMap(string columnName, int value)
    {
        ColumnName = columnName;
        _value = value;
    }

    public string ColumnName { get; }
    public bool RequiresInput => false;

    public string UpdateExpression(string paramName) => $"[{ColumnName}] = {_value}";
    public string InsertExpression(string paramName) => _value.ToString();
}
