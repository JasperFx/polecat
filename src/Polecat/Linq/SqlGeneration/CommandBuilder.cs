using System.Text;
using Microsoft.Data.SqlClient;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     Accumulates SQL text and parameters for building a SqlCommand.
/// </summary>
internal class CommandBuilder
{
    private readonly StringBuilder _sql = new();
    private readonly List<(string Name, object Value)> _parameters = [];
    private int _nextParameterIndex;

    public string AddParameter(object? value)
    {
        var name = $"@p{_nextParameterIndex++}";
        _parameters.Add((name, value ?? DBNull.Value));
        return name;
    }

    public void Append(string text) => _sql.Append(text);

    public void AppendParameter(object? value)
    {
        var name = AddParameter(value);
        _sql.Append(name);
    }

    public void ApplyTo(SqlCommand cmd)
    {
        cmd.CommandText = _sql.ToString();
        foreach (var (name, value) in _parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }
    }

    public override string ToString() => _sql.ToString();
}
