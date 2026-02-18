using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;

namespace Polecat.Projections.Flattened;

/// <summary>
///     A storage operation that executes a flat table MERGE or DELETE statement.
/// </summary>
internal class FlatTableSqlOperation : IStorageOperation
{
    private readonly string _sql;
    private readonly IEvent _source;
    private readonly IParameterSetter[] _parameterSetters;

    public FlatTableSqlOperation(string sql, IEvent source, IParameterSetter[] parameterSetters,
        OperationRole role)
    {
        _sql = sql;
        _source = source;
        _parameterSetters = parameterSetters;
        Role = role;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role { get; }

    public void ConfigureCommand(SqlCommand command)
    {
        command.CommandText = _sql;
        for (var i = 0; i < _parameterSetters.Length; i++)
        {
            var param = command.CreateParameter();
            param.ParameterName = $"@p{i}";
            _parameterSetters[i].SetValue(param, _source);
            command.Parameters.Add(param);
        }
    }

    public async Task PostprocessAsync(SqlCommand command, CancellationToken token)
    {
        await command.ExecuteNonQueryAsync(token);
    }
}
