using System.Data.Common;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Projections.Flattened;

/// <summary>
///     A storage operation that executes a flat table MERGE or DELETE statement.
/// </summary>
internal class FlatTableSqlOperation : Polecat.Internal.IStorageOperation, Weasel.Storage.NoDataReturnedCall
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
        _role = role;
    }

    private readonly OperationRole _role;

    public Type DocumentType => typeof(object);
    public OperationRole Role() => _role;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        builder.Append(_sql);
        for (var i = 0; i < _parameterSetters.Length; i++)
        {
            var param = new SqlParameter { ParameterName = $"p{i}" };
            _parameterSetters[i].SetValue(param, _source);
            // Use AddParameters with a dictionary to add the pre-configured parameter value
            builder.AddParameters(new Dictionary<string, object?> { [$"p{i}"] = param.Value });
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) => Task.CompletedTask;
}
