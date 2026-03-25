using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Internal.Operations;

internal class ExecuteSqlStorageOperation : IStorageOperation
{
    private readonly string _commandText;
    private readonly object[] _parameterValues;

    public ExecuteSqlStorageOperation(string commandText, params object[] parameterValues)
    {
        _commandText = commandText.TrimEnd(';');
        _parameterValues = parameterValues;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var parameters = builder.AppendWithParameters(_commandText);
        if (parameters.Length != _parameterValues.Length)
        {
            throw new InvalidOperationException(
                $"Wrong number of parameter values to SQL '{_commandText}', got {_parameterValues.Length} parameters");
        }

        for (var i = 0; i < parameters.Length; i++)
        {
            if (_parameterValues[i] == null)
            {
                parameters[i].Value = DBNull.Value;
            }
            else
            {
                parameters[i].Value = _parameterValues[i];
            }
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token) =>
        Task.CompletedTask;
}
