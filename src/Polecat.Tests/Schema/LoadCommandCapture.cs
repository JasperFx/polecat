using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace Polecat.Tests.Schema;

/// <summary>
///     #935: the SqlCommand a piece of Polecat really sends, read off SqlClient's own diagnostic events, so
///     a plan probe can run that exact statement with its exact parameters. Writing the statement out in the
///     test would measure the test.
/// </summary>
internal static class LoadCommandCapture
{
    private const string WriteCommandBefore = "Microsoft.Data.SqlClient.WriteCommandBefore";

    /// <summary>
    ///     A copy of the first command sent while <paramref name="act" /> runs whose text names
    ///     <paramref name="table" /> and selects by id.
    /// </summary>
    public static async Task<SqlCommand> CaptureAsync(string table, Func<Task> act)
    {
        SqlCommand? captured = null;

        var commands = new Observer<KeyValuePair<string, object?>>(pair =>
        {
            if (pair.Key != WriteCommandBefore || captured != null) return;
            if (pair.Value?.GetType().GetProperty("Command")?.GetValue(pair.Value) is not SqlCommand command) return;
            if (!command.CommandText.Contains(table, StringComparison.Ordinal)
                || !command.CommandText.Contains("WHERE id = @id", StringComparison.Ordinal)) return;

            var copy = new SqlCommand(command.CommandText);
            foreach (SqlParameter parameter in command.Parameters)
            {
                copy.Parameters.Add((SqlParameter)((ICloneable)parameter).Clone());
            }

            captured = copy;
        });

        var subscriptions = new List<IDisposable>();
        using var listeners = DiagnosticListener.AllListeners.Subscribe(new Observer<DiagnosticListener>(listener =>
        {
            if (listener.Name == "SqlClientDiagnosticListener")
            {
                lock (subscriptions) subscriptions.Add(listener.Subscribe(commands));
            }
        }));

        try
        {
            await act();
        }
        finally
        {
            lock (subscriptions)
            {
                foreach (var subscription in subscriptions) subscription.Dispose();
            }
        }

        return captured ?? throw new InvalidOperationException($"No command selecting from {table} by id was sent.");
    }

    /// <summary>The actual execution plan of <paramref name="command" />, run on a connection of its own.</summary>
    public static async Task<string> ActualPlanAsync(string connectionString, SqlCommand command, CancellationToken token)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(token);

        await using (var on = conn.CreateCommand())
        {
            on.CommandText = "SET STATISTICS XML ON;";
            await on.ExecuteNonQueryAsync(token);
        }

        command.Connection = conn;
        await using var reader = await command.ExecuteReaderAsync(token);

        string? plan = null;
        do
        {
            while (await reader.ReadAsync(token))
            {
                if (reader.FieldCount == 1 && reader.GetValue(0) is string text
                                           && text.Contains("ShowPlanXML", StringComparison.Ordinal))
                {
                    plan = text;
                }
            }
        } while (await reader.NextResultAsync(token));

        return plan ?? throw new InvalidOperationException("SQL Server returned no execution plan.");
    }

    private sealed class Observer<T>(Action<T> next) : IObserver<T>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(T value) => next(value);
    }
}
