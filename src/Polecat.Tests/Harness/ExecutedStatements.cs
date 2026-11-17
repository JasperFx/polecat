using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace Polecat.Tests.Harness;

/// <summary>
///     The statements a piece of test code sent to SQL Server against one table, captured on the client from
///     SqlClient's <see cref="DiagnosticListener" /> (<c>Microsoft.Data.SqlClient.WriteCommandBefore</c>). Start a
///     <see cref="Capture" /> around the code and read <see cref="Capture.Statements" /> afterwards.
/// </summary>
/// <remarks>
///     <para>
///         #1061: this used to read the statements back from <c>sys.dm_exec_query_stats</c>. A SQL Server short
///         of memory evicts plans from that cache, or never caches them, and scanning it timed out, so the
///         tosql_preview tests failed whenever the box was busy. The text SqlClient sends is what the server
///         runs, so reading it on the client proves the same thing without the plan cache.
///     </para>
///     <para>
///         Only commands run by the code inside the capture are recorded: the capture is an
///         <see cref="AsyncLocal{T}" />, and SqlClient raises the event on the thread that executes the command,
///         so another test's store, or a daemon started before the capture, never shows up in it.
///     </para>
///     <para>
///         For a <see cref="SqlBatch" />, SqlClient raises one event for the whole batch, with the text of its
///         last command. The queries these tests run are one command each.
///     </para>
/// </remarks>
public static class ExecutedStatements
{
    private const string CommandBefore = "Microsoft.Data.SqlClient.WriteCommandBefore";

    private static readonly AsyncLocal<Capture?> _current = new();
    private static readonly Lazy<IDisposable> _subscription = new(() =>
        DiagnosticListener.AllListeners.Subscribe(new ListenerObserver()));

    /// <summary>
    ///     Records every statement the code that follows sends to SQL Server, until the capture is disposed.
    ///     Call it synchronously from the test, before the code to observe.
    /// </summary>
    /// <param name="pattern">A LIKE pattern naming the table, e.g. <c>%schema%pc_doc_previewdoc%</c>.</param>
    public static Capture Start(string pattern)
    {
        _ = _subscription.Value;

        var capture = new Capture(pattern);
        _current.Value = capture;
        return capture;
    }

    public sealed class Capture : IDisposable
    {
        private readonly Regex _pattern;
        private readonly List<string> _statements = new();

        internal Capture(string pattern)
        {
            _pattern = new Regex(
                "^" + string.Join(".*", pattern.Split('%').Select(Regex.Escape)) + "$",
                RegexOptions.Singleline | RegexOptions.IgnoreCase);
        }

        /// <summary>The normalized text of every distinct statement sent against the table, in the order sent.</summary>
        public List<string> Statements
        {
            get
            {
                lock (_statements)
                {
                    return _statements.Distinct().ToList();
                }
            }
        }

        internal void Record(string commandText)
        {
            if (!_pattern.IsMatch(commandText)) return;

            lock (_statements)
            {
                _statements.Add(Normalize(commandText));
            }
        }

        public void Dispose()
        {
            if (ReferenceEquals(_current.Value, this)) _current.Value = null;
        }
    }

    private sealed class ListenerObserver : IObserver<DiagnosticListener>
    {
        public void OnNext(DiagnosticListener listener)
        {
            if (listener.Name != "SqlClientDiagnosticListener") return;

            // SqlClient only builds the event while a capture is open on the flow executing the command
            listener.Subscribe(new CommandObserver(), (name, _, _) => name == CommandBefore && _current.Value != null);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }

    private sealed class CommandObserver : IObserver<KeyValuePair<string, object?>>
    {
        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Key != CommandBefore) return;
            if (_current.Value is not { } capture) return;

            var command = value.Value?.GetType().GetProperty("Command")?.GetValue(value.Value) as SqlCommand;
            if (command?.CommandText is { Length: > 0 } text) capture.Record(text);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }

    /// <summary>
    ///     Trims a statement for comparison with a preview: surrounding whitespace, a trailing semicolon, and the
    ///     parameter declarations sp_executesql text starts with, "(@p0 int,@p1 nvarchar(4000))SELECT ...", which a
    ///     preview doesn't have.
    /// </summary>
    public static string Normalize(string sql)
    {
        var text = sql.Trim();
        if (text.StartsWith("(@", StringComparison.Ordinal))
        {
            var depth = 0;
            for (var i = 0; i < text.Length; i++)
            {
                if (text[i] == '(') depth++;
                else if (text[i] == ')') depth--;

                if (depth == 0)
                {
                    text = text[(i + 1)..].Trim();
                    break;
                }
            }
        }

        return text.TrimEnd(';').Trim();
    }
}
