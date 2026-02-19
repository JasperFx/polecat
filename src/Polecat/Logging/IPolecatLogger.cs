using Microsoft.Data.SqlClient;

namespace Polecat.Logging;

/// <summary>
///     Store-level logger factory. Creates session loggers for each new session.
/// </summary>
public interface IPolecatLogger
{
    IPolecatSessionLogger StartSession(IQuerySession session);
}

/// <summary>
///     Per-session logger that receives SQL command lifecycle events.
/// </summary>
public interface IPolecatSessionLogger
{
    void OnBeforeExecute(SqlCommand command);
    void LogSuccess(SqlCommand command);
    void LogFailure(SqlCommand command, Exception ex);
    void RecordSavedChanges(IDocumentSession session);
}

/// <summary>
///     Default no-op logger implementations.
/// </summary>
public class NullPolecatLogger : IPolecatLogger, IPolecatSessionLogger
{
    public static readonly NullPolecatLogger Instance = new();

    public IPolecatSessionLogger StartSession(IQuerySession session) => this;
    public void OnBeforeExecute(SqlCommand command) { }
    public void LogSuccess(SqlCommand command) { }
    public void LogFailure(SqlCommand command, Exception ex) { }
    public void RecordSavedChanges(IDocumentSession session) { }
}
