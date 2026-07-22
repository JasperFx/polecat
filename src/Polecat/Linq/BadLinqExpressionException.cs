namespace Polecat.Linq;

/// <summary>
///     Thrown when a LINQ expression cannot be translated to a valid SQL Server query — for example,
///     attempting to stream a client-side-fallback projection as raw JSON, which would silently
///     return incorrect results. Mirrors Marten's <c>BadLinqExpressionException</c>.
/// </summary>
public class BadLinqExpressionException : Exception
{
    public BadLinqExpressionException(string message) : base(message)
    {
    }

    public BadLinqExpressionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
