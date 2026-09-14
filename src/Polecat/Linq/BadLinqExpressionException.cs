namespace Polecat.Linq;

/// <summary>
///     Thrown when a LINQ expression cannot be translated to a valid SQL Server query — for example,
///     attempting to stream a client-side-fallback projection as raw JSON, which would silently
///     return incorrect results.
/// </summary>
/// <remarks>
///     <para>
///         <b>Derives from <see cref="JasperFx.BadLinqExpressionException" /> as of JasperFx 2.71.0
///         (jasperfx#795), which lifted the per-store copies.</b> Kept as a Polecat type rather than
///         replaced by the shared one, so application code already catching
///         <c>Polecat.Linq.BadLinqExpressionException</c> keeps compiling and keeps catching; code that
///         wants to be store-agnostic catches the base instead and works against Fisher too. The
///         derived type adds nothing — it exists for the name.
///     </para>
///     <para>
///         ⚠️ <b>The refusal is a correctness guarantee rather than a convenience.</b> A caller who
///         catches this has been told "I cannot answer this correctly", which is categorically
///         different from an empty result — the alternative here is a query that returns plausible but
///         wrong rows.
///     </para>
///     <para>
///         Marten deliberately does NOT join this hierarchy yet: marten#5346 ruled that
///         <c>all_exceptions_should_derive_from_MartenException</c> keeps it on its own until Marten 10,
///         and C# has single inheritance. So this is a two-store convergence for now.
///     </para>
/// </remarks>
public class BadLinqExpressionException : JasperFx.BadLinqExpressionException
{
    public BadLinqExpressionException(string message) : base(message)
    {
    }

    public BadLinqExpressionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
