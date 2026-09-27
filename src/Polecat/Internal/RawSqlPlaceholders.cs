namespace Polecat.Internal;

/// <summary>
///     The one definition of Polecat's raw-SQL placeholder rule: the first N occurrences of the
///     placeholder character stand for the N supplied parameter values, in order.
/// </summary>
/// <remarks>
///     <para>
///         #676 extracted this because raw SQL now has two destinations — a standalone
///         <see cref="IAdvancedSql" /> command, which numbers its own <c>@p{i}</c> parameters into one
///         <c>SqlCommand</c>, and a batched query item, which hands each value to the shared
///         <c>ICommandBuilder</c> and lets it name the parameter per batch command. Only the
///         *splitting* is common, so only the splitting is shared; two copies of the placeholder rule
///         would be two places for the two APIs to disagree about the same SQL string.
///     </para>
///     <para>
///         Occurrences beyond the Nth are left alone, which is what makes the
///         <c>char placeholder</c> overloads useful: SQL that contains a literal <c>?</c> picks some
///         other character and its own <c>?</c>s survive untouched.
///     </para>
/// </remarks>
internal static class RawSqlPlaceholders
{
    /// <summary>
    ///     The default placeholder, matching Marten.
    /// </summary>
    public const char Default = '?';

    /// <summary>
    ///     Split <paramref name="sql" /> into <paramref name="parameterCount" /> + 1 literal segments,
    ///     one for each side of the placeholders being replaced. Reassembling
    ///     <c>segments[0] + p0 + segments[1] + p1 + …</c> reproduces the original SQL with the
    ///     placeholders substituted.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Fewer than <paramref name="parameterCount" /> placeholders are present. Reported rather than
    ///     silently binding a shorter parameter list, because the leftover values would otherwise never
    ///     reach the database and the query would quietly run on partial criteria.
    /// </exception>
    public static string[] Split(string sql, char placeholder, int parameterCount)
    {
        // TrimStart is historical IAdvancedSql behaviour and part of the observable contract: the text
        // a logger records for a verbatim string literal starts at the SQL.
        var remaining = sql.TrimStart();
        var segments = new string[parameterCount + 1];

        for (var i = 0; i < parameterCount; i++)
        {
            var index = remaining.IndexOf(placeholder);
            if (index < 0)
            {
                throw new InvalidOperationException(
                    $"Wrong number of supplied parameters. Expected at least {parameterCount} placeholder(s) '{placeholder}' but found {i}.");
            }

            segments[i] = remaining[..index];
            remaining = remaining[(index + 1)..];
        }

        segments[parameterCount] = remaining;
        return segments;
    }
}
