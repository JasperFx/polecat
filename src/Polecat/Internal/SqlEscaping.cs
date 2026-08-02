namespace Polecat.Internal;

/// <summary>
///     The single place Polecat escapes a value it is about to interpolate into SQL text, for the
///     two positions where T-SQL requires escaping (polecat#390, companion to weasel#416):
///     inside a bracketed identifier, where an embedded <c>]</c> closes the bracket early and must
///     be doubled, and inside a string literal, where an embedded <c>'</c> must be doubled.
/// </summary>
/// <remarks>
///     <para>
///         Nothing upstream is a sanitizing boundary, so this type cannot be skipped on the grounds
///         that a value "came from Weasel": <c>SqlServerMigrator.AssertValidIdentifier</c> is a
///         no-op, <c>SchemaUtils.QuoteName</c> brackets only reserved keywords (and escapes
///         nothing), and <c>DbObjectName</c> performs no validation at all.
///     </para>
///     <para>
///         <strong>Use one builder per object name.</strong> The failure mode this type exists to
///         prevent is two code paths composing the <em>same</em> object name by different means —
///         they agree right up until quoting is needed, and then diverge into a syntax error or a
///         reference to the wrong object. Notably, a name that appears in both an identifier
///         position (<c>ALTER TABLE [s].[t]</c>) and a string-literal position
///         (<c>OBJECT_ID('[s].[t]')</c>) needs <em>both</em> escapes applied, in that order:
///         <see cref="QualifiedName" /> then <see cref="Literal" />.
///     </para>
///     <para>
///         There is deliberately no "is this already escaped?" shortcut. That guess cannot be made
///         safely from the shape of untrusted input — a value that happens to start and end with a
///         quote would skip escaping entirely, which is strictly worse than the missing-escape case
///         it replaces (see the weasel#416 postmortem).
///     </para>
/// </remarks>
internal static class SqlEscaping
{
    /// <summary>
    ///     Wrap <paramref name="name" /> as a bracketed T-SQL identifier, doubling any embedded
    ///     <c>]</c> so it cannot terminate the bracket early. Equivalent to <c>QUOTENAME()</c>
    ///     evaluated client-side.
    /// </summary>
    public static string QuoteIdentifier(string name)
        => string.Concat("[", name.Replace("]", "]]"), "]");

    /// <summary>
    ///     The schema-qualified, bracket-escaped form of a table or other schema-scoped object:
    ///     <c>[schema].[name]</c>. Every Polecat code path that needs a qualified name in an
    ///     identifier position should compose it here rather than by hand.
    /// </summary>
    public static string QualifiedName(string schema, string name)
        => string.Concat(QuoteIdentifier(schema), ".", QuoteIdentifier(name));

    /// <summary>
    ///     Escape <paramref name="value" /> for embedding inside an <em>existing</em> pair of
    ///     single quotes (doubles <c>'</c>, adds no quotes of its own). Use when the surrounding
    ///     literal is already written into the SQL template.
    /// </summary>
    public static string LiteralBody(string value)
        => value.Replace("'", "''");

    /// <summary>
    ///     <paramref name="value" /> as a complete, quoted T-SQL string literal with embedded
    ///     <c>'</c> doubled — <c>'value'</c>. Prefer a bound parameter where the position allows
    ///     one; this is for the positions that cannot take a parameter, such as the object-name
    ///     argument to <c>OBJECT_ID</c>/<c>COL_LENGTH</c> and nested dynamic-SQL bodies.
    /// </summary>
    public static string Literal(string value)
        => string.Concat("'", LiteralBody(value), "'");
}
