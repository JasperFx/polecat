using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     One <c>ORDER BY</c> term: an <see cref="ISqlFragment" /> and a direction.
/// </summary>
/// <remarks>
///     <para>
///         <b>A fragment rather than a string, so an ordering key can BIND A PARAMETER.</b> A
///         statement's <c>Wheres</c> have always been composable fragments; its orderings were bare
///         text appended verbatim, which is fine for <c>JSON_VALUE(data, '$.name')</c> and impossible
///         for a vector distance — <c>VECTOR_DISTANCE('cosine', col, @query)</c> has the query vector
///         inside it. That asymmetry is why <c>VectorSearchAsync</c> was written against
///         <c>IAdvancedSql</c> instead of the LINQ path, and this is what retires it.
///     </para>
///     <para>
///         The implicit conversion from <c>(string, bool)</c> is what keeps that retirement from being
///         a rewrite: every existing ordering site still reads <c>OrderBys.Add((locator, descending))</c>
///         and compiles unchanged.
///     </para>
///     <para>
///         ⚠️ <see cref="Literal" /> is null for a parameterised ordering, and callers that render an
///         ordering into a STRING rather than through a command builder must refuse one — there is
///         nowhere for the parameter to go. <see cref="Statement" />'s window-function ordering is the
///         one such caller, and it says so.
///     </para>
/// </remarks>
internal sealed record OrderByClause(ISqlFragment Fragment, bool Descending, string? Literal)
{
    /// <summary>The plain-text ordering key, or null when this term binds a parameter.</summary>
    public string? Literal { get; init; } = Literal;

    /// <summary>An ordering over literal SQL — the shape every non-vector ordering uses.</summary>
    public static implicit operator OrderByClause((string Locator, bool Descending) ordering)
        => new(new LiteralSqlFragment(ordering.Locator), ordering.Descending, ordering.Locator);

    /// <summary>
    ///     The same term in the opposite direction, which is how <c>Last()</c> is answered — reverse the
    ///     ordering and take one.
    /// </summary>
    public OrderByClause Reversed() => this with { Descending = !Descending };

    public void Apply(ICommandBuilder builder)
    {
        Fragment.Apply(builder);
        if (Descending) builder.Append(" DESC");
    }
}
