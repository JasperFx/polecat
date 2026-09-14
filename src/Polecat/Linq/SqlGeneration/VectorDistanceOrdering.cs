using JasperFx.Events.Vectors;
using Polecat.Internal;
using Polecat.Storage;
using Weasel.SqlServer;

namespace Polecat.Linq.SqlGeneration;

/// <summary>
///     An ordering key that is a vector distance: <c>VECTOR_DISTANCE('cosine', col, CAST(@p AS
///     VECTOR(n)))</c>.
/// </summary>
/// <remarks>
///     <para>
///         <b>This is the shape <see cref="OrderByClause" /> was widened for.</b> The query vector is a
///         VALUE, so the ordering key cannot be a string appended verbatim the way every other one is —
///         it has to bind a parameter, which only a fragment can do.
///     </para>
///     <para>
///         The vector is bound as its text form and cast server-side, which is what <c>VECTOR(n)</c>
///         accepts; the stored side is the persisted computed column, so the distance is computed over a
///         real vector on both sides rather than re-parsed per row. Same rendering
///         <c>VectorSearchAsync</c>'s raw SQL uses, minus its table alias, because a LINQ statement
///         selects from the table unaliased.
///     </para>
/// </remarks>
internal sealed class VectorDistanceOrdering: ISqlFragment
{
    private readonly string _column;
    private readonly int _dimensions;
    private readonly string _metric;
    private readonly string _vector;

    public VectorDistanceOrdering(VectorIndex index, ReadOnlyMemory<float> query, DistanceFunction? distance)
    {
        _column = SqlEscaping.QuoteIdentifier(index.ColumnName);
        _dimensions = index.Dimensions;
        _metric = VectorIndex.MetricName(distance ?? index.Distance);
        _vector = VectorSearchExtensions.ToVectorLiteral(query.Span);
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append($"VECTOR_DISTANCE('{_metric}', {_column}, CAST(");
        builder.AppendParameter(_vector);
        builder.Append($" AS VECTOR({_dimensions})))");
    }
}
