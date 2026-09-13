using System.Reflection;
using JasperFx.Events.Vectors;
using Polecat.Internal;

namespace Polecat.Storage;

/// <summary>
///     Declares a member of a document as an embedding, stored as a persisted computed
///     <c>VECTOR(n)</c> column over the JSON, so <c>VECTOR_DISTANCE</c> can order by similarity.
/// </summary>
/// <remarks>
///     <para>
///         <b>A computed column, not a written one, and that is the whole design.</b> The embedding
///         lives in <c>data</c> like every other member, and the column is derived from it — so the
///         write path is untouched, the column cannot drift from the document, and declaring a
///         vector on a type that already has rows makes every one of them searchable at once with no
///         backfill. It is the same trade a duplicated field already makes here, one type over.
///     </para>
///     <para>
///         <b>JSON_QUERY, not JSON_VALUE.</b> Every other computed column in Polecat extracts a
///         scalar through <c>JSON_VALUE</c>, which truncates at 4000 characters — a 768-float array
///         is several times that, so the scalar form cannot carry an embedding at all. Only
///         <c>JSON_QUERY</c> returns the array intact. That is why this is its own type rather than
///         a <see cref="DocumentIndex" /> with a different SQL type.
///     </para>
///     <para>
///         <b>No index is created, deliberately.</b> SQL Server will not build a NONCLUSTERED index
///         over a vector column, and the vector index this build offers is the legacy DiskANN form,
///         which needs PREVIEW_FEATURES, a hundred rows, and makes the table READ ONLY (Msg 42231 on
///         the next insert). A read-only document table is not a trade worth making, so this ships
///         exact k-nearest-neighbour over a sequential scan and leaves the approximate index for
///         when the engine supports one that permits writes. The search is the seam it would slot
///         behind.
///     </para>
/// </remarks>
public class VectorIndex
{
    internal VectorIndex(string jsonPath, MemberInfo[]? memberChain, int dimensions, DistanceFunction distance)
    {
        JsonPath = jsonPath;
        MemberChain = memberChain;
        Dimensions = dimensions;
        Distance = distance;
    }

    /// <summary>
    ///     The JSON path the embedding is read from, re-rendered under the store's serializer naming
    ///     policy by <see cref="ApplyNamingPolicy" />.
    /// </summary>
    public string JsonPath { get; private set; }

    /// <summary>
    ///     The member chain the path came from, kept so the path can be re-rendered once the store's
    ///     policy is in reach. Null for a hand-written path, which is left verbatim.
    /// </summary>
    internal MemberInfo[]? MemberChain { get; }

    /// <summary>How many floats the embedding carries. Pinned so a query vector can be checked against it.</summary>
    public int Dimensions { get; }

    /// <summary>The metric a search uses unless it names another.</summary>
    public DistanceFunction Distance { get; }

    /// <summary>The persisted computed column this declaration creates.</summary>
    public string ColumnName => "vec_" + JsonPath.Replace("$.", "").Replace(".", "_").ToLowerInvariant();

    /// <summary>The member as the caller wrote it, for error messages.</summary>
    internal string MemberName => MemberChain is { Length: > 0 }
        ? string.Join(".", MemberChain.Select(x => x.Name))
        : JsonPath;

    /// <summary>
    ///     Re-render the path under the store's serializer naming policy — the same #510 pass
    ///     <see cref="DocumentIndex.ApplyNamingPolicy" /> makes, and for the same reason. A vector
    ///     column built from the CLR member name while the serializer wrote camelCase reads SQL NULL
    ///     for every row, which fails as an empty result rather than as an error.
    /// </summary>
    internal void ApplyNamingPolicy(StoreOptions options)
    {
        if (MemberChain is { Length: > 0 })
        {
            JsonPath = SerializedNames.PathFor(MemberChain, options);
        }
    }

    /// <summary>
    ///     The DDL that adds the persisted computed column. One statement, and no index — see the
    ///     remarks on the type.
    /// </summary>
    internal string[] ToDdlStatements(DocumentMapping mapping)
    {
        var qualifiedTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, mapping.TableName);

        // QUOTED_IDENTIFIER has to be ON for a PERSISTED computed column, and SqlClient sets it ON
        // per connection by default — stated rather than relied on silently, because a session that
        // had turned it off would fail here with a message about the setting rather than the column.
        return
        [
            $"""
             IF COL_LENGTH({SqlEscaping.Literal(qualifiedTable)}, {SqlEscaping.Literal(ColumnName)}) IS NULL
                 ALTER TABLE {qualifiedTable} ADD {SqlEscaping.QuoteIdentifier(ColumnName)} AS {ColumnExpression()} PERSISTED;
             """
        ];
    }

    /// <summary>
    ///     <c>CAST(JSON_QUERY(data, '$.path') AS VECTOR(n))</c> — see the remarks on the type for why
    ///     this cannot be the <c>JSON_VALUE</c> form every other computed column here uses.
    /// </summary>
    internal string ColumnExpression() => $"CAST(JSON_QUERY(data, '{JsonPath}') AS VECTOR({Dimensions}))";

    /// <summary>
    ///     The metric name <c>VECTOR_DISTANCE</c> takes. Every one of the three is already a
    ///     DISTANCE on SQL Server — smaller is closer — including <c>dot</c>, which the engine
    ///     returns negated, so the promise <see cref="DistanceFunction" /> makes on every store holds
    ///     here with nothing to correct. Verified against 17.0.4045.
    /// </summary>
    internal static string MetricName(DistanceFunction distance) => distance switch
    {
        DistanceFunction.Cosine => "cosine",
        DistanceFunction.L2 => "euclidean",
        DistanceFunction.InnerProduct => "dot",
        _ => throw new ArgumentOutOfRangeException(nameof(distance), distance, null)
    };
}
