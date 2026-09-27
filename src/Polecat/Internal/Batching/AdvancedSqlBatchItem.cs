using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Polecat.Serialization;
using Weasel.SqlServer;

namespace Polecat.Internal.Batching;

/// <summary>
///     #676 — a raw-SQL query enlisted in an <c>IBatchedQuery</c>, so it shares the batch's single
///     round trip with the document loads, LINQ queries and event fetches around it.
/// </summary>
/// <remarks>
///     <para>
///         The reading half is the same <see cref="AdvancedSqlResultReader" /> that backs
///         <c>IAdvancedSql.QueryAsync&lt;T&gt;</c>, so the scalar / document / JSON rules are shared
///         rather than reimplemented, and the placeholder rule comes from
///         <see cref="RawSqlPlaceholders" /> for the same reason.
///     </para>
///     <para>
///         Parameters are handed to the shared <see cref="ICommandBuilder" /> one at a time instead of
///         being named <c>@p{i}</c> up front the way the standalone path does. The builder names them
///         per batch command, which is what keeps two raw-SQL items in one batch from colliding on
///         <c>@p0</c>.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: materializes raw SQL results through AdvancedSqlResultReader, which deserializes via ISerializer.FromJson. T is preserved by the IBatchedQuery.Query<T>(sql) registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class AdvancedSqlBatchItem<T> : IBatchQueryItem
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _tcs = new();
    private readonly string[] _segments;
    private readonly object[] _parameters;
    private readonly AdvancedSqlResultReader _reader;

    public AdvancedSqlBatchItem(string sql, char placeholder, object[] parameters,
        ISerializer serializer, DocumentProviderRegistry providers)
    {
        _parameters = parameters;

        // Split eagerly, in the caller's frame: a wrong placeholder count is a programming error and
        // should be raised by the Query<T>(sql) call that made it, not by Execute() one batch later
        // where it would be indistinguishable from a failure of somebody else's item.
        _segments = RawSqlPlaceholders.Split(sql, placeholder, parameters.Length);
        _reader = AdvancedSqlResultReader.ForType(typeof(T), serializer, providers);
    }

    public Task<IReadOnlyList<T>> Result => _tcs.Task;

    public void WriteSql(ICommandBuilder builder)
    {
        builder.Append(_segments[0]);
        for (var i = 0; i < _parameters.Length; i++)
        {
            builder.AppendParameter(_parameters[i] ?? DBNull.Value);
            builder.Append(_segments[i + 1]);
        }

        // The batch needs each item's statement terminated. SQL the caller already terminated is left
        // alone rather than given a second ';'.
        if (!_segments[^1].TrimEnd().EndsWith(';'))
        {
            builder.Append(';');
        }

        builder.Append('\n');
    }

    public async Task ReadResultSetAsync(DbDataReader reader, CancellationToken token)
    {
        var results = new List<T>();
        while (await reader.ReadAsync(token))
        {
            results.Add(AdvancedSqlResultReader.Cast<T>(_reader.ReadValue(reader, 0)));
        }

        _tcs.SetResult(results);
    }
}
