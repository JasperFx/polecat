using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Polecat.Serialization;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads JSON_OBJECT results from a GroupBy query and deserializes each row.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: deserializes JSON_OBJECT rows via ISerializer.FromJson. Result types flow in from GroupBy<T>() registration on the caller side and are preserved per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.FromJson is annotated RDC. AOT consumers supply a source-generator-backed impl.")]
internal class GroupByListHandler<T>
{
    private readonly ISerializer _serializer;

    public GroupByListHandler(ISerializer serializer)
    {
        _serializer = serializer;
    }

    public async Task<IReadOnlyList<T>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var results = new List<T>();

        while (await reader.ReadAsync(token))
        {
            var json = reader.GetString(0);
            var item = _serializer.FromJson<T>(json);
            results.Add(item);
        }

        return results;
    }
}
