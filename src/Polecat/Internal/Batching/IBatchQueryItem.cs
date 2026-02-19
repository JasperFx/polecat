using Microsoft.Data.SqlClient;
using Polecat.Linq.SqlGeneration;

namespace Polecat.Internal.Batching;

/// <summary>
///     A single item in a batched query. Writes its SQL into a shared CommandBuilder
///     and reads its result set when Execute() processes the reader.
/// </summary>
internal interface IBatchQueryItem
{
    void WriteSql(CommandBuilder builder);
    Task ReadResultSetAsync(SqlDataReader reader, CancellationToken token);
}
