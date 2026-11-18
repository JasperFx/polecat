using Microsoft.Data.SqlClient;
using Polecat.Services;
using Polecat.TestUtils;

namespace Polecat.Tests.Compliance;

/// <summary>
///     #858: returns from a commit only after <c>SYSDATETIMEOFFSET()</c> has visibly moved on, so the next
///     write in a compliance fact can't share a timestamp with this one.
/// </summary>
/// <remarks>
///     The ordering facts assert on rows written "in turn" and space them with a fixed delay, but the
///     server's clock ticks coarsely on a Linux container, and on a loaded runner two writes 25ms apart
///     have come out with equal timestamps. The tie then falls to a random Guid. This makes what those
///     facts assume true, rather than touching what they assert. It lives on the fixtures because the
///     delays are private to the shared suites, and any failure here is ignored so a fact is never
///     failed by the wait.
/// </remarks>
internal sealed class ServerClockTickListener : IDocumentSessionListener
{
    public static readonly ServerClockTickListener Instance = new();

    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(1);

    public Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token) => Task.CompletedTask;

    public async Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
    {
        try
        {
            await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
            await conn.OpenAsync(token);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT SYSDATETIMEOFFSET()";

            var committed = (DateTimeOffset)(await cmd.ExecuteScalarAsync(token))!;
            var giveUpAt = DateTimeOffset.UtcNow + Limit;

            while ((DateTimeOffset)(await cmd.ExecuteScalarAsync(token))! <= committed
                   && DateTimeOffset.UtcNow < giveUpAt)
            {
                await Task.Delay(1, token);
            }
        }
        catch (SqlException)
        {
            // Without the wait a fact is only as exposed as it always was
        }
    }
}
