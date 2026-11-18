using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.TestUtils;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Storage;

/// <summary>
///     Covers the guard that turns "this instance has no VECTOR type" into an actionable message,
///     and — the half that regressed — covers it NOT firing on every other way the ALTER TABLE that
///     adds a vector column can fail.
/// </summary>
/// <remarks>
///     <para>
///         <c>SqlException</c> has no public constructor and cannot be mocked, so both facts obtain a
///         REAL one from the server by running SQL that raises the error in question. That is also
///         what makes them meaningful: they assert against the error numbers SQL Server actually
///         produces, not against numbers restated from a doc page.
///     </para>
///     <para>
///         Neither fact needs the VECTOR type, so both run on the `edge` lane too — which is the
///         point, since the true-positive case is what that lane exercises for real.
///     </para>
/// </remarks>
public class vector_type_guard_tests
{
    private static async Task<SqlException> captureAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        return await Should.ThrowAsync<SqlException>(async () =>
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task an_undefined_type_is_recognized_as_a_missing_vector_type()
    {
        // Exactly the shape a pre-2025 instance answers a VECTOR(n) declaration with. Raised here
        // from a type name no instance defines, so the fact holds on a server that HAS vectors.
        var e = await captureAsync("SELECT CAST('a' AS NOTAREALTYPE);");

        e.Number.ShouldBe(243);
        DocumentTableEnsurer.IsMissingVectorType(e).ShouldBeTrue();
    }

    [Fact]
    public async Task another_failure_that_merely_mentions_the_column_is_not_a_missing_vector_type()
    {
        // A member named `Vector` produces the computed column `vec_vector`. Any failure quoting
        // that name contains the substring "vector" — which is what the first cut of this guard
        // matched on, and why it reported a missing type on an instance that has one.
        var table = $"#vec_guard_{Guid.NewGuid():N}";
        var e = await captureAsync(
            $"CREATE TABLE {table} (id int); "
            + $"ALTER TABLE {table} ADD [vec_vector] int; "
            + $"ALTER TABLE {table} ADD [vec_vector] int;");

        // The precondition that made the old rule wrong: the message really does say "vector".
        e.Message.ShouldContain("vec_vector");
        e.Number.ShouldNotBe(243);

        DocumentTableEnsurer.IsMissingVectorType(e).ShouldBeFalse();
    }
}
