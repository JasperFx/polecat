using System.Text.Json;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Storage.FullText;
using Polecat.TestUtils;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer;

namespace Polecat.Tests.Storage.FullText;

/// <summary>
///     #642: both Porter stemmers — the C# one the query side uses and the T-SQL one the trigger
///     uses — pinned to the same independent oracle.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>This is the test the whole feature stands on.</b> A query term stemmed differently from
///         the stored term it should match is not an error, just a search that quietly answers empty. So
///         the two implementations are not compared with each other — a shared mistake would pass —
///         but each with <c>porter_oracle.tsv</c>: SQLite FTS5's <c>porter</c> tokenizer over every tenth
///         word of the system dictionary (Webster's 2nd, public domain), 23,549 pairs. That is also what
///         Fisher stems with, so passing here means Polecat and Fisher stem identically.
///     </para>
///     <para>
///         Regenerate it with SQLite, not by hand: insert one word per row into an
///         <c>fts5(x, tokenize='porter ascii')</c> table and read stems back through an
///         <c>fts5vocab(..., 'instance')</c> table joined on the row id.
///     </para>
///     <para>
///         The T-SQL half runs the real function inside SQL Server, created through Weasel exactly as a
///         store creates it — a C# re-implementation of the T-SQL would pin nothing.
///     </para>
/// </remarks>
[Collection("integration")]
public class porter_stemmer_tests
{
    private const string Schema = "porter_stemmer";

    private static readonly (string Word, string Stem)[] Oracle =
        File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "Storage", "FullText", "porter_oracle.tsv"))
            .Select(line => line.Split('\t'))
            .Select(parts => (parts[0], parts[1]))
            .ToArray();

    [Fact]
    public void the_oracle_is_the_one_the_comments_describe()
    {
        Oracle.Length.ShouldBe(23549);

        // the motivating example from #630 / #642, and Porter's own textbook cases
        var stems = Oracle.ToDictionary(x => x.Word, x => x.Stem);
        stems["configuring"].ShouldBe("configur");
        stems["configure"].ShouldBe("configur");
        stems["retries"].ShouldBe("retri");
        stems["retry"].ShouldBe("retri");
        stems["caresses"].ShouldBe("caress");
        stems["ponies"].ShouldBe("poni");
        stems["generalizations"].ShouldBe("gener");
    }

    [Fact]
    public void csharp_stems_every_oracle_word_as_sqlite_does()
    {
        var mismatches = Oracle
            .Where(x => PorterStemmer.Stem(x.Word) != x.Stem)
            .Select(x => $"{x.Word}: expected {x.Stem}, got {PorterStemmer.Stem(x.Word)}")
            .ToList();

        mismatches.ShouldBeEmpty();
    }

    [Fact]
    public async Task tsql_stems_every_oracle_word_as_sqlite_does()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await EnsureFunctionsAsync(conn);

        var stems = await StemInSqlServerAsync(conn, Oracle.Select(x => x.Word).ToArray());

        var mismatches = Oracle
            .Where(x => stems[x.Word] != x.Stem)
            .Select(x => $"{x.Word}: expected {x.Stem}, got {stems[x.Word]}")
            .ToList();

        mismatches.ShouldBeEmpty();
    }

    // The oracle is all lowercase ASCII words, so it cannot say what happens to anything else. Both
    // sides pass such a token through unchanged (PorterStemmer.IsStemmable), and that agreement has to
    // be pinned separately or a token like "café" could stem differently on each side.
    [Theory]
    [InlineData("cafés")]          // non-ASCII letter
    [InlineData("naïve")]
    [InlineData("2024s")]               // digits
    [InlineData("ab")]                  // under three characters
    [InlineData("is")]
    [InlineData("x")]
    public async Task both_sides_pass_an_unstemmable_token_through_unchanged(string token)
    {
        PorterStemmer.Stem(token).ShouldBe(token);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await EnsureFunctionsAsync(conn);

        (await StemInSqlServerAsync(conn, [token]))[token].ShouldBe(token);
    }

    [Fact]
    public async Task both_sides_pass_a_token_over_sixty_four_characters_through()
    {
        var longWord = string.Concat(Enumerable.Repeat("relational", 7));   // 70 characters
        var atLimit = longWord[..64];

        PorterStemmer.Stem(longWord).ShouldBe(longWord);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await EnsureFunctionsAsync(conn);

        var stems = await StemInSqlServerAsync(conn, [longWord, atLimit]);
        stems[longWord].ShouldBe(longWord);
        stems[atLimit].ShouldBe(PorterStemmer.Stem(atLimit));
    }

    private static async Task EnsureFunctionsAsync(SqlConnection conn)
    {
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"IF SCHEMA_ID('{Schema}') IS NULL EXEC('CREATE SCHEMA [{Schema}]');";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var migration = await SchemaMigration.DetermineAsync(conn, TestContext.Current.CancellationToken,
            PorterStemFunction.For(Schema).ToArray<Weasel.Core.ISchemaObject>());
        await new SqlServerMigrator().ApplyAllAsync(conn, migration, AutoCreate.CreateOrUpdate,
            ct: TestContext.Current.CancellationToken);
    }

    private static async Task<Dictionary<string, string>> StemInSqlServerAsync(SqlConnection conn,
        string[] words)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = 300;
        cmd.CommandText =
            $"SELECT w.value, {PorterStemFunction.Call(Schema, "w.value")} FROM OPENJSON(@words) w;";
        cmd.Parameters.AddWithValue("@words", JsonSerializer.Serialize(words));

        var stems = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            stems[reader.GetString(0)] = reader.GetString(1);
        }

        return stems;
    }
}
