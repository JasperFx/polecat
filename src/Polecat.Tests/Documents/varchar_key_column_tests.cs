using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Shouldly;

namespace Polecat.Tests.Documents;

/// <summary>
///     #919: the client decides what SQL Server would lose without asking it each time, so it had better
///     agree with SQL Server. The sweep is what says it does, on every code page a database can have.
/// </summary>
public class varchar_key_column_tests
{
    private const int Utf8 = 65001;

    [Theory]
    [InlineData(1252, "SQL_Latin1_General_CP1_CI_AS")]
    [InlineData(1252, "Latin1_General_100_CI_AS")]
    [InlineData(932, "Japanese_CI_AS")]
    [InlineData(936, "Chinese_PRC_CI_AS")]
    [InlineData(949, "Korean_Wansung_CI_AS")]
    [InlineData(950, "Chinese_Taiwan_Stroke_CI_AS")]
    [InlineData(1250, "Polish_CI_AS")]
    [InlineData(1251, "Cyrillic_General_CI_AS")]
    [InlineData(1253, "Greek_CI_AS")]
    [InlineData(1254, "Turkish_CI_AS")]
    [InlineData(1255, "Hebrew_CI_AS")]
    [InlineData(1256, "Arabic_CI_AS")]
    [InlineData(1257, "Lithuanian_CI_AS")]
    [InlineData(1258, "Vietnamese_CI_AS")]
    [InlineData(874, "Thai_CI_AS")]
    [InlineData(437, "SQL_Latin1_General_CP437_CI_AS")]
    [InlineData(850, "SQL_Latin1_General_CP850_CI_AS")]
    [InlineData(Utf8, "Latin1_General_100_CI_AS_SC_UTF8")]
    public async Task it_refuses_exactly_what_the_server_would_change(int codePage, string collation)
    {
        var token = TestContext.Current.CancellationToken;

        // Every character of the basic plane, through varchar under that collation and back.
        var lostByTheServer = new HashSet<int>();
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(token);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                WITH n AS (SELECT TOP 65535 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                           FROM sys.all_columns a CROSS JOIN sys.all_columns b)
                SELECT CONVERT(int, i) FROM n
                WHERE i NOT BETWEEN 0xD800 AND 0xDFFF
                  AND CONVERT(varbinary(8), CONVERT(nvarchar(4), CONVERT(varchar(8), NCHAR(i) COLLATE {collation})))
                      <> CONVERT(varbinary(8), NCHAR(i));
                """;

            await using var reader = await cmd.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token))
            {
                lostByTheServer.Add(reader.GetInt32(0));
            }
        }

        var column = new VarcharKeyColumn(codePage, codePage);
        var refusedByTheClient = Enumerable.Range(1, 65535)
            .Where(x => x is < 0xD800 or > 0xDFFF)
            .Where(x => column.CodePageLosing(((char)x).ToString()) != null)
            .ToHashSet();

        refusedByTheClient.Except(lostByTheServer).ShouldBeEmpty();
        lostByTheServer.Except(refusedByTheClient).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(1252, "café", null)]
    [InlineData(1252, "東京", 1252)]
    [InlineData(1252, "ā", 1252)]
    [InlineData(1252, "?", null)]
    [InlineData(1252, "😀", 1252)]
    [InlineData(932, "東京", null)]
    [InlineData(932, "café", 932)]
    [InlineData(Utf8, "東京", null)]
    [InlineData(Utf8, "😀", null)]
    public void it_goes_by_the_code_page(int codePage, string value, int? expected)
    {
        new VarcharKeyColumn(codePage, codePage).CodePageLosing(value).ShouldBe(expected);
    }

    [Fact]
    public void half_a_surrogate_pair_survives_nothing_not_even_utf8()
    {
        new VarcharKeyColumn(Utf8, Utf8).CodePageLosing("a\uD800b").ShouldBe(Utf8);
    }

    [Fact]
    public void either_code_page_can_lose_it()
    {
        // The parameter is encoded with the database's, and the column keeps what its own holds
        new VarcharKeyColumn(1252, 932).CodePageLosing("東京").ShouldBe(1252);
        new VarcharKeyColumn(932, 1252).CodePageLosing("東京").ShouldBe(1252);
        new VarcharKeyColumn(Utf8, 1252).CodePageLosing("東京").ShouldBe(1252);
        new VarcharKeyColumn(1252, Utf8).CodePageLosing("東京").ShouldBe(1252);
        new VarcharKeyColumn(932, Utf8).CodePageLosing("東京").ShouldBeNull();
    }

    [Theory]
    [InlineData(0)] // no collation to go by
    [InlineData(99999)] // not one the provider has
    public void a_code_page_that_cannot_be_read_refuses_nothing(int codePage)
    {
        new VarcharKeyColumn(codePage, codePage).CodePageLosing("東京").ShouldBeNull();
    }

    [Fact]
    public void ascii_is_never_a_question()
    {
        new VarcharKeyColumn(932, 1252).CodePageLosing("plain ~ id").ShouldBeNull();
    }
}
