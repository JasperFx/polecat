using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Storage;

/// <summary>
///     #877: the <c>term</c> column was <c>varchar(255)</c>, which is two defects with one cause.
/// </summary>
/// <remarks>
///     <para>
///         <b>It lost every character outside the database's code page.</b> Under the default
///         <c>SQL_Latin1_General_CP1_CI_AS</c> the trigger's <c>CONVERT(varchar(255), ...)</c> stored
///         <c>東京</c> as <c>??</c>, so nothing written in Chinese, Japanese, Korean, Cyrillic, Greek,
///         Arabic or Hebrew could ever be found. No error, just an empty result.
///     </para>
///     <para>
///         <b>And a search scanned.</b> The search term binds as <c>nvarchar</c> against a <c>varchar</c>
///         column, and under a SQL collation that puts <c>CONVERT_IMPLICIT</c> on the column.
///     </para>
///     <para>
///         ⚠️ <b>Binding the term as <c>varchar</c> looks like the fix for the second and is a trap.</b> It
///         seeks, and it turns the first defect from a miss into a false match: <c>大阪</c> binds as
///         <c>??</c> and finds every document holding <c>東京</c>. The column has to hold what it is asked
///         to hold, and then the <c>nvarchar</c> parameter is simply the right type.
///     </para>
/// </remarks>
public class full_text_unicode_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private const string Schema = "full_text_unicode_tests";
    private const string TokenTable = $"[{Schema}].[pc_ft_article]";
    private const string IndexName = "ix_pc_ft_article_term";
    private const string DocumentIndexName = "ix_pc_ft_article_doc";

    private const string Tokyo = "東京";
    private const string Osaka = "大阪";
    private const string Peace = "мир";
    private const string Hello = "안녕";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<Article>().FullTextIndex(x => x.Body);
        });
    }

    private async Task storeAsync(params string[] bodies)
    {
        await using var session = theStore.LightweightSession();
        foreach (var body in bodies) session.Store(new Article { Id = Guid.NewGuid(), Body = body });
        await session.SaveChangesAsync(Ct);
    }

    private async Task<string[]> searchAsync(string term)
    {
        await using var query = theStore.QuerySession();
        return (await query.Query<Article>().Where(x => x.Body.PlainTextSearch(term)).ToListAsync(Ct))
            .Select(x => x.Body).ToArray();
    }

    private static async Task execAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task text_outside_the_code_page_is_findable(bool nativeJson)
    {
        configure(nativeJson);
        await storeAsync($"{Tokyo} station", $"{Peace} and quiet", $"{Hello} there");

        (await searchAsync(Tokyo)).ShouldBe([$"{Tokyo} station"]);
        (await searchAsync(Peace)).ShouldBe([$"{Peace} and quiet"]);
        (await searchAsync(Hello)).ShouldBe([$"{Hello} there"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task two_terms_of_the_same_length_outside_the_code_page_stay_apart(bool nativeJson)
    {
        configure(nativeJson);
        await storeAsync($"{Tokyo} one", $"{Osaka} two");

        // A varchar column or a varchar parameter makes both of these "??".
        (await searchAsync(Tokyo)).ShouldBe([$"{Tokyo} one"]);
        (await searchAsync(Osaka)).ShouldBe([$"{Osaka} two"]);
    }

    [Fact]
    public async Task a_phrase_and_a_prefix_reach_text_outside_the_code_page()
    {
        configure(ConnectionSource.SupportsNativeJson);
        await storeAsync($"{Tokyo} {Osaka} line", $"{Osaka} {Tokyo} line");

        await using var query = theStore.QuerySession();

        var phrase = await query.Query<Article>().Where(x => x.Body.PhraseSearch($"{Tokyo} {Osaka}")).ToListAsync(Ct);
        phrase.Single().Body.ShouldBe($"{Tokyo} {Osaka} line");

        var prefix = await query.Query<Article>().Where(x => x.Body.PrefixSearch("東")).ToListAsync(Ct);
        prefix.Count.ShouldBe(2);
    }

    [Fact]
    public async Task ranking_reaches_text_outside_the_code_page()
    {
        configure(ConnectionSource.SupportsNativeJson);
        await storeAsync($"{Tokyo} {Tokyo} {Tokyo}", $"{Tokyo} and something else entirely", $"{Osaka} only");

        await using var query = theStore.QuerySession();

        var ranked = await query.FullTextSearchAsync<Article>(x => x.Body, Tokyo, token: Ct);

        ranked.Count.ShouldBe(2);
    }

    [Fact]
    public async Task a_search_seeks_the_token_index()
    {
        configure(ConnectionSource.SupportsNativeJson);

        await storeAsync(Enumerable.Range(0, 1500).Select(i => $"word{i} filler{i % 17}").ToArray());

        await using var query = theStore.QuerySession();

        var queryable = query.Query<Article>().Where(x => x.Body.PlainTextSearch("word42"));
        (await queryable.ToListAsync(Ct)).ShouldHaveSingleItem();

        var accesses = await QueryPlanProbe.AccessesAsync(query, queryable, Ct);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index == IndexName);
        accesses.ShouldNotContain(x => x.Table == "pc_ft_article" && x.Operator.EndsWith("Scan"));
    }

    [Fact]
    public async Task a_prefix_search_seeks_the_token_index()
    {
        configure(ConnectionSource.SupportsNativeJson);

        await storeAsync(Enumerable.Range(0, 1500).Select(i => $"word{i} filler{i % 17}").ToArray());

        await using var query = theStore.QuerySession();

        var queryable = query.Query<Article>().Where(x => x.Body.PrefixSearch("word4"));
        (await queryable.ToListAsync(Ct)).ShouldNotBeEmpty();

        var accesses = await QueryPlanProbe.AccessesAsync(query, queryable, Ct);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index == IndexName);
        accesses.ShouldNotContain(x => x.Table == "pc_ft_article" && x.Operator.EndsWith("Scan"));
    }

    /// <summary>The shape every earlier version created, holding what they stored.</summary>
    private async Task anOldTokenTableAsync(bool nativeJson)
    {
        configure(nativeJson);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await storeAsync("the quick brown fox");

        await execAsync($"""
                         DROP INDEX {IndexName} ON {TokenTable};
                         DROP INDEX {DocumentIndexName} ON {TokenTable};
                         ALTER TABLE {TokenTable} ALTER COLUMN term varchar(255) NOT NULL;
                         CREATE INDEX {IndexName} ON {TokenTable} (term, doc_id) INCLUDE (pos, member, tenant_id);
                         """);

        // A process that has not touched this type yet.
        configure(nativeJson);
    }

    private static async Task<string?> columnTypeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
                           SELECT t.name FROM sys.columns c
                           INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
                           WHERE c.object_id = OBJECT_ID('{TokenTable}') AND c.name = 'term'
                           """;
        return (string?)await cmd.ExecuteScalarAsync(Ct);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task first_use_leaves_an_existing_token_table_alone(bool nativeJson)
    {
        await anOldTokenTableAsync(nativeJson);

        // Timing out in here is the failure this is avoiding, and it would repeat on every request.
        (await searchAsync("quick")).ShouldBe(["the quick brown fox"]);
        (await columnTypeAsync()).ShouldBe("varchar");

        // A write through the current trigger lands in the old column, and stays findable.
        await storeAsync("a slow green turtle");
        (await searchAsync("turtle")).ShouldBe(["a slow green turtle"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task applying_the_schema_widens_an_existing_token_table_with_its_rows_intact(bool nativeJson)
    {
        await anOldTokenTableAsync(nativeJson);

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await columnTypeAsync()).ShouldBe("nvarchar");
        await theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct);

        (await searchAsync("quick")).ShouldBe(["the quick brown fox"]);

        await storeAsync($"{Tokyo} station");
        (await searchAsync(Tokyo)).ShouldBe([$"{Tokyo} station"]);
    }

    [Fact]
    public async Task a_rebuild_refuses_a_token_table_that_would_lose_the_same_characters_again()
    {
        await anOldTokenTableAsync(ConnectionSource.SupportsNativeJson);
        (await searchAsync("quick")).ShouldNotBeEmpty();

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            theStore.Advanced.RebuildFullTextIndexAsync<Article>(Ct));

        ex.Message.ShouldContain("ApplyAllConfiguredChangesToDatabaseAsync");
    }

    [Fact]
    public async Task a_rebuild_recovers_a_token_an_earlier_version_lost()
    {
        configure(ConnectionSource.SupportsNativeJson);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await storeAsync($"{Tokyo} station");

        // What the old column kept of it.
        await execAsync($"UPDATE {TokenTable} SET term = N'??' WHERE term = N'{Tokyo}'");
        (await searchAsync(Tokyo)).ShouldBeEmpty();

        await theStore.Advanced.RebuildFullTextIndexAsync<Article>(Ct);

        (await searchAsync(Tokyo)).ShouldBe([$"{Tokyo} station"]);
    }

    [Fact]
    public async Task the_longest_term_still_fits_on_both_sides()
    {
        configure(ConnectionSource.SupportsNativeJson);

        var token = new string('東', FullTextIndex.MaxTermLength + 50);
        await storeAsync(token);

        (await searchAsync(token)).ShouldBe([token]);
    }
}
