using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.Tests.Linq;

namespace Polecat.Tests.Storage;

/// <summary>
///     #916: the trigger replaces a document's tokens, and finds them by <c>doc_id</c>, which led no index.
///     Every write to a type with a full-text index scanned the token table: 3,819 logical reads for a
///     document of 25 tokens among half a million, against 253 once the index is there.
/// </summary>
public class full_text_write_cost_tests: OneOffConfigurationsContext
{
    public class CostedArticle
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private const string Schema = "full_text_write_cost_tests";
    private const string TokenTable = $"[{Schema}].[pc_ft_costedarticle]";
    private const string DocumentTable = $"[{Schema}].[pc_doc_costedarticle]";
    private const string DocumentIndex = "ix_pc_ft_costedarticle_doc";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(bool nativeJson = true, bool conjoined = false)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;

            var expression = opts.Schema.For<CostedArticle>().FullTextIndex(x => x.Body);
            if (conjoined) expression.MultiTenanted();
        });
    }

    private async Task<Guid[]> seedAsync(int documents = 1500)
    {
        var ids = Enumerable.Range(0, documents).Select(_ => Guid.NewGuid()).ToArray();

        await using var session = theStore.LightweightSession();
        for (var i = 0; i < documents; i++)
        {
            session.Store(new CostedArticle { Id = ids[i], Body = $"word{i} filler{i % 17} other{i % 5} more{i % 3}" });
        }

        await session.SaveChangesAsync(Ct);
        return ids;
    }

    private static async Task execAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<bool> hasDocumentIndexAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('{TokenTable}') AND name = '{DocumentIndex}'";
        return (int)(await cmd.ExecuteScalarAsync(Ct))! == 1;
    }

    /// <summary>How the statements of a write, its trigger's included, got to the token table.</summary>
    private static async Task<IReadOnlyList<PlanAccess>> accessesOfAsync(string sql, Guid id)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);

        await using (var on = conn.CreateCommand())
        {
            on.CommandText = "SET STATISTICS XML ON;";
            await on.ExecuteNonQueryAsync(Ct);
        }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@id", id);

        var accesses = new List<PlanAccess>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        do
        {
            while (await reader.ReadAsync(Ct))
            {
                if (reader.FieldCount == 1 && reader.GetValue(0) is string text
                                           && text.Contains("ShowPlanXML", StringComparison.Ordinal))
                {
                    accesses.AddRange(QueryPlanProbe.ReadAccesses(text));
                }
            }
        } while (await reader.NextResultAsync(Ct));

        return accesses.Where(x => x.Table == "pc_ft_costedarticle").ToArray();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_seeks_the_tokens_it_replaces(bool nativeJson)
    {
        configure(nativeJson);
        var ids = await seedAsync();

        var accesses = await accessesOfAsync($"UPDATE {DocumentTable} SET version = version WHERE id = @id", ids[7]);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index == DocumentIndex);
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }

    [Fact]
    public async Task a_delete_seeks_the_tokens_it_removes()
    {
        configure();
        var ids = await seedAsync();

        var accesses = await accessesOfAsync($"DELETE FROM {DocumentTable} WHERE id = @id", ids[7]);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index == DocumentIndex);
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }

    [Fact]
    public async Task a_conjoined_write_seeks_the_tokens_of_its_own_tenant()
    {
        configure(conjoined: true);
        var ids = await seedAsync();

        var accesses = await accessesOfAsync($"UPDATE {DocumentTable} SET version = version WHERE id = @id", ids[7]);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index == DocumentIndex);
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }

    [Fact]
    public async Task a_replaced_document_keeps_only_its_new_tokens()
    {
        configure();
        var ids = await seedAsync(documents: 3);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new CostedArticle { Id = ids[1], Body = "replacement" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("word1")).CountAsync(Ct)).ShouldBe(0);
        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("replacement")).CountAsync(Ct)).ShouldBe(1);
        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("word0")).CountAsync(Ct)).ShouldBe(1);
    }

    /// <summary>The shape every earlier version created, with a document in it.</summary>
    private async Task aTokenTableWithoutTheIndexAsync()
    {
        configure();
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await seedAsync(documents: 2);

        await execAsync($"DROP INDEX {DocumentIndex} ON {TokenTable}");

        // A process that has not touched this type yet.
        configure();
    }

    [Fact]
    public async Task first_use_leaves_an_existing_token_table_alone()
    {
        await aTokenTableWithoutTheIndexAsync();

        await using var query = theStore.QuerySession();

        // Building it in here is the failure this avoids: it would land on whichever request came first.
        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("word1")).CountAsync(Ct)).ShouldBe(1);
        (await hasDocumentIndexAsync()).ShouldBeFalse();

        await using var session = theStore.LightweightSession();
        session.Store(new CostedArticle { Id = Guid.NewGuid(), Body = "a green turtle" });
        await session.SaveChangesAsync(Ct);

        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("turtle")).CountAsync(Ct)).ShouldBe(1);
        (await hasDocumentIndexAsync()).ShouldBeFalse();
    }

    [Fact]
    public async Task applying_the_schema_adds_it_to_an_existing_token_table()
    {
        await aTokenTableWithoutTheIndexAsync();

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        (await hasDocumentIndexAsync()).ShouldBeTrue();
        await theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct);

        await using var query = theStore.QuerySession();
        (await query.Query<CostedArticle>().Where(x => x.Body.PlainTextSearch("word1")).CountAsync(Ct)).ShouldBe(1);
    }

    [Fact]
    public void it_reaches_the_generated_script()
    {
        configure();
        theStore.Options.Providers.GetProvider<CostedArticle>();

        theStore.Advanced.ToDatabaseScript().ShouldContain($"CREATE INDEX {DocumentIndex}");
    }
}
