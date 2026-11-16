using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     #877: the backfill behind a newly declared index runs a batch of documents at a time, because as one
///     statement it could not finish. Over 8,000 documents of 7,000 characters it timed out at 30 seconds
///     in the first request and again in every one after it, so the type could not be queried at all.
/// </summary>
/// <remarks>
///     These run over a few batches rather than at that scale, which took minutes to prove once. They pin
///     what batching could break: documents past the first batch, a document that yields nothing, and the
///     list of keys the batches work down, in each shape a key comes in.
/// </remarks>
public class full_text_backfill_tests: OneOffConfigurationsContext
{
    public class BackfilledArticle
    {
        public Guid Id { get; set; }
        public string? Body { get; set; }
    }

    public class KeyedArticle
    {
        public string Id { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    public class NumberedArticle
    {
        public int Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Past the batch size, so that the last documents are only reached by paging.
    private const int Count = 250;

    private static readonly string LongBody = new string('x', 2500) + " needle " + new string('y', 2500);

    private void configure(bool nativeJson, Action<StoreOptions>? also = null)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            also?.Invoke(opts);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task declaring_an_index_covers_every_document_and_every_length(bool nativeJson)
    {
        configure(nativeJson);
        await theStore.Advanced.CleanAsync<BackfilledArticle>(Ct);

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < Count; i++)
            {
                session.Store(new BackfilledArticle { Id = Guid.NewGuid(), Body = $"common unique{i}" });
            }

            session.Store(new BackfilledArticle { Id = Guid.NewGuid(), Body = LongBody });
            await session.SaveChangesAsync(Ct);
        }

        configure(nativeJson, opts => opts.Schema.For<BackfilledArticle>().FullTextIndex(x => x.Body));

        await using var query = theStore.QuerySession();

        (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch("common"), Ct)).ShouldBe(Count);
        (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch("needle"), Ct)).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_document_that_yields_no_tokens_does_not_stop_it(bool nativeJson)
    {
        configure(nativeJson);
        await theStore.Advanced.CleanAsync<BackfilledArticle>(Ct);

        await using (var session = theStore.LightweightSession())
        {
            // Several batches of nothing to tokenize, wherever they sort among the ids.
            for (var i = 0; i < Count; i++)
            {
                session.Store(new BackfilledArticle
                {
                    Id = Guid.NewGuid(), Body = i % 3 == 0 ? "" : i % 3 == 1 ? "..." : null
                });
            }

            session.Store(new BackfilledArticle { Id = Guid.NewGuid(), Body = "findable" });
            await session.SaveChangesAsync(Ct);
        }

        configure(nativeJson, opts => opts.Schema.For<BackfilledArticle>().FullTextIndex(x => x.Body));

        await using var query = theStore.QuerySession();

        (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch("findable"), Ct)).ShouldBe(1);
    }

    [Fact]
    public async Task it_covers_a_string_key()
    {
        configure(ConnectionSource.SupportsNativeJson);
        await theStore.Advanced.CleanAsync<KeyedArticle>(Ct);

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < Count; i++)
            {
                session.Store(new KeyedArticle { Id = $"key-{i}", Body = "common" });
            }

            await session.SaveChangesAsync(Ct);
        }

        configure(ConnectionSource.SupportsNativeJson,
            opts => opts.Schema.For<KeyedArticle>().FullTextIndex(x => x.Body));

        await using var query = theStore.QuerySession();

        (await query.Query<KeyedArticle>().CountAsync(x => x.Body.PlainTextSearch("common"), Ct)).ShouldBe(Count);
    }

    [Fact]
    public async Task it_covers_a_numeric_key()
    {
        configure(ConnectionSource.SupportsNativeJson);
        await theStore.Advanced.CleanAsync<NumberedArticle>(Ct);

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 1; i <= Count; i++)
            {
                session.Store(new NumberedArticle { Id = i, Body = "common" });
            }

            await session.SaveChangesAsync(Ct);
        }

        configure(ConnectionSource.SupportsNativeJson,
            opts => opts.Schema.For<NumberedArticle>().FullTextIndex(x => x.Body));

        await using var query = theStore.QuerySession();

        (await query.Query<NumberedArticle>().CountAsync(x => x.Body.PlainTextSearch("common"), Ct)).ShouldBe(Count);
    }

    [Fact]
    public async Task it_covers_tenants_that_share_ids()
    {
        void Conjoined(StoreOptions opts) => opts.Policies.AllDocumentsAreMultiTenanted();

        configure(ConnectionSource.SupportsNativeJson, Conjoined);
        await theStore.Advanced.CleanAsync<BackfilledArticle>(Ct);

        var ids = Enumerable.Range(0, Count).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(tenant);
            foreach (var id in ids)
            {
                session.Store(new BackfilledArticle { Id = id, Body = $"common {tenant}" });
            }

            await session.SaveChangesAsync(Ct);
        }

        configure(ConnectionSource.SupportsNativeJson, opts =>
        {
            Conjoined(opts);
            opts.Schema.For<BackfilledArticle>().FullTextIndex(x => x.Body);
        });

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var query = theStore.QuerySession(tenant);

            (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch("common"), Ct))
                .ShouldBe(Count);
            (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch(tenant), Ct))
                .ShouldBe(Count);
        }
    }

    /// <summary>
    ///     gh-1021: the backfill used to tokenize only documents last written before it began, and the
    ///     server clock ticks every 4 ms or so on Linux. A document written before the index existed, in
    ///     the tick the backfill began, was skipped by the backfill and had never met the trigger, so it
    ///     was out of every search for good. CI saw it as "should be 250 but was 147".
    /// </summary>
    /// <remarks>
    ///     Waiting for the clock to land that way is a flake, not a test, so this writes the timestamp
    ///     instead: every document is stamped far past the backfill's start, which is the same tick and
    ///     everything after it at once. Before the fix none of them could be found.
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_document_stamped_at_or_after_the_start_is_still_covered(bool nativeJson)
    {
        configure(nativeJson);
        await theStore.Advanced.CleanAsync<BackfilledArticle>(Ct);

        await using (var session = theStore.LightweightSession())
        {
            for (var i = 0; i < Count; i++)
            {
                session.Store(new BackfilledArticle { Id = Guid.NewGuid(), Body = $"common unique{i}" });
            }

            await session.SaveChangesAsync(Ct);
        }

        // No index yet, so no trigger: nothing is tokenized by this.
        await stampInTheFutureAsync(typeof(BackfilledArticle));

        configure(nativeJson, opts => opts.Schema.For<BackfilledArticle>().FullTextIndex(x => x.Body));

        await using var query = theStore.QuerySession();

        (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch("common"), Ct)).ShouldBe(Count);

        // Covered once, not twice: one "common" per document.
        (await countTokensAsync(typeof(BackfilledArticle), "common")).ShouldBe(Count);
    }

    /// <summary>
    ///     gh-1021, through the other way in: an index that already has tokens, under conjoined tenancy,
    ///     repairing one tenant's documents while another tenant holds the same ids. That takes the
    ///     established branch of the list, and the tenant has to be part of the question the batch asks
    ///     of the token table, or Red's tokens would answer for Blue's documents.
    /// </summary>
    [Fact]
    public async Task a_tenant_repaired_after_the_start_is_still_covered()
    {
        void Conjoined(StoreOptions opts)
        {
            opts.Policies.AllDocumentsAreMultiTenanted();
            opts.Schema.For<BackfilledArticle>().FullTextIndex(x => x.Body);
        }

        configure(ConnectionSource.SupportsNativeJson, Conjoined);
        await theStore.Advanced.CleanAsync<BackfilledArticle>(Ct);

        var ids = Enumerable.Range(0, Count).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(tenant);
            foreach (var id in ids)
            {
                session.Store(new BackfilledArticle { Id = id, Body = $"common {tenant}" });
            }

            await session.SaveChangesAsync(Ct);
        }

        // Blue loses its tokens without a write, and its documents look written after the backfill
        // begins. Raw SQL rather than a session, because a write through the trigger would repair it.
        var mapping = theStore.Options.Providers.GetProvider(typeof(BackfilledArticle)).Mapping;
        await executeAsync(
            $"DELETE FROM [{mapping.DatabaseSchemaName}].[{FullTextIndex.TableNameFor(mapping)}] WHERE tenant_id = 'blue'");
        await executeAsync(
            $"DISABLE TRIGGER [{mapping.DatabaseSchemaName}].[{FullTextTrigger.NameFor(mapping)}] ON [{mapping.DatabaseSchemaName}].[{mapping.TableName}]");
        await stampInTheFutureAsync(typeof(BackfilledArticle), "blue");
        await executeAsync(
            $"ENABLE TRIGGER [{mapping.DatabaseSchemaName}].[{FullTextTrigger.NameFor(mapping)}] ON [{mapping.DatabaseSchemaName}].[{mapping.TableName}]");

        // A new store ensures the table again, which runs the backfill again.
        configure(ConnectionSource.SupportsNativeJson, Conjoined);

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var query = theStore.QuerySession(tenant);

            (await query.Query<BackfilledArticle>().CountAsync(x => x.Body!.PlainTextSearch(tenant), Ct))
                .ShouldBe(Count);
        }

        (await countTokensAsync(typeof(BackfilledArticle), "blue")).ShouldBe(Count);
        (await countTokensAsync(typeof(BackfilledArticle), "red")).ShouldBe(Count);
        (await countTokensAsync(typeof(BackfilledArticle), "common")).ShouldBe(Count * 2);
    }

    private async Task stampInTheFutureAsync(Type documentType, string? tenantId = null)
    {
        var mapping = theStore.Options.Providers.GetProvider(documentType).Mapping;
        var where = tenantId == null ? "" : $" WHERE tenant_id = '{tenantId}'";

        await executeAsync(
            $"UPDATE [{mapping.DatabaseSchemaName}].[{mapping.TableName}] SET last_modified = '9999-12-31T00:00:00+00:00'{where}");
    }

    private async Task<int> countTokensAsync(Type documentType, string term)
    {
        var mapping = theStore.Options.Providers.GetProvider(documentType).Mapping;

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT COUNT(*) FROM [{mapping.DatabaseSchemaName}].[{FullTextIndex.TableNameFor(mapping)}] WHERE term = @term";
        cmd.Parameters.AddWithValue("@term", term);

        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static async Task executeAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(Ct);
    }
}
