using Polecat.Linq;
using Polecat.Storage;
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
}
