using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Storage;

/// <summary>
///     #642: opt-in Porter stemming on a full-text index. The stemmer itself is pinned to SQLite's in
///     <c>porter_stemmer_tests</c>; these pin what the index does with it.
/// </summary>
public class full_text_porter_tests: OneOffConfigurationsContext
{
    public class Note
    {
        public Guid Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public string Body { get; set; } = string.Empty;
    }

    private const string Retry = "Configure retry for the outbox";

    private async Task<Guid> StoreNoteAsync(string body, string title = "untitled")
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new Note { Id = id, Title = title, Body = body });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return id;
    }

    private async Task<IReadOnlyList<Note>> PlainAsync(string search)
    {
        await using var query = theStore.QuerySession();
        return await query.Query<Note>().Where(x => x.Body.PlainTextSearch(search))
            .ToListAsync(TestContext.Current.CancellationToken);
    }

    private async Task<List<(string Member, string Term)>> StoredTermsAsync(Guid id)
    {
        var mapping = theStore.Options.Providers.GetProvider(typeof(Note)).Mapping;
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT member, term FROM [{mapping.DatabaseSchemaName}].[{FullTextIndex.TableNameFor(mapping)}] "
            + "WHERE doc_id = @id ORDER BY member, pos";
        cmd.Parameters.AddWithValue("@id", id);

        var rows = new List<(string, string)>();
        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }

    // #630 / #642's load-bearing test, extended over every tokenizer: what the TRIGGER stored has to be
    // exactly what the query side computes, or a search answers empty with no error. Against the real
    // trigger -- a C# re-implementation of it would pin nothing.
    [Theory]
    [InlineData(FullTextTokenizer.Literal)]
    [InlineData(FullTextTokenizer.Porter)]
    public async Task the_stored_terms_are_the_query_terms(FullTextTokenizer tokenizer)
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(tokenizer, x => x.Body));
        const string text =
            "Configuring RETRIES; running runs ran, generalizations happily (it's) cafés 2024s a-b";

        var id = await StoreNoteAsync(text);
        var index = theStore.Options.Providers.GetProvider(typeof(Note)).Mapping.FullTextIndexes.Single();

        var stored = await StoredTermsAsync(id);
        stored.Select(x => x.Member).Distinct().ShouldBe([index.TokenKey]);
        stored.Select(x => x.Term).ShouldBe(index.TokenizeQuery(text));
    }

    // Both halves, or the test passes against a corpus that happened to match either way.
    [Fact]
    public async Task porter_matches_an_inflection()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync(Retry);

        (await PlainAsync("configuring retries")).Single().Id.ShouldBe(id);
        (await PlainAsync("configured")).Single().Id.ShouldBe(id);
    }

    [Fact]
    public async Task literal_still_does_not()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(x => x.Body));
        await StoreNoteAsync(Retry);

        (await PlainAsync("configuring retries")).ShouldBeEmpty();
        (await PlainAsync("configure retry")).Count.ShouldBe(1);
    }

    // ⚠️ Written as a CHANGE to an existing index, not a first declaration -- the remarks on
    // the_backfill_repairs_a_tenant_whose_tokens_are_missing record the obvious version of this shape
    // passing against a real bug. Keyed by member alone, the backfill would see the member already
    // "indexed" (with literal terms) and do nothing, and the stemmed search would miss this document.
    [Fact]
    public async Task changing_an_existing_index_to_porter_retokenizes_what_was_written_before()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(x => x.Body));
        var id = await StoreNoteAsync(Retry);
        (await PlainAsync("configuring")).ShouldBeEmpty();

        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));

        (await PlainAsync("configuring")).Single().Id.ShouldBe(id);

        // and the literal rows are gone, not merely shadowed
        (await StoredTermsAsync(id)).Select(x => x.Member).Distinct().ShouldBe(["Body#porter"]);
    }

    [Fact]
    public async Task changing_it_back_retokenizes_too()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync("Configuring retries");

        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(x => x.Body));

        // "configuring" is now a literal term again, and the stem "configur" is not
        (await PlainAsync("configuring")).Single().Id.ShouldBe(id);
        (await StoredTermsAsync(id)).ShouldBe([("Body", "configuring"), ("Body", "retries")]);
    }

    // Stemmed like SQLite's prefix queries, so Fisher and Polecat agree on what a prefix finds. The
    // last case is the documented wrinkle: a partial word whose stem is not a prefix of the stored stem.
    [Theory]
    [InlineData("configuring", true)]
    [InlineData("configures", true)]
    [InlineData("config", true)]
    [InlineData("configuri", false)]
    public async Task prefix_search_is_stemmed_as_sqlite_stems_it(string prefix, bool matches)
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        await StoreNoteAsync(Retry);

        await using var query = theStore.QuerySession();
        var found = await query.Query<Note>().Where(x => x.Body.PrefixSearch(prefix))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Count.ShouldBe(matches ? 1 : 0);
    }

    [Fact]
    public async Task a_phrase_is_matched_by_stem_and_position()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync(Retry);

        await using var query = theStore.QuerySession();
        var phrase = await query.Query<Note>().Where(x => x.Body.PhraseSearch("configuring retries"))
            .ToListAsync(TestContext.Current.CancellationToken);
        phrase.Single().Id.ShouldBe(id);

        // the same stems in the other order are not the phrase
        var reversed = await query.Query<Note>().Where(x => x.Body.PhraseSearch("retries configuring"))
            .ToListAsync(TestContext.Current.CancellationToken);
        reversed.ShouldBeEmpty();
    }

    [Fact]
    public async Task ranked_search_uses_stems()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync(Retry);

        await using var query = theStore.QuerySession();
        var ranked = await query.FullTextSearchAsync<Note>(x => x.Body, "configuring retries",
            token: TestContext.Current.CancellationToken);

        ranked.Single().Id.ShouldBe(id);
    }

    // The tokenizer is per index: an identifier-like member stays literal while prose stems.
    [Fact]
    public async Task tokenizers_are_per_index()
    {
        ConfigureStore(opts => opts.Schema.For<Note>()
            .FullTextIndex(x => x.Title)
            .FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync(body: "running", title: "running");

        await using var query = theStore.QuerySession();
        var body = await query.Query<Note>().Where(x => x.Body.PlainTextSearch("runs"))
            .ToListAsync(TestContext.Current.CancellationToken);
        var title = await query.Query<Note>().Where(x => x.Title.PlainTextSearch("runs"))
            .ToListAsync(TestContext.Current.CancellationToken);

        body.Single().Id.ShouldBe(id);
        title.ShouldBeEmpty();

        (await StoredTermsAsync(id)).ShouldBe([("Body#porter", "run"), ("Title", "running")]);
    }

    // An update rewrites the stemmed terms through the trigger, not only the first insert.
    [Fact]
    public async Task an_update_restems_the_document()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        var id = await StoreNoteAsync(Retry);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Note { Id = id, Body = "Generalizations happily" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        (await PlainAsync("configuring")).ShouldBeEmpty();
        (await PlainAsync("generalization")).Single().Id.ShouldBe(id);
    }

    // The stemmer functions are Weasel objects compared against sys.sql_modules, so a body SQL Server
    // stores differently from the way Polecat declares it would report drift on every pass. Applied and
    // asserted twice: the second round is what proves the first reached a fixed point.
    [Fact]
    public async Task a_porter_index_reaches_a_fixed_point()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body));
        theStore.Options.Providers.GetProvider<Note>();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();

        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();
        await theStore.Database.AssertDatabaseMatchesConfigurationAsync();
    }

    // The functions are in the model, so AutoCreate.None can refuse a store that lacks them -- which is
    // the whole point of putting them through Weasel rather than creating them at first use.
    [Fact]
    public async Task auto_create_none_refuses_a_missing_stemmer()
    {
        ConfigureStore(opts => opts.Schema.For<Note>().FullTextIndex(x => x.Body));
        theStore.Options.Providers.GetProvider<Note>();
        await theStore.Database.ApplyAllConfiguredChangesToDatabaseAsync();

        ConfigureStore(opts =>
        {
            opts.AutoCreateSchemaObjects = JasperFx.AutoCreate.None;
            opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body);
        });
        theStore.Options.Providers.GetProvider<Note>();

        var ex = await Should.ThrowAsync<Exception>(
            () => theStore.Database.AssertDatabaseMatchesConfigurationAsync());
        ex.ToString().ShouldContain(PorterStemFunction.StemFunctionName);
    }

    // #625's tenant isolation holds under a Porter index: the trigger and the stemmed search both stay
    // inside the session's tenant.
    [Fact]
    public async Task conjoined_tenants_stay_apart_under_porter()
    {
        ConfigureStore(opts =>
        {
            opts.Policies.AllDocumentsAreMultiTenanted();
            opts.Schema.For<Note>().FullTextIndex(FullTextTokenizer.Porter, x => x.Body);
        });

        var shared = Guid.NewGuid();
        var token = TestContext.Current.CancellationToken;
        await using (var a = theStore.LightweightSession("a"))
        {
            a.Store(new Note { Id = shared, Body = Retry });
            await a.SaveChangesAsync(token);
        }

        await using (var b = theStore.LightweightSession("b"))
        {
            b.Store(new Note { Id = shared, Body = "Generalizations happily" });
            await b.SaveChangesAsync(token);
        }

        await using var queryA = theStore.QuerySession("a");
        await using var queryB = theStore.QuerySession("b");
        (await queryA.Query<Note>().Where(x => x.Body.PlainTextSearch("configuring")).ToListAsync(token))
            .Count.ShouldBe(1);
        (await queryB.Query<Note>().Where(x => x.Body.PlainTextSearch("configuring")).ToListAsync(token))
            .ShouldBeEmpty();
        (await queryB.Query<Note>().Where(x => x.Body.PlainTextSearch("generalization")).ToListAsync(token))
            .Count.ShouldBe(1);
    }
}
