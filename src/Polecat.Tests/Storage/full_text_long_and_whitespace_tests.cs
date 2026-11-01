using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Storage.FullText;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Storage;

/// <summary>
///     #835 and #839 at the token table. Both stored fewer or different terms than the document holds,
///     and neither said so: the first indexed nothing at all for a member over 4000 characters, and the
///     second kept <c>quick\nbrown</c> as one term so neither word could be found.
/// </summary>
/// <remarks>
///     The terms are asserted directly as well as searched for, for the reason
///     <c>full_text_index_tests</c> gives: the token table is where this design fails silently, and a
///     fact going only through a query cannot tell a missing token from a mistranslated <c>Where</c>.
/// </remarks>
public class full_text_long_and_whitespace_tests: OneOffConfigurationsContext
{
    public class Article
    {
        public Guid Id { get; set; }
        public string Body { get; set; } = string.Empty;
    }

    private const int TermWidth = 255;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, FullTextTokenizer tokenizer = FullTextTokenizer.Literal)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<Article>().FullTextIndex(tokenizer, x => x.Body);
        });
    }

    private async Task<Guid> storeAsync(string body)
    {
        var id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(new Article { Id = id, Body = body });
        await session.SaveChangesAsync(Ct);
        return id;
    }

    private async Task<Guid[]> searchAsync(string text)
    {
        await using var session = theStore.QuerySession();
        return (await session.Query<Article>().Where(x => x.Body.PlainTextSearch(text)).ToListAsync(Ct))
            .Select(x => x.Id).ToArray();
    }

    private async Task<string[]> termsAsync(Guid id)
    {
        var mapping = theStore.Options.Providers.GetProvider(typeof(Article)).Mapping;
        var table = FullTextIndex.TableNameFor(mapping);

        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT term FROM [{mapping.DatabaseSchemaName}].[{table}] WHERE doc_id = @id ORDER BY pos";
        cmd.Parameters.AddWithValue("@id", id);

        var terms = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        while (await reader.ReadAsync(Ct)) terms.Add(reader.GetString(0));
        return terms.ToArray();
    }

    // ---- #835: the issue's table ----------------------------------------------------------------

    private static string TextOfLength(int length, string needle)
    {
        var filler = string.Concat(Enumerable.Repeat("lorem ipsum ", length / 12 + 1));
        return (filler[..(length - needle.Length - 1)].TrimEnd() + " " + needle).PadRight(length, 'z');
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_over_four_thousand_characters_is_indexed(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var body = TextOfLength(4107, "needle");
        body.Length.ShouldBe(4107);

        var id = await storeAsync(body);

        (await searchAsync("needle")).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_token_wider_than_the_term_column_does_not_fail_the_save(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        // The issue's own case: 3997 characters, so under the old limit, with a 3990 character token.
        var body = "needle " + new string('b', 3990);
        body.Length.ShouldBe(3997);

        var id = await storeAsync(body);

        (await searchAsync("needle")).ShouldBe([id]);
    }

    // ---- past the limit, every read of the member is the same read ------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task every_term_of_a_long_member_is_stored(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var words = Enumerable.Range(0, 3000).Select(i => $"word{i}").ToArray();
        var id = await storeAsync(string.Join(' ', words));

        (await termsAsync(id)).ShouldBe(words);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_phrase_is_found_past_the_old_limit(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new string('a', 4500) + " the quick brown fox");

        await using var session = theStore.QuerySession();
        var hits = await session.Query<Article>()
            .Where(x => x.Body.PhraseSearch("quick brown")).ToListAsync(Ct);

        hits.Single().Id.ShouldBe(id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_ranked_search_finds_a_long_member(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(TextOfLength(6000, "needle"));

        await using var session = theStore.QuerySession();
        var hits = await session.FullTextSearchAsync<Article>(x => x.Body, "needle", limit: 5, token: Ct);

        hits.Single().Id.ShouldBe(id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_token_at_the_width_is_stored_whole(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var token = new string('c', TermWidth);
        var id = await storeAsync(token);

        (await termsAsync(id)).ShouldBe([token]);
    }

    [Theory]
    [InlineData(true, FullTextTokenizer.Literal)]
    [InlineData(false, FullTextTokenizer.Literal)]
    [InlineData(true, FullTextTokenizer.Porter)]
    [InlineData(false, FullTextTokenizer.Porter)]
    public async Task an_oversized_token_is_clipped_and_stays_findable(bool nativeJson, FullTextTokenizer tokenizer)
    {
        ConfigureFor(nativeJson, tokenizer);

        // Dropping the token would save the document too, but it would leave a value the document does
        // contain unfindable with no error, which is the failure this issue is about. Clipping both
        // sides at one width keeps "search for the thing that is in the document" true.
        var token = new string('d', TermWidth + 45);
        var id = await storeAsync("before " + token + " after");

        var terms = await termsAsync(id);
        terms.Length.ShouldBe(3);
        terms[1].Length.ShouldBe(TermWidth);

        (await searchAsync(token)).ShouldBe([id]);
        (await searchAsync("after")).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true, FullTextTokenizer.Literal)]
    [InlineData(false, FullTextTokenizer.Literal)]
    [InlineData(true, FullTextTokenizer.Porter)]
    [InlineData(false, FullTextTokenizer.Porter)]
    public async Task an_oversized_token_past_the_old_limit_is_clipped_too(bool nativeJson, FullTextTokenizer tokenizer)
    {
        ConfigureFor(nativeJson, tokenizer);

        // Over 4000 in one token, so past the nvarchar(4000) parameter pc_porter_stem takes as well.
        var token = new string('e', 4500);
        var id = await storeAsync("before " + token);

        (await termsAsync(id)).Length.ShouldBe(2);
        (await searchAsync(token)).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_prefix_search_reaches_a_clipped_token(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new string('f', 400));

        await using var session = theStore.QuerySession();
        (await session.Query<Article>().Where(x => x.Body.PrefixSearch("fff")).ToListAsync(Ct))
            .Single().Id.ShouldBe(id);

        var longerThanTheColumn = new string('f', 300);
        (await session.Query<Article>().Where(x => x.Body.PrefixSearch(longerThanTheColumn)).ToListAsync(Ct))
            .Single().Id.ShouldBe(id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task declaring_an_index_backfills_a_long_member(bool nativeJson)
    {
        // The backfill renders the same clause as the trigger, but it is a separate statement, and it is
        // what covers everything written before the index was declared.
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);
        var id = await storeAsync(TextOfLength(5000, "needle"));

        ConfigureFor(nativeJson);

        (await searchAsync("needle")).ShouldBe([id]);
    }

    // ---- #839: the issue's table ----------------------------------------------------------------

    [Theory]
    [InlineData(true, "quick\nbrown fox")]
    [InlineData(false, "quick\nbrown fox")]
    [InlineData(true, "quick brown fox")]
    [InlineData(false, "quick brown fox")]
    public async Task brown_is_found_however_it_is_separated(bool nativeJson, string body)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync(body);

        (await searchAsync("brown")).ShouldBe([id]);
    }

    public static TheoryData<bool, int> EveryWhitespaceCharacter()
    {
        var data = new TheoryData<bool, int>();

        for (var c = 0; c <= char.MaxValue; c++)
        {
            if (!char.IsWhiteSpace((char)c)) continue;

            data.Add(true, c);
            data.Add(false, c);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EveryWhitespaceCharacter))]
    public async Task every_dotnet_whitespace_character_separates_terms(bool nativeJson, int codePoint)
    {
        ConfigureFor(nativeJson);

        var separator = ((char)codePoint).ToString();
        var body = $"alpha{separator}bravo";
        var id = await storeAsync(body);

        (await termsAsync(id)).ShouldBe(["alpha", "bravo"]);

        // The other half of the pair: the C# tokenizer has to split a search the same way, because a
        // query tokenized differently from the document matches nothing and raises nothing.
        FullTextIndex.Tokenize(body).ShouldBe(["alpha", "bravo"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_phrase_can_span_a_line_break(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync("the quick\r\n\tbrown fox");

        await using var session = theStore.QuerySession();
        (await session.Query<Article>().Where(x => x.Body.PhraseSearch("quick brown")).ToListAsync(Ct))
            .Single().Id.ShouldBe(id);
    }

    [Theory]
    [InlineData(true, "quick, brown")]
    [InlineData(false, "quick, brown")]
    [InlineData(true, "quick -- brown")]
    [InlineData(false, "quick -- brown")]
    [InlineData(true, "quick\r\nbrown")]
    [InlineData(false, "quick\r\nbrown")]
    public async Task words_with_several_separators_between_them_are_still_adjacent(bool nativeJson, string body)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync(body);

        await using var session = theStore.QuerySession();
        (await session.Query<Article>().Where(x => x.Body.PhraseSearch("quick brown")).ToListAsync(Ct))
            .Single().Id.ShouldBe(id);
    }

    [Theory]
    [InlineData(true, "line one\nline two\ttabbed\r\nthird  gap")]
    [InlineData(false, "line one\nline two\ttabbed\r\nthird  gap")]
    [InlineData(true, "\u00A0non\u00A0breaking\u3000ideographic\u2003em")]
    [InlineData(false, "\u00A0non\u00A0breaking\u3000ideographic\u2003em")]
    public async Task the_query_tokenizer_still_matches_the_stored_one(bool nativeJson, string body)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(body);

        (await termsAsync(id)).ShouldBe(FullTextIndex.Tokenize(body));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_search_typed_with_a_line_break_is_two_terms(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await storeAsync("quick brown fox");

        (await searchAsync("quick\nfox")).ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_invisible_character_does_not_split_a_term(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        // U+200B is not whitespace to .NET, so it must not be whitespace to the trigger either. Matching
        // through the default collation would treat it as the empty string.
        var body = "foo\u200Bbar";
        char.IsWhiteSpace('\u200B').ShouldBeFalse();

        FullTextIndex.Tokenize(body).Length.ShouldBe(1);

        var id = await storeAsync(body);
        (await termsAsync(id)).Length.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_fullwidth_punctuation_mark_is_not_the_ascii_one(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        // U+FF01 and '!' compare equal under the width-insensitive default collation, so TRANSLATE matched
        // one for the other in the trigger while Tokenize, which is ordinal, did not. Same text, two
        // tokenizations: a search for it found nothing.
        var body = "foo\uFF01bar";
        var id = await storeAsync(body);

        (await termsAsync(id)).Length.ShouldBe(FullTextIndex.Tokenize(body).Length);
    }
}
