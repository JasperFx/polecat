using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Patching;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: <c>$.a-b</c> is not a JSON path. A key that isn't a plain identifier has to be quoted, and
///     only the patch path did that (#908). So a query on such a member failed with "JSON path is not
///     properly formatted", and an index on one built a column that failed every write.
/// </summary>
/// <remarks>
///     That is every member of a store on a kebab-case naming policy.
/// </remarks>
public class non_identifier_key_tests: OneOffConfigurationsContext
{
    public class KeyedInner
    {
        [JsonPropertyName("in-ner")] public string? Value { get; set; }
    }

    public class KeyedDoc
    {
        public Guid Id { get; set; }

        [JsonPropertyName("a-b")] public string? Hyphen { get; set; }
        [JsonPropertyName("with space")] public string? Spaced { get; set; }
        [JsonPropertyName("1st")] public int First { get; set; }
        [JsonPropertyName("has.dot")] public string? Dotted { get; set; }
        [JsonPropertyName("clé")] public string? Accented { get; set; }
        [JsonPropertyName("it's")] public string? Apostrophe { get; set; }
        [JsonPropertyName("nest-ed")] public KeyedInner? Nested { get; set; }
        [JsonPropertyName("tag-list")] public List<string> Tags { get; set; } = [];
    }

    public class KebabDoc
    {
        public Guid Id { get; set; }
        public string? FirstName { get; set; }
        public int LuckyNumber { get; set; }
        public KebabDocAddress? HomeAddress { get; set; }
    }

    public class KebabDocAddress
    {
        public string? PostalCode { get; set; }
    }

    public class IndexedKeyDoc
    {
        public Guid Id { get; set; }
        [JsonPropertyName("a-b")] public string? Hyphen { get; set; }
        [JsonPropertyName("c d")] public int Spaced { get; set; }
    }

    public class SearchedKeyDoc
    {
        public Guid Id { get; set; }
        [JsonPropertyName("a-b")] public string? Hyphen { get; set; }
    }

    public class ApostropheDoc
    {
        public Guid Id { get; set; }
        [JsonPropertyName("it's")] public string? Apostrophe { get; set; }
    }

    public record KeyedEvent([property: JsonPropertyName("a-b")] string Hyphen, [property: JsonPropertyName("plain")] int Number);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static KeyedDoc doc(string value, int first) => new()
    {
        Id = Guid.NewGuid(), Hyphen = value, Spaced = value, First = first, Dotted = value, Accented = value,
        Apostrophe = value, Nested = new KeyedInner { Value = value }, Tags = [value, "shared"]
    };

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(doc("one", 1), doc("two", 2));
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_predicate_finds_each_kind_of_key(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyedDoc>();

        (await docs.Where(x => x.Hyphen == "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Spaced == "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.First == 1).CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Dotted == "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Accented == "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Apostrophe == "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Nested!.Value == "one").CountAsync(Ct)).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_full_read_of_a_string_reaches_it(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyedDoc>();

        // Each of these goes through the OPENJSON form on nvarchar(max) storage.
        (await docs.Where(x => x.Hyphen!.Contains("ne")).CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Hyphen != "one").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Accented!.StartsWith("tw")).CountAsync(Ct)).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task it_can_be_ordered_selected_and_grouped(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyedDoc>();

        (await docs.OrderByDescending(x => x.First).Select(x => x.Hyphen).ToListAsync(Ct)).ShouldBe(["two", "one"]);
        (await docs.OrderBy(x => x.Hyphen).Select(x => x.First).ToListAsync(Ct)).ShouldBe([1, 2]);
        (await docs.Select(x => x.Nested!.Value).Distinct().CountAsync(Ct)).ShouldBe(2);
        (await docs.GroupBy(x => x.Spaced).Select(g => g.Key).ToListAsync(Ct)).ShouldBe(["one", "two"], ignoreOrder: true);
        (await docs.MaxAsync(x => x.First, Ct)).ShouldBe(2);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_collection_is_read_from_its_key(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyedDoc>();

        (await docs.Where(x => x.Tags.Contains("one")).CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.Tags.Contains("shared")).CountAsync(Ct)).ShouldBe(2);
        (await docs.Where(x => x.Tags.IsEmpty()).CountAsync(Ct)).ShouldBe(0);
        (await docs.Where(x => x.Tags.Count == 2).CountAsync(Ct)).ShouldBe(2);
    }

    [Fact]
    public async Task a_projection_streams_with_it()
    {
        await using var query = await seededAsync(true);

        var json = await query.Query<KeyedDoc>().Where(x => x.First == 1).Select(x => new { x.Hyphen, x.First })
            .ToJsonArrayAsync(Ct);

        JsonDocument.Parse(json).RootElement[0].GetProperty("hyphen").GetString().ShouldBe("one");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_kebab_case_store_works_at_all(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.ConfigureSerialization(configure: x => x.PropertyNamingPolicy = JsonNamingPolicy.KebabCaseLower);
            opts.Schema.For<KebabDoc>().Index(x => x.FirstName);
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new KebabDoc
            {
                Id = Guid.NewGuid(), FirstName = "Ann", LuckyNumber = 7, HomeAddress = new KebabDocAddress { PostalCode = "0150" }
            });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var docs = query.Query<KebabDoc>();

        (await docs.Where(x => x.FirstName == "Ann").CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.LuckyNumber > 5).CountAsync(Ct)).ShouldBe(1);
        (await docs.Where(x => x.HomeAddress!.PostalCode == "0150").CountAsync(Ct)).ShouldBe(1);
        (await docs.OrderBy(x => x.LuckyNumber).Select(x => x.FirstName).ToListAsync(Ct)).ShouldBe(["Ann"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_index_on_it_works_and_is_used(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<IndexedKeyDoc>().Index(x => x.Hyphen);
            opts.Schema.For<IndexedKeyDoc>().Index(x => x.Spaced);
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Enumerable.Range(0, 1500)
                .Select(i => new IndexedKeyDoc { Id = Guid.NewGuid(), Hyphen = $"value{i}", Spaced = i }).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct);

        await using var query = theStore.QuerySession();

        var text = query.Query<IndexedKeyDoc>().Where(x => x.Hyphen == "value42");
        (await text.ToListAsync(Ct)).ShouldHaveSingleItem();
        (await QueryPlanProbe.AccessesAsync(query, text, Ct)).ShouldContain(x => x.Operator == "Index Seek");

        var number = query.Query<IndexedKeyDoc>().Where(x => x.Spaced == 42);
        (await number.ToListAsync(Ct)).ShouldHaveSingleItem();
        (await QueryPlanProbe.AccessesAsync(query, number, Ct)).ShouldContain(x => x.Operator == "Index Seek");
    }

    [Fact]
    public void an_index_is_typed_from_the_member_a_quoted_path_names()
    {
        ConfigureStore(opts => opts.Schema.For<IndexedKeyDoc>().Index(x => x.Spaced));

        // Quoted, the path no longer split into its keys, so this was a varchar column with no error.
        var mapping = theStore.Options.Providers.GetProvider<IndexedKeyDoc>().Mapping;

        mapping.ResolveClrMemberType("$.\"c d\"").ShouldBe(typeof(int));
    }

    [Fact]
    public async Task a_json_index_on_it_works()
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = true;
            opts.Schema.For<IndexedKeyDoc>().JsonIndex(x => x.Hyphen);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);
        await theDatabase.AssertDatabaseMatchesConfigurationAsync(Ct);

        await using var session = theStore.LightweightSession();
        session.Store(new IndexedKeyDoc { Id = Guid.NewGuid(), Hyphen = "value" });
        await session.SaveChangesAsync(Ct);

        (await session.Query<IndexedKeyDoc>().Where(x => x.Hyphen == "value").CountAsync(Ct)).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_full_text_index_on_it_works(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<SearchedKeyDoc>().FullTextIndex(x => x.Hyphen);
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new SearchedKeyDoc { Id = Guid.NewGuid(), Hyphen = "quick brown fox" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<SearchedKeyDoc>().Where(x => x.Hyphen!.PlainTextSearch("brown")).CountAsync(Ct)).ShouldBe(1);
        (await query.FullTextSearchAsync<SearchedKeyDoc>(x => x.Hyphen, "brown", token: Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public void a_plain_key_keeps_the_names_it_always_had()
    {
        // Renaming these would be a migration against every existing table.
        DocumentIndex.ColumnNameForPath("$.address.city").ShouldBe("cc_address_city");
        new DocumentIndex(["$.userName"]).GetIndexName("pc_doc_user").ShouldBe("ix_pc_doc_user_username");
        new DocumentIndex(["$.a.b", "$.c"]).GetIndexName("t").ShouldBe("ix_t_a_b_c");
    }

    [Fact]
    public void a_name_built_from_a_key_is_a_legal_name_and_keeps_keys_apart()
    {
        var hyphen = DocumentIndex.ColumnNameForPath(SerializedNames.PathFor(["a-b"], null));
        var nested = DocumentIndex.ColumnNameForPath(SerializedNames.PathFor(["a", "b"], null));
        var dotted = DocumentIndex.ColumnNameForPath(SerializedNames.PathFor(["a.b"], null));

        new[] { hyphen, nested, dotted }.Distinct().Count().ShouldBe(3);

        foreach (var name in new[] { hyphen, nested, dotted })
        {
            name.ShouldMatch("^[a-z0-9_]+$");
        }
    }

    [Theory]
    [InlineData("plain", "$.plain")]
    [InlineData("a-b", "$.\"a-b\"")]
    [InlineData("1st", "$.\"1st\"")]
    [InlineData("clé", "$.\"cl\\u00E9\"")]
    public void a_path_reuses_the_patch_paths_rule(string key, string expected)
    {
        SerializedNames.PathFor([key], null).ShouldBe(expected);
        ("$." + JsonPathHelper.Segment(key, null)).ShouldBe(expected);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_apostrophe_a_relaxed_encoder_leaves_alone_cannot_end_the_literal(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.ConfigureSerialization(configure: x => x.Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping);
        });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ApostropheDoc { Id = Guid.NewGuid(), Apostrophe = "one" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<ApostropheDoc>().Where(x => x.Apostrophe == "one").CountAsync(Ct)).ShouldBe(1);
        (await query.Query<ApostropheDoc>().Where(x => x.Apostrophe != "one").CountAsync(Ct)).ShouldBe(0);
    }

    [Fact]
    public async Task an_event_body_is_read_from_the_key_it_was_written_under()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new KeyedEvent("one", 1), new KeyedEvent("two", 2));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = query.Events.QueryRawEventDataOnly<KeyedEvent>();

        (await events.Where(x => x.Hyphen == "one").CountAsync(Ct)).ShouldBe(1);
        (await events.Where(x => x.Number == 2).Select(x => x.Hyphen).FirstAsync(Ct)).ShouldBe("two");
    }
}
