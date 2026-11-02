using System.Linq.Expressions;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using JasperFx.Events;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Patching;

/// <summary>
///     #861. A patch has to name a key exactly as the serializer wrote it. A JSON path matches the raw
///     text of the stored key, so a path that misses doesn't fail: <c>JSON_MODIFY</c> adds a second
///     member next to the first, and that is why these all assert the entry count as well as the value.
/// </summary>
public class patching_dictionary_keys: OneOffConfigurationsContext
{
    public enum Shade
    {
        Light = 1,
        Dark = 2
    }

    public class Inner
    {
        public Dictionary<string, string> Labels { get; set; } = new();
    }

    public class Doc
    {
        public Guid Id { get; set; }
        public Dictionary<int, string> ByNumber { get; set; } = new();
        public Dictionary<long, string> ByLong { get; set; } = new();
        public Dictionary<decimal, string> ByDecimal { get; set; } = new();
        public Dictionary<double, string> ByDouble { get; set; } = new();
        public Dictionary<bool, string> ByFlag { get; set; } = new();
        public Dictionary<Guid, string> ByGuid { get; set; } = new();
        public Dictionary<DateOnly, string> ByDate { get; set; } = new();
        public Dictionary<TimeOnly, string> ByTime { get; set; } = new();
        public Dictionary<DateTime, string> ByDateTime { get; set; } = new();
        public Dictionary<DateTimeOffset, string> ByOffset { get; set; } = new();
        public Dictionary<Shade, string> ByShade { get; set; } = new();
        public Dictionary<string, string> Labels { get; set; } = new();
        public Dictionary<string, int> Counts { get; set; } = new();
        public IDictionary<int, string> Interfaced { get; set; } = new Dictionary<int, string>();
        public Inner? Nested { get; set; }

        [JsonPropertyName("my-key")] public string? Hyphenated { get; set; }
        [JsonPropertyName("clé")] public string? NotAscii { get; set; }
        public string? Prénom { get; set; }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, EnumStorage enumStorage = EnumStorage.AsInteger,
        Action<JsonSerializerOptions>? configure = null)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.ConfigureSerialization(enumStorage, configure: configure);
        });
    }

    private async Task<Guid> storeAsync(Doc doc)
    {
        doc.Id = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Ct);
        return doc.Id;
    }

    private async Task patchAsync(Guid id, Action<IPatchExpression<Doc>> patch)
    {
        await using var session = theStore.LightweightSession();
        patch(session.Patch<Doc>(id));
        await session.SaveChangesAsync(Ct);
    }

    private async Task<Doc> loadAsync(Guid id)
    {
        await using var session = theStore.QuerySession();
        return (await session.LoadAsync<Doc>(id, Ct)).ShouldNotBeNull();
    }

    public static readonly TheoryData<bool, string> EveryShapeAndCulture = new()
    {
        { true, "en-US" },
        { false, "en-US" },
        { true, "th-TH" },
        { false, "th-TH" },
        { true, "sv-SE" },
        { false, "sv-SE" },
        { true, "ar-SA" },
        { false, "ar-SA" }
    };

    private async Task the_patch_finds_the_stored_key<TKey>(bool nativeJson, string culture, TKey key,
        Func<Doc, IDictionary<TKey, string>> dictionary, Expression<Func<Doc, string>> entry,
        EnumStorage enumStorage = EnumStorage.AsInteger) where TKey : notnull
    {
        ConfigureFor(nativeJson, enumStorage);
        using var scope = CultureScope.Formatting(culture);

        var seeded = new Doc();
        dictionary(seeded)[key] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(entry, "new"));

        var loaded = dictionary(await loadAsync(id));
        loaded[key].ShouldBe("new");
        loaded.Count.ShouldBe(1);
    }

    // ---- every kind of key -------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task an_int_key(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, 5, x => x.ByNumber, x => x.ByNumber[5]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_negative_int_key(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, -5, x => x.ByNumber, x => x.ByNumber[-5]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_key_through_an_interface(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, 5, x => x.Interfaced, x => x.Interfaced[5]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_long_key(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, long.MinValue, x => x.ByLong,
            x => x.ByLong[long.MinValue]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_decimal_key(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, 1.50m, x => x.ByDecimal, x => x.ByDecimal[1.50m]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_double_key_in_exponent_form(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, 1.5E+20, x => x.ByDouble, x => x.ByDouble[1.5E+20]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_bool_key(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, true, x => x.ByFlag, x => x.ByFlag[true]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_guid_key(bool nativeJson, string culture)
    {
        var key = Guid.NewGuid();
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByGuid, x => x.ByGuid[key]);
    }

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_date_key(bool nativeJson, string culture)
    {
        var key = new DateOnly(2026, 11, 2);
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByDate, x => x.ByDate[key]);
    }

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_time_key(bool nativeJson, string culture)
    {
        var key = new TimeOnly(3, 4, 5, 678);
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByTime, x => x.ByTime[key]);
    }

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_date_time_key(bool nativeJson, string culture)
    {
        var key = new DateTime(2026, 11, 2, 3, 4, 5, 678, DateTimeKind.Utc);
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByDateTime, x => x.ByDateTime[key]);
    }

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task a_date_time_offset_key(bool nativeJson, string culture)
    {
        var key = new DateTimeOffset(2026, 11, 2, 3, 4, 5, 678, TimeSpan.FromHours(-5));
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByOffset, x => x.ByOffset[key]);
    }

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task an_enum_key_is_written_by_name_even_where_the_value_is_an_integer(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, Shade.Dark, x => x.ByShade, x => x.ByShade[Shade.Dark]);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task an_enum_key_with_enums_stored_as_strings(bool nativeJson, string culture)
        => the_patch_finds_the_stored_key(nativeJson, culture, Shade.Dark, x => x.ByShade, x => x.ByShade[Shade.Dark],
            EnumStorage.AsString);

    [Theory]
    [MemberData(nameof(EveryShapeAndCulture))]
    public Task an_enum_key_with_no_name(bool nativeJson, string culture)
    {
        var key = (Shade)99;
        return the_patch_finds_the_stored_key(nativeJson, culture, key, x => x.ByShade, x => x.ByShade[key]);
    }

    // ---- every kind of string key --------------------------------------------------------------------

    public static readonly TheoryData<bool, string> AwkwardKeys = AwkwardKeysOnBothShapes();

    private static TheoryData<bool, string> AwkwardKeysOnBothShapes()
    {
        var data = new TheoryData<bool, string>();
        string[] keys =
        [
            "", "plain", "Upper", "_under", "a1", "5", "-5", "1.5", "a.b", "a..b", ".", "a b", " padded ", "a-b", "$x", "a$", "*",
            "[0]", "a[0]", "2026-11-02", "a'b", "'", "a\"b", "\"", "\"quoted\"", "a\\b", "\\", "a/b", "a+b", "a<b>c", "a&b", "`",
            "a\tb", "a\nb", "clé", "日本語", "🐈", "é1"
        ];

        foreach (var key in keys)
        {
            data.Add(true, key);
            data.Add(false, key);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task set_replaces_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels[key] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(x => x.Labels[key], "new"));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task set_adds_an_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Labels[key], "new"));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task append_replaces_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels[key] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Append(x => x.Labels, key, "new"));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task append_if_not_exists_finds_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels[key] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Labels, key, "new"));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("old");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task append_if_not_exists_adds_an_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Labels, key, "new"));

        (await loadAsync(id)).Labels[key].ShouldBe("new");
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task remove_takes_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels[key] = "old";
        seeded.Labels["kept"] = "kept";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Remove(x => x.Labels, key));

        (await loadAsync(id)).Labels.Keys.ShouldBe(["kept"]);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task delete_takes_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels[key] = "old";
        seeded.Labels["kept"] = "kept";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Delete(x => x.Labels[key]));

        (await loadAsync(id)).Labels.Keys.ShouldBe(["kept"]);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task increment_finds_the_entry_with_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Counts[key] = 5;

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Increment(x => x.Counts[key], 2));

        var counts = (await loadAsync(id)).Counts;
        counts[key].ShouldBe(7);
        counts.Count.ShouldBe(1);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task duplicate_reads_and_writes_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);
        var destination = key + " copy";

        var seeded = new Doc();
        seeded.Labels[key] = "value";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Duplicate(x => x.Labels[key], x => x.Labels[destination]));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("value");
        labels[destination].ShouldBe("value");
        labels.Count.ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(AwkwardKeys))]
    public async Task rename_writes_to_that_key(bool nativeJson, string key)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels["source"] = "value";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Rename("source", x => x.Labels[key]));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("value");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_keeps_a_string_past_the_limit_under_a_key_that_needs_quoting(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = new string('x', 5000);

        var seeded = new Doc();
        seeded.Labels["a.b"] = text;

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Duplicate(x => x.Labels["a.b"], x => x.Labels["c.d"]));

        (await loadAsync(id)).Labels["c.d"].ShouldBe(text);
    }

    // ---- what decides the spelling --------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_key_is_not_given_the_property_naming_policy(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc();
        seeded.Labels["Key"] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(x => x.Labels["Key"], "new"));

        var labels = (await loadAsync(id)).Labels;
        labels["Key"].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_key_is_given_the_dictionary_key_policy(bool nativeJson)
    {
        ConfigureFor(nativeJson, configure: o => o.DictionaryKeyPolicy = JsonNamingPolicy.KebabCaseLower);

        var seeded = new Doc();
        seeded.Labels["MyKey"] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(x => x.Labels["MyKey"], "new"));

        var labels = (await loadAsync(id)).Labels;
        labels["my-key"].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_gives_a_key_the_dictionary_key_policy(bool nativeJson)
    {
        ConfigureFor(nativeJson, configure: o => o.DictionaryKeyPolicy = JsonNamingPolicy.KebabCaseLower);

        var seeded = new Doc();
        seeded.Labels["MyKey"] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Append(x => x.Labels, "MyKey", "new"));

        var labels = (await loadAsync(id)).Labels;
        labels["my-key"].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(true, "clé")]
    [InlineData(false, "clé")]
    [InlineData(true, "a<b>'c\"d")]
    [InlineData(false, "a<b>'c\"d")]
    public async Task a_key_follows_the_encoder_the_serializer_was_given(bool nativeJson, string key)
    {
        // The other spelling of the same key: this encoder leaves it alone where the default escapes it
        ConfigureFor(nativeJson, configure: o => o.Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping);

        var seeded = new Doc();
        seeded.Labels[key] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(x => x.Labels[key], "new"));

        var labels = (await loadAsync(id)).Labels;
        labels[key].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_quoted_key_under_a_nested_member(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var seeded = new Doc { Nested = new Inner() };
        seeded.Nested.Labels["a.b"] = "old";

        var id = await storeAsync(seeded);
        await patchAsync(id, p => p.Set(x => x.Nested!.Labels["a.b"], "new"));

        var labels = (await loadAsync(id)).Nested!.Labels;
        labels["a.b"].ShouldBe("new");
        labels.Count.ShouldBe(1);
    }

    // ---- a member name is a key too ---------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_named_with_a_hyphen(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Hyphenated = "old" });
        await patchAsync(id, p => p.Set(x => x.Hyphenated, "new"));

        (await loadAsync(id)).Hyphenated.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_named_outside_ascii_with_an_attribute(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { NotAscii = "old" });
        await patchAsync(id, p => p.Set(x => x.NotAscii, "new"));

        (await loadAsync(id)).NotAscii.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_named_outside_ascii_in_the_code(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Prénom = "old" });
        await patchAsync(id, p => p.Set(x => x.Prénom, "new"));

        (await loadAsync(id)).Prénom.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_from_a_member_named_outside_ascii(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Prénom = "value" });
        await patchAsync(id, p => p.Rename("Prénom", x => x.NotAscii!));

        var doc = await loadAsync(id);
        doc.NotAscii.ShouldBe("value");
        doc.Prénom.ShouldBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_by_name_outside_ascii(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Prénom = "old" });
        await patchAsync(id, p => p.Set("Prénom", "new"));

        (await loadAsync(id)).Prénom.ShouldBe("new");
    }

    // ---- not a dictionary -------------------------------------------------------------------------------

    public class Listed
    {
        public Guid Id { get; set; }
        public List<string> Tags { get; set; } = [];
    }

    [Fact]
    public async Task a_list_index_is_refused_rather_than_read_as_a_key()
    {
        await using var session = theStore.LightweightSession();
        var patch = session.Patch<Listed>(Guid.NewGuid());

        Should.Throw<NotSupportedException>(() => patch.Set(x => x.Tags[0], "new"));
    }
}
