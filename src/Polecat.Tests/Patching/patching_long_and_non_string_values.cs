using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using JasperFx.Events;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Patching;

/// <summary>
///     #876. A patch that reads a value back out of the document has to hand it back as the JSON it
///     was. Reading a member through <c>JSON_VALUE</c> only does that for a string under 4000
///     characters: past that it answers NULL, and on anything that is not a string it answers either
///     NULL or the value's text, which <c>JSON_MODIFY</c> then stores as a string.
/// </summary>
/// <remarks>
///     Every fact runs on both storage shapes, because the read that works on one is a syntax error on
///     the other. See <c>SqlText.UnlimitedJsonString</c>.
/// </remarks>
public class patching_long_and_non_string_values: OneOffConfigurationsContext
{
    public record Child(string Name, int Size);

    public enum Shade
    {
        Light = 1,
        Dark = 2
    }

    public class Nested
    {
        public string? Text { get; set; }
        public string? OtherText { get; set; }
    }

    public class Doc
    {
        public Guid Id { get; set; }
        public string? Color { get; set; }

        public string? Text { get; set; }
        public string? OtherText { get; set; }
        public string? ThirdText { get; set; }
        public Nested? Inner { get; set; }

        public int Number { get; set; }
        public int OtherNumber { get; set; }
        public int ThirdNumber { get; set; }
        public long Long { get; set; }
        public long OtherLong { get; set; }
        public decimal Decimal { get; set; }
        public decimal OtherDecimal { get; set; }
        public double Double { get; set; }
        public double OtherDouble { get; set; }
        public bool Flag { get; set; }
        public bool OtherFlag { get; set; }
        public bool? Maybe { get; set; }
        public bool? OtherMaybe { get; set; }
        public Shade Tone { get; set; }
        public Shade OtherTone { get; set; }

        public Guid? Key { get; set; }
        public Guid? OtherKey { get; set; }
        public DateTimeOffset? When { get; set; }
        public DateTimeOffset? OtherWhen { get; set; }

        // Not scalars to PatchOperation.IsScalarType, and each one is stored as a JSON string.
        public TimeSpan? Span { get; set; }
        public TimeSpan? OtherSpan { get; set; }
        public Uri? Link { get; set; }
        public Uri? OtherLink { get; set; }
        public byte[]? Bytes { get; set; }
        public byte[]? OtherBytes { get; set; }

        // What is stored here is only known at runtime.
        public object? Anything { get; set; }
        public object? OtherAnything { get; set; }

        public Child? Kid { get; set; }
        public Child? OtherKid { get; set; }
        public int[]? Numbers { get; set; }
        public int[]? OtherNumbers { get; set; }
        public string[]? Names { get; set; }
        public string[]? OtherNames { get; set; }
        public Dictionary<string, int>? Map { get; set; }
        public Dictionary<string, int>? OtherMap { get; set; }

        public List<string?> Tags { get; set; } = [];
        public List<Child?> Kids { get; set; } = [];
        public List<bool> Flags { get; set; } = [];
        public List<double> Amounts { get; set; } = [];
        public List<Guid> Keys { get; set; } = [];
        public List<DateTimeOffset> Times { get; set; } = [];
        public List<Shade> Tones { get; set; } = [];
        public Dictionary<string, string> Labels { get; set; } = new();
        public Dictionary<string, object> Bag { get; set; } = new();
        public Dictionary<string, int> Counts { get; set; } = new();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, EnumStorage enumStorage = EnumStorage.AsInteger)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.ConfigureSerialization(enumStorage);
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

    private static string Long(int length) => new string('x', length - 3) + "end";

    // ---- the string past the limit --------------------------------------------------------------

    [Theory]
    [InlineData(true, 4000)]
    [InlineData(false, 4000)]
    [InlineData(true, 4001)]
    [InlineData(false, 4001)]
    [InlineData(true, 100_000)]
    [InlineData(false, 100_000)]
    public async Task rename_keeps_a_string_of_any_length(bool nativeJson, int length)
    {
        ConfigureFor(nativeJson);
        var text = Long(length);

        var id = await storeAsync(new Doc { Text = text });
        await patchAsync(id, p => p.Rename("Text", x => x.OtherText!));

        var doc = await loadAsync(id);
        doc.OtherText.ShouldBe(text);
        doc.Text.ShouldBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_keeps_a_long_string_on_a_nested_member(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Inner = new Nested { Text = text } });
        await patchAsync(id, p => p.Rename("Text", x => x.Inner!.OtherText!));

        var doc = await loadAsync(id);
        doc.Inner!.OtherText.ShouldBe(text);
        doc.Inner.Text.ShouldBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_keeps_a_long_string_that_needs_escaping(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000) + "\"quoted\" back\\slash\nnew line é 🐈";

        var id = await storeAsync(new Doc { Text = text });
        await patchAsync(id, p => p.Rename("Text", x => x.OtherText!));

        (await loadAsync(id)).OtherText.ShouldBe(text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_keeps_a_long_string_on_every_destination(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Text = text });
        await patchAsync(id, p => p.Duplicate(x => x.Text, x => x.OtherText, x => x.ThirdText));

        var doc = await loadAsync(id);
        doc.Text.ShouldBe(text);
        doc.OtherText.ShouldBe(text);
        doc.ThirdText.ShouldBe(text);
    }

    // ---- is the key there? -----------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_keeps_a_long_string_of_the_same_type_on_the_key(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var doc = new Doc();
        doc.Labels["key"] = text;

        var id = await storeAsync(doc);
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Labels, "key", "replacement"));

        (await loadAsync(id)).Labels["key"].ShouldBe(text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_does_not_overwrite_a_long_string_with_an_object(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var doc = new Doc();
        doc.Bag["key"] = text;

        var id = await storeAsync(doc);
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Bag, "key", new Child("replacement", 1)));

        var value = (JsonElement)(await loadAsync(id)).Bag["key"];
        value.ValueKind.ShouldBe(JsonValueKind.String);
        value.GetString().ShouldBe(text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_does_not_overwrite_what_is_already_there(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var doc = new Doc();
        doc.Bag["text"] = "short";
        doc.Bag["number"] = 5;
        doc.Bag["flag"] = true;
        doc.Bag["object"] = new Child("existing", 2);
        doc.Bag["array"] = new[] { 1, 2 };

        var id = await storeAsync(doc);
        await patchAsync(id, p =>
        {
            foreach (var key in doc.Bag.Keys)
            {
                p.AppendIfNotExists(x => x.Bag, key, new Child("replacement", 1));
            }
        });

        var bag = (await loadAsync(id)).Bag;
        ((JsonElement)bag["text"]).GetRawText().ShouldBe("\"short\"");
        ((JsonElement)bag["number"]).GetRawText().ShouldBe("5");
        ((JsonElement)bag["flag"]).GetRawText().ShouldBe("true");
        ((JsonElement)bag["object"]).GetProperty("name").GetString().ShouldBe("existing");
        ((JsonElement)bag["array"]).GetArrayLength().ShouldBe(2);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_still_adds_an_absent_key(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Bag, "key", new Child("added", 1)));

        ((JsonElement)(await loadAsync(id)).Bag["key"]).GetProperty("name").GetString().ShouldBe("added");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_adds_a_string_to_an_absent_key(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Labels, "key", text));

        (await loadAsync(id)).Labels["key"].ShouldBe(text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_adds_a_number_to_an_absent_key(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Counts, "key", 5));

        (await loadAsync(id)).Counts["key"].ShouldBe(5);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_writes_a_string_to_a_dictionary(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.Labels, "key", text));

        (await loadAsync(id)).Labels["key"].ShouldBe(text);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_writes_a_number_to_a_dictionary(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.Counts, "key", 5));

        (await loadAsync(id)).Counts["key"].ShouldBe(5);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_writes_a_string_held_in_an_object_to_a_dictionary(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.Bag, "key", (object)"text"));

        ((JsonElement)(await loadAsync(id)).Bag["key"]).GetString().ShouldBe("text");
    }

    // ---- every kind of value ---------------------------------------------------------------------

    /// <summary>A pair of members of one type, and a value to move or copy from one to the other.</summary>
    public class Sample(string name, Action<Doc> seed, Func<IPatchExpression<Doc>, IPatchExpression<Doc>> rename,
        Func<IPatchExpression<Doc>, IPatchExpression<Doc>> duplicate, Action<Doc> sourceIsGone, Action<Doc> sourceIsKept,
        Action<Doc> destinationHasIt)
    {
        public void Seed(Doc doc) => seed(doc);
        public void Rename(IPatchExpression<Doc> patch) => rename(patch);
        public void Duplicate(IPatchExpression<Doc> patch) => duplicate(patch);
        public void SourceIsGone(Doc doc) => sourceIsGone(doc);
        public void SourceIsKept(Doc doc) => sourceIsKept(doc);
        public void DestinationHasIt(Doc doc) => destinationHasIt(doc);
        public override string ToString() => name;
    }

    private static Sample Of<TValue>(string name, Expression<Func<Doc, TValue>> source,
        Expression<Func<Doc, TValue>> destination, TValue value, Action<TValue?, TValue>? same = null)
    {
        same ??= (actual, expected) => actual.ShouldBe(expected);

        var sourceMember = (PropertyInfo)((MemberExpression)source.Body).Member;
        var read = source.Compile();
        var readDestination = destination.Compile();

        var boxedDestination = Expression.Lambda<Func<Doc, object>>(
            Expression.Convert(destination.Body, typeof(object)), destination.Parameters);

        return new Sample(
            name,
            doc => sourceMember.SetValue(doc, value),
            patch => patch.Rename(sourceMember.Name, boxedDestination),
            patch => patch.Duplicate(source, destination),
            doc => read(doc).ShouldBe(default),
            doc => same(read(doc), value),
            doc => same(readDestination(doc), value));
    }

    private static void SameJson(object? actual, object? expected)
    {
        var element = actual.ShouldBeOfType<JsonElement>();
        element.GetRawText().ShouldBe(JsonSerializer.Serialize(expected,
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
    }

    private static readonly Sample[] Samples =
    [
        Of("int", x => x.Number, x => x.OtherNumber, 5),
        Of("negative int", x => x.Number, x => x.OtherNumber, -5),
        Of("long past double precision", x => x.Long, x => x.OtherLong, long.MaxValue),
        Of("decimal with trailing zero", x => x.Decimal, x => x.OtherDecimal, 1.10m),
        Of("decimal at full precision", x => x.Decimal, x => x.OtherDecimal, 7922816251426433759354395.0335m),
        Of("double at full precision", x => x.Double, x => x.OtherDouble, 0.1 + 0.2),
        Of("double with an exponent", x => x.Double, x => x.OtherDouble, 1.5E+20),
        Of("true", x => x.Flag, x => x.OtherFlag, true),
        Of("false", x => x.Maybe, x => x.OtherMaybe, false),
        Of("enum", x => x.Tone, x => x.OtherTone, Shade.Dark),

        Of("guid", x => x.Key, x => x.OtherKey, Guid.Parse("7e2f5bd6-12f3-4a3c-9d2e-5a6b7c8d9e0f")),
        Of("date", x => x.When, x => x.OtherWhen, new DateTimeOffset(2026, 11, 1, 12, 30, 15, TimeSpan.FromHours(-5))),

        Of("timespan", x => x.Span, x => x.OtherSpan, TimeSpan.FromMinutes(90)),
        Of("uri", x => x.Link, x => x.OtherLink, new Uri("https://example.com/a")),
        Of("byte array", x => x.Bytes, x => x.OtherBytes, new byte[] { 1, 2, 3 }),

        Of("object", x => x.Kid, x => x.OtherKid, new Child("kid", 3)),
        Of("array", x => x.Numbers, x => x.OtherNumbers, new[] { 1, 2, 3 }),
        Of("string array", x => x.Names, x => x.OtherNames, new[] { "a", "b" }),
        Of("empty array", x => x.Numbers, x => x.OtherNumbers, Array.Empty<int>()),
        Of("dictionary", x => x.Map, x => x.OtherMap, new Dictionary<string, int> { ["a"] = 1 }),
        Of("empty dictionary", x => x.Map, x => x.OtherMap, new Dictionary<string, int>()),

        Of<object?>("a string in an object member", x => x.Anything, x => x.OtherAnything, "text", SameJson),
        Of<object?>("a number in an object member", x => x.Anything, x => x.OtherAnything, 5, SameJson),
        Of<object?>("a bool in an object member", x => x.Anything, x => x.OtherAnything, true, SameJson),
        Of<object?>("an object in an object member", x => x.Anything, x => x.OtherAnything, new Child("kid", 3), SameJson),
        Of<object?>("an array in an object member", x => x.Anything, x => x.OtherAnything, new[] { 1, 2 }, SameJson)
    ];

    public static TheoryData<string, bool> EveryKindOnBothStorageShapes()
    {
        var data = new TheoryData<string, bool>();
        foreach (var sample in Samples)
        {
            data.Add(sample.ToString(), true);
            data.Add(sample.ToString(), false);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EveryKindOnBothStorageShapes))]
    public async Task rename_keeps_every_kind_of_value(string kind, bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var sample = Samples.Single(x => x.ToString() == kind);

        var seeded = new Doc();
        sample.Seed(seeded);

        var id = await storeAsync(seeded);
        await patchAsync(id, sample.Rename);

        var doc = await loadAsync(id);
        sample.DestinationHasIt(doc);
        sample.SourceIsGone(doc);
    }

    [Theory]
    [MemberData(nameof(EveryKindOnBothStorageShapes))]
    public async Task duplicate_keeps_every_kind_of_value(string kind, bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var sample = Samples.Single(x => x.ToString() == kind);

        var seeded = new Doc();
        sample.Seed(seeded);

        var id = await storeAsync(seeded);
        await patchAsync(id, sample.Duplicate);

        var doc = await loadAsync(id);
        sample.DestinationHasIt(doc);
        sample.SourceIsKept(doc);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_writes_a_number_to_every_destination(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Number = 5 });
        await patchAsync(id, p => p.Duplicate(x => x.Number, x => x.OtherNumber, x => x.ThirdNumber));

        var doc = await loadAsync(id);
        doc.Number.ShouldBe(5);
        doc.OtherNumber.ShouldBe(5);
        doc.ThirdNumber.ShouldBe(5);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_keeps_the_scale_of_a_decimal(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Decimal = 1.10m });
        await patchAsync(id, p => p.Rename("Decimal", x => x.OtherDecimal));

        (await loadAsync(id)).OtherDecimal.ToString(CultureInfo.InvariantCulture).ShouldBe("1.10");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_keeps_an_enum_stored_as_a_string(bool nativeJson)
    {
        ConfigureFor(nativeJson, EnumStorage.AsString);

        var id = await storeAsync(new Doc { Tone = Shade.Dark });
        await patchAsync(id, p => p.Rename("Tone", x => x.OtherTone));

        (await loadAsync(id)).OtherTone.ShouldBe(Shade.Dark);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_writes_a_type_the_serializer_stores_as_a_string(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var link = new Uri("https://example.com/a");

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Span, TimeSpan.FromMinutes(90)).Set(x => x.Link, link));

        var doc = await loadAsync(id);
        doc.Span.ShouldBe(TimeSpan.FromMinutes(90));
        doc.Link.ShouldBe(link);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_writes_whatever_an_object_member_is_given(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p
            .Set<object?>(x => x.Anything, "text")
            .Set<object?>(x => x.OtherAnything, new Child("kid", 3)));

        var doc = await loadAsync(id);
        ((JsonElement)doc.Anything!).GetString().ShouldBe("text");
        ((JsonElement)doc.OtherAnything!).GetProperty("name").GetString().ShouldBe("kid");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_by_a_where_clause_reads_the_kind_from_each_document(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var text = await storeAsync(new Doc { Color = "patch me", Anything = Long(5000) });
        var number = await storeAsync(new Doc { Color = "patch me", Anything = 5 });
        var kid = await storeAsync(new Doc { Color = "patch me", Anything = new Child("kid", 3) });

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Doc>(x => x.Color == "patch me").Rename("Anything", x => x.OtherAnything!);
            await session.SaveChangesAsync(Ct);
        }

        SameJson((await loadAsync(text)).OtherAnything, Long(5000));
        SameJson((await loadAsync(number)).OtherAnything, 5);
        SameJson((await loadAsync(kid)).OtherAnything, new Child("kid", 3));
    }

    // ---- nothing to read -------------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task rename_of_a_member_that_is_not_there_leaves_the_destination_alone(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        // The second run of a migration, or a document already in the new shape.
        var id = await storeAsync(new Doc { OtherText = "already renamed" });
        await patchAsync(id, p => p.Rename("Text", x => x.OtherText!));

        (await loadAsync(id)).OtherText.ShouldBe("already renamed");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_of_a_null_member_clears_the_destination(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        // The serializer leaves a null member out, so absent IS null here and copying it is the point.
        var id = await storeAsync(new Doc { OtherText = "stale" });
        await patchAsync(id, p => p.Duplicate(x => x.Text, x => x.OtherText));

        (await loadAsync(id)).OtherText.ShouldBeNull();
    }

    // ---- elements written back through OPENJSON ---------------------------------------------------

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_leaves_the_other_strings_as_strings(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["a", "b", "c"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "b", action));

        (await loadAsync(id)).Tags.ShouldBe(["a", "c"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_does_not_turn_a_string_that_looks_like_json_into_json(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["1", "true", "null", "[2]", "remove me"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "remove me", action));

        (await loadAsync(id)).Tags.ShouldBe(["1", "true", "null", "[2]"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_keeps_a_long_string_and_one_that_needs_escaping(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);
        const string awkward = "\"quoted\" back\\slash for/ward\nnew line";

        var id = await storeAsync(new Doc { Tags = [text, awkward, "remove me"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "remove me", action));

        (await loadAsync(id)).Tags.ShouldBe([text, awkward]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_a_long_string(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Tags = ["a", text] });
        await patchAsync(id, p => p.Remove(x => x.Tags, text, action));

        (await loadAsync(id)).Tags.ShouldBe(["a"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_of_a_string_that_is_not_there_leaves_the_array_alone(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["a", "b"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "not there", action));

        (await loadAsync(id)).Tags.ShouldBe(["a", "b"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_of_a_number_that_is_not_there_leaves_the_array_alone(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Numbers = [1, 2] });
        await patchAsync(id, p => p.Remove(x => x.Numbers!, 3, action));

        (await loadAsync(id)).Numbers.ShouldBe([1, 2]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_of_an_object_that_is_not_there_leaves_the_array_alone(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("kid", 1);

        var id = await storeAsync(new Doc { Kids = [kid] });
        await patchAsync(id, p => p.Remove(x => x.Kids, new Child("not there", 2), action));

        (await loadAsync(id)).Kids.ShouldBe([kid]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_keeps_a_null_string_element(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["a", null, "b"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "a", action));

        (await loadAsync(id)).Tags.ShouldBe([null, "b"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_keeps_a_null_object_element(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("kid", 1);

        var id = await storeAsync(new Doc { Kids = [kid, null, new Child("other", 2)] });
        await patchAsync(id, p => p.Remove(x => x.Kids, kid, action));

        (await loadAsync(id)).Kids.ShouldBe([null, new Child("other", 2)]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_at_an_index_keeps_the_strings_already_there(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);
        const string awkward = "\"quoted\" back\\slash for/ward\nnew line";

        var id = await storeAsync(new Doc { Tags = ["1", null, text, awkward] });
        await patchAsync(id, p => p.Insert(x => x.Tags, "new", 1));

        (await loadAsync(id)).Tags.ShouldBe(["1", "new", null, text, awkward]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_a_long_string_at_an_index(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Tags = ["a", "b"] });
        await patchAsync(id, p => p.Insert(x => x.Tags, text, 1));

        (await loadAsync(id)).Tags.ShouldBe(["a", text, "b"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_at_an_index_keeps_a_null_object_element(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("kid", 1);
        var added = new Child("added", 2);

        var id = await storeAsync(new Doc { Kids = [kid, null] });
        await patchAsync(id, p => p.Insert(x => x.Kids, added, 1));

        (await loadAsync(id)).Kids.ShouldBe([kid, added, null]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_if_not_exists_at_an_index_keeps_the_strings_already_there(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Tags = ["1", null, text] });
        await patchAsync(id, p => p.InsertIfNotExists(x => x.Tags, "new", 1));

        (await loadAsync(id)).Tags.ShouldBe(["1", "new", null, text]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_if_not_exists_of_an_object_at_an_index_keeps_a_null_element(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("kid", 1);
        var added = new Child("added", 2);

        var id = await storeAsync(new Doc { Kids = [kid, null] });
        await patchAsync(id, p => p.InsertIfNotExists(x => x.Kids, added, 1));

        (await loadAsync(id)).Kids.ShouldBe([kid, added, null]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task if_not_exists_finds_a_long_string_in_an_array(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var text = Long(5000);

        var id = await storeAsync(new Doc { Tags = [text] });
        await patchAsync(id, p =>
        {
            p.AppendIfNotExists(x => x.Tags, text);
            p.InsertIfNotExists(x => x.Tags, text);
            p.InsertIfNotExists(x => x.Tags, text, 0);
        });

        (await loadAsync(id)).Tags.ShouldBe([text]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task if_not_exists_finds_a_long_object_in_an_array(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var kid = new Child(Long(5000), 1);

        var id = await storeAsync(new Doc { Kids = [kid] });
        await patchAsync(id, p =>
        {
            p.AppendIfNotExists(x => x.Kids, kid);
            p.InsertIfNotExists(x => x.Kids, kid);
        });

        (await loadAsync(id)).Kids.ShouldBe([kid]);
    }

    // ---- finding an element that isn't a string -----------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_finds_a_bool(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Flags = [true] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Flags, true));

        (await loadAsync(id)).Flags.ShouldBe([true]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_still_adds_a_bool_that_is_not_there(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Flags = [true] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Flags, false));

        (await loadAsync(id)).Flags.ShouldBe([true, false]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_if_not_exists_finds_a_bool(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Flags = [true] });
        await patchAsync(id, p =>
        {
            p.InsertIfNotExists(x => x.Flags, true);
            p.InsertIfNotExists(x => x.Flags, true, 0);
        });

        (await loadAsync(id)).Flags.ShouldBe([true]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_finds_a_double(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        const double amount = 0.1 + 0.2;

        var id = await storeAsync(new Doc { Amounts = [amount] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Amounts, amount));

        (await loadAsync(id)).Amounts.ShouldBe([amount]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_a_bool(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Flags = [true, false] });
        await patchAsync(id, p => p.Remove(x => x.Flags, true, action));

        (await loadAsync(id)).Flags.ShouldBe([false]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_a_double(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        const double amount = 0.1 + 0.2;

        var id = await storeAsync(new Doc { Amounts = [1.5, amount] });
        await patchAsync(id, p => p.Remove(x => x.Amounts, amount, action));

        (await loadAsync(id)).Amounts.ShouldBe([1.5]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_a_guid(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var key = Guid.NewGuid();
        var other = Guid.NewGuid();

        var id = await storeAsync(new Doc { Keys = [other, key] });
        await patchAsync(id, p => p.Remove(x => x.Keys, key, action));

        (await loadAsync(id)).Keys.ShouldBe([other]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_a_date(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var when = new DateTimeOffset(2026, 11, 1, 12, 30, 15, TimeSpan.FromHours(-5));
        var other = when.AddDays(1);

        var id = await storeAsync(new Doc { Times = [other, when] });
        await patchAsync(id, p => p.Remove(x => x.Times, when, action));

        (await loadAsync(id)).Times.ShouldBe([other]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_an_enum_stored_as_a_string(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson, EnumStorage.AsString);

        var id = await storeAsync(new Doc { Tones = [Shade.Light, Shade.Dark] });
        await patchAsync(id, p => p.Remove(x => x.Tones, Shade.Dark, action));

        (await loadAsync(id)).Tones.ShouldBe([Shade.Light]);
    }

    // ---- increment reads the number it writes back ------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_of_a_member_that_is_not_there_starts_from_zero(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Delete(x => x.Number).Increment(x => x.Number, 3));

        (await loadAsync(id)).Number.ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_of_a_dictionary_key_that_is_not_there_starts_from_zero(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Increment(x => x.Counts["new"], 3));

        (await loadAsync(id)).Counts["new"].ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_of_a_decimal_keeps_its_scale(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Decimal = 1.1234567m });
        await patchAsync(id, p => p.Increment(x => x.Decimal, 1m));

        (await loadAsync(id)).Decimal.ShouldBe(2.1234567m);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_of_a_large_decimal(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Decimal = 12345678901234.5m });
        await patchAsync(id, p => p.Increment(x => x.Decimal, 1m));

        (await loadAsync(id)).Decimal.ShouldBe(12345678901235.5m);
    }
}
