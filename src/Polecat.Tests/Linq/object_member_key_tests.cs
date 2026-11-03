using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: <c>JSON_VALUE</c> reads NULL for an object or an array, so ordering by one did nothing and
///     grouping by one put every row in one group, with no error. A member the serializer is known to
///     write that way is refused.
/// </summary>
/// <remarks>
///     The second half of this class is the point of the first. A class with a converter of its own can
///     be stored as a scalar, where all of this works, and the declared type cannot tell the two apart.
/// </remarks>
public class object_member_key_tests: OneOffConfigurationsContext
{
    public class KeyAddress
    {
        public string City { get; set; } = string.Empty;
    }

    public struct KeyPoint
    {
        public int X { get; set; }
    }

    /// <summary>A class, stored as a number by a converter it declares.</summary>
    [JsonConverter(typeof(AmountConverter))]
    public class Amount
    {
        public int Cents { get; set; }
    }

    /// <summary>A class stored as a string by a converter the type does not name.</summary>
    public class Sku
    {
        public string Code { get; set; } = string.Empty;
    }

    /// <summary>A class with nothing of its own, stored as a number where one member says so.</summary>
    public class Weight
    {
        public int Grams { get; set; }
    }

    public class AmountConverter: JsonConverter<Amount>
    {
        public override Amount Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => new() { Cents = reader.GetInt32() };

        public override void Write(Utf8JsonWriter writer, Amount value, JsonSerializerOptions options)
            => writer.WriteNumberValue(value.Cents);
    }

    public class SkuConverter: JsonConverter<Sku>
    {
        public override Sku Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => new() { Code = reader.GetString()! };

        public override void Write(Utf8JsonWriter writer, Sku value, JsonSerializerOptions options)
            => writer.WriteStringValue(value.Code);
    }

    public class WeightConverter: JsonConverter<Weight>
    {
        public override Weight Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => new() { Grams = reader.GetInt32() };

        public override void Write(Utf8JsonWriter writer, Weight value, JsonSerializerOptions options)
            => writer.WriteNumberValue(value.Grams);
    }

    public class KeyDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public KeyAddress? Address { get; set; }
        public KeyPoint? Point { get; set; }
        public KeyPoint Origin { get; set; }
        public List<string>? Tags { get; set; }
        public int[]? Numbers { get; set; }
        public Dictionary<string, int>? Map { get; set; }

        public Amount? Price { get; set; }
        public Sku? Code { get; set; }

        [JsonConverter(typeof(WeightConverter))]
        public Weight? Heft { get; set; }

        public object? Anything { get; set; }
        public Uri? Link { get; set; }
    }

    public record KeyEvent(string Label, KeyAddress? Address);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static KeyDoc row(string name, int value) => new()
    {
        Id = Guid.NewGuid(), Name = name, Address = new KeyAddress { City = name }, Tags = [name], Numbers = [value],
        Map = new Dictionary<string, int> { [name] = value },
        Price = new Amount { Cents = value }, Code = new Sku { Code = $"sku-{value}" },
        Heft = new Weight { Grams = value }, Anything = value, Link = new Uri($"https://example.com/{value}")
    };

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.ConfigureSerialization(configure: x => x.Converters.Add(new SkuConverter()));
        });

        await using var session = theStore.LightweightSession();
        session.Store(row("b", 2), row("a", 1), row("c", 3), row("a", 1));
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task refused(Func<Task> query, string member)
        => (await Should.ThrowAsync<BadLinqExpressionException>(query)).Message.ShouldContain(member);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ordering_by_an_object_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyDoc>();

        await refused(() => docs.OrderBy(x => x.Address).ToListAsync(Ct), "Address");
        await refused(() => docs.OrderByDescending(x => x.Address).ToListAsync(Ct), "Address");
        await refused(() => docs.OrderBy(x => x.Name).ThenBy(x => x.Address).ToListAsync(Ct), "Address");
        await refused(() => docs.OrderBy(x => x.Name).ThenByDescending(x => x.Address).ToListAsync(Ct), "Address");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task every_shape_of_object_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyDoc>();

        await refused(() => docs.OrderBy(x => x.Point).ToListAsync(Ct), "Point");
        await refused(() => docs.OrderBy(x => x.Origin).ToListAsync(Ct), "Origin");
        await refused(() => docs.OrderBy(x => x.Tags).ToListAsync(Ct), "Tags");
        await refused(() => docs.OrderBy(x => x.Numbers).ToListAsync(Ct), "Numbers");
        await refused(() => docs.OrderBy(x => x.Map).ToListAsync(Ct), "Map");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task grouping_by_an_object_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var docs = query.Query<KeyDoc>();

        await refused(() => docs.GroupBy(x => x.Address).Select(g => g.Count()).ToListAsync(Ct), "Address");
        await refused(() => docs.GroupBy(x => x.Tags).Select(g => g.Count()).ToListAsync(Ct), "Tags");
        await refused(() => docs.GroupBy(x => new { x.Name, x.Address }).Select(g => g.Count()).ToListAsync(Ct),
            "Address");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task taking_one_row_per_object_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        await refused(() => query.Query<KeyDoc>().DistinctBy(x => x.Address).ToListAsync(Ct), "Address");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_object_a_select_projected_is_refused_after_it(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        await refused(() => query.Query<KeyDoc>().Select(x => x.Address).OrderBy(a => a).ToListAsync(Ct), "Address");
        await refused(() => query.Query<KeyDoc>().Select(x => new { Place = x.Address }).OrderBy(y => y.Place)
            .ToListAsync(Ct), "Address");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_of_an_object_still_works(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var names = await query.Query<KeyDoc>().OrderBy(x => x.Address!.City).ToListAsync(Ct);
        names.Select(x => x.Name).ShouldBe(["a", "a", "b", "c"]);

        (await query.Query<KeyDoc>().GroupBy(x => x.Address!.City).Select(g => g.Count()).ToListAsync(Ct))
            .OrderBy(x => x).ShouldBe([1, 1, 2]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_class_whose_type_declares_a_converter_is_ordered_by_what_it_stores(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ordered = await query.Query<KeyDoc>().OrderByDescending(x => x.Price).ToListAsync(Ct);

        ordered.Select(x => x.Price!.Cents).ShouldBe([3, 2, 1, 1]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_class_with_a_registered_converter_is_ordered_by_what_it_stores(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ordered = await query.Query<KeyDoc>().OrderBy(x => x.Code).ToListAsync(Ct);

        ordered.Select(x => x.Code!.Code).ShouldBe(["sku-1", "sku-1", "sku-2", "sku-3"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_class_with_a_converter_on_the_member_is_ordered_by_what_it_stores(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ordered = await query.Query<KeyDoc>().OrderByDescending(x => x.Heft).ToListAsync(Ct);

        ordered.Select(x => x.Heft!.Grams).ShouldBe([3, 2, 1, 1]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_class_with_a_converter_groups_by_what_it_stores(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<KeyDoc>().GroupBy(x => x.Price).Select(g => g.Count()).ToListAsync(Ct))
            .OrderBy(x => x).ShouldBe([1, 1, 2]);
        (await query.Query<KeyDoc>().GroupBy(x => x.Code).Select(g => g.Count()).ToListAsync(Ct))
            .OrderBy(x => x).ShouldBe([1, 1, 2]);
        (await query.Query<KeyDoc>().GroupBy(x => x.Heft).Select(g => g.Count()).ToListAsync(Ct))
            .OrderBy(x => x).ShouldBe([1, 1, 2]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_typed_object_is_ordered_by_the_scalar_it_holds(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ordered = await query.Query<KeyDoc>().OrderBy(x => x.Anything).ToListAsync(Ct);

        ordered.Select(x => x.Name).ShouldBe(["a", "a", "b", "c"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_framework_class_stored_as_text_is_ordered_by_it(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ordered = await query.Query<KeyDoc>().OrderBy(x => x.Link).ToListAsync(Ct);

        ordered.Select(x => x.Name).ShouldBe(["a", "a", "b", "c"]);
    }

    [Fact]
    public async Task an_event_body_is_held_to_the_same_rule()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new KeyEvent("one", new KeyAddress { City = "Oslo" }));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        await refused(() => query.Events.QueryRawEventDataOnly<KeyEvent>().OrderBy(x => x.Address).ToListAsync(Ct),
            "Address");
    }
}
