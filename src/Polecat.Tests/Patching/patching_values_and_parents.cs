using System.Text.Json;
using System.Text.Json.Serialization;
using JasperFx.Events;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Patching;

/// <summary>
///     #880. What <c>JSON_MODIFY</c> is handed for a value, and where it can put it. A CLR value bound
///     as a parameter is only right for a few types: a Guid or a DateTimeOffset is refused, and a
///     double comes out changed. A path whose parent is absent is no error at all, the update just
///     does nothing.
/// </summary>
public class patching_values_and_parents: OneOffConfigurationsContext
{
    public record Child(string Name, int Size);

    public enum Shade
    {
        Light = 1,
        Dark = 2
    }

    public class Deep
    {
        public string? Text { get; set; }
        public string? OtherText { get; set; }
        public int Number { get; set; }
        public double Amount { get; set; }
        public Child? Kid { get; set; }
        public List<string>? Tags { get; set; }
        public Dictionary<string, string>? Labels { get; set; }
    }

    public class Middle
    {
        public string? Sibling { get; set; }
        public Deep? Deep { get; set; }
    }

    public class Doc
    {
        public Guid Id { get; set; }

        public int Number { get; set; }
        public int? MaybeNumber { get; set; }
        public double Double { get; set; }
        public float Single { get; set; }
        public decimal Decimal { get; set; }
        public long Long { get; set; }
        public ulong Unsigned { get; set; }
        public uint SmallUnsigned { get; set; }
        public short Short { get; set; }
        public byte Byte { get; set; }
        public sbyte SignedByte { get; set; }
        public Int128 Huge { get; set; }
        public Half HalfSize { get; set; }
        public object? Anything { get; set; }
        public string? Text { get; set; }

        public Middle? Middle { get; set; }

        public List<Guid> Guids { get; set; } = [];
        public List<DateTimeOffset> Offsets { get; set; } = [];
        public List<DateTime> Dates { get; set; } = [];
        public List<DateOnly> Days { get; set; } = [];
        public List<TimeOnly> Times { get; set; } = [];
        public List<TimeSpan> Spans { get; set; } = [];
        public List<Uri> Links { get; set; } = [];
        public List<Shade> Shades { get; set; } = [];
        public List<double> Doubles { get; set; } = [];
        public List<float> Singles { get; set; } = [];
        public List<decimal> Decimals { get; set; } = [];
        public List<long> Longs { get; set; } = [];
        public List<bool> Flags { get; set; } = [];
        public List<string> Tags { get; set; } = [];
        public List<object> Things { get; set; } = [];
        public List<Child> Kids { get; set; } = [];
        public Dictionary<string, Guid> GuidByName { get; set; } = new();
        public Dictionary<string, double> DoubleByName { get; set; } = new();
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

    // ---- every way of adding an element ------------------------------------------------------------

    public enum Adding
    {
        Append,
        AppendIfNotExists,
        InsertAtTheEnd,
        InsertAtAnIndex,
        InsertIfNotExists,
        InsertIfNotExistsAtAnIndex
    }

    private static readonly DateTimeOffset AnOffset = new(2026, 11, 2, 3, 4, 5, 678, TimeSpan.FromHours(-5));

    private static void Add<TElement>(IPatchExpression<Doc> patch, Adding how,
        System.Linq.Expressions.Expression<Func<Doc, IEnumerable<TElement>>> list, TElement element)
    {
        switch (how)
        {
            case Adding.Append:
                patch.Append(list, element);
                break;
            case Adding.AppendIfNotExists:
                patch.AppendIfNotExists(list, element);
                break;
            case Adding.InsertAtTheEnd:
                patch.Insert(list, element);
                break;
            case Adding.InsertAtAnIndex:
                patch.Insert(list, element, 1);
                break;
            case Adding.InsertIfNotExists:
                patch.InsertIfNotExists(list, element);
                break;
            case Adding.InsertIfNotExistsAtAnIndex:
                patch.InsertIfNotExists(list, element, 1);
                break;
        }
    }

    public static TheoryData<bool, Adding> EveryWayOfAdding()
    {
        var data = new TheoryData<bool, Adding>();
        foreach (var how in Enum.GetValues<Adding>())
        {
            data.Add(true, how);
            data.Add(false, how);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_guid(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var key = Guid.NewGuid();

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Guids, key));

        (await loadAsync(id)).Guids.ShouldBe([key]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_date_time_offset(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Offsets, AnOffset));

        (await loadAsync(id)).Offsets.ShouldBe([AnOffset]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_date_time(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var when = new DateTime(2026, 11, 2, 3, 4, 5, 678, DateTimeKind.Utc);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Dates, when));

        (await loadAsync(id)).Dates.ShouldBe([when]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_date_only(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var day = new DateOnly(2026, 11, 2);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Days, day));

        (await loadAsync(id)).Days.ShouldBe([day]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_time_only(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var time = new TimeOnly(3, 4, 5);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Times, time));

        (await loadAsync(id)).Times.ShouldBe([time]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_time_span(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var span = TimeSpan.FromMinutes(90);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Spans, span));

        (await loadAsync(id)).Spans.ShouldBe([span]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_uri(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var link = new Uri("https://example.com/a");

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Links, link));

        (await loadAsync(id)).Links.ShouldBe([link]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_an_enum_as_an_integer(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Shades, Shade.Dark));

        (await loadAsync(id)).Shades.ShouldBe([Shade.Dark]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_an_enum_as_a_string(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson, EnumStorage.AsString);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Shades, Shade.Dark));

        (await loadAsync(id)).Shades.ShouldBe([Shade.Dark]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_double_at_full_precision(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        const double amount = 0.1 + 0.2;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Doubles, amount));

        (await loadAsync(id)).Doubles.ShouldBe([amount]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_double_with_an_exponent(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        const double amount = 1.2345678901234567E-05;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Doubles, amount));

        (await loadAsync(id)).Doubles.ShouldBe([amount]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_float(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        const float amount = 0.1f;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Singles, amount));

        (await loadAsync(id)).Singles.ShouldBe([amount]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_decimal(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Decimals, 7922816251426433759354395.0335m));

        (await loadAsync(id)).Decimals.ShouldBe([7922816251426433759354395.0335m]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_long(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Longs, long.MaxValue));

        (await loadAsync(id)).Longs.ShouldBe([long.MaxValue]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_bool(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Flags, true));

        (await loadAsync(id)).Flags.ShouldBe([true]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_string_outside_ascii(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Tags, "日本語 🐈"));

        (await loadAsync(id)).Tags.ShouldBe(["日本語 🐈"]);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_string_held_in_an_object(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add<object>(p, how, x => x.Things, "text"));

        ((JsonElement)(await loadAsync(id)).Things.Single()).GetString().ShouldBe("text");
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_guid_held_in_an_object(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);
        var key = Guid.NewGuid();

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add<object>(p, how, x => x.Things, key));

        ((JsonElement)(await loadAsync(id)).Things.Single()).GetGuid().ShouldBe(key);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_a_number_held_in_an_object(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add<object>(p, how, x => x.Things, 5));

        ((JsonElement)(await loadAsync(id)).Things.Single()).GetInt32().ShouldBe(5);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adds_an_object_held_in_an_object(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add<object>(p, how, x => x.Things, new Child("kid", 3)));

        ((JsonElement)(await loadAsync(id)).Things.Single()).GetProperty("name").GetString().ShouldBe("kid");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_finds_a_guid(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var key = Guid.NewGuid();

        var id = await storeAsync(new Doc { Guids = [key] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Guids, key));

        (await loadAsync(id)).Guids.ShouldBe([key]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_finds_a_date_time_offset(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Offsets = [AnOffset] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Offsets, AnOffset));

        (await loadAsync(id)).Offsets.ShouldBe([AnOffset]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_a_guid_to_a_dictionary(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var key = Guid.NewGuid();

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.GuidByName, "key", key));

        (await loadAsync(id)).GuidByName["key"].ShouldBe(key);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_a_double_to_a_dictionary(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        const double amount = 0.1 + 0.2;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.DoubleByName, "key", amount));

        (await loadAsync(id)).DoubleByName["key"].ShouldBe(amount);
    }

    // ---- writing a number --------------------------------------------------------------------------

    [Theory]
    [InlineData(true, 0.1 + 0.2)]
    [InlineData(false, 0.1 + 0.2)]
    [InlineData(true, 1234567890123456.7)]
    [InlineData(false, 1234567890123456.7)]
    [InlineData(true, 1.5E-30)]
    [InlineData(false, 1.5E-30)]
    [InlineData(true, 1.2345678901234567E-05)]
    [InlineData(false, 1.2345678901234567E-05)]
    [InlineData(true, 1.5E+30)]
    [InlineData(false, 1.5E+30)]
    [InlineData(true, -1E-11)]
    [InlineData(false, -1E-11)]
    [InlineData(false, double.MaxValue)]
    [InlineData(false, 5E-324)]
    public async Task set_stores_a_double_exactly(bool nativeJson, double value)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Double, value));

        (await loadAsync(id)).Double.ShouldBe(value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_stores_a_double_held_in_an_object_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        const double value = 0.1 + 0.2;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set<object?>(x => x.Anything, value));

        ((JsonElement)(await loadAsync(id)).Anything!).GetDouble().ShouldBe(value);
    }

    [Theory]
    [InlineData(true, 0.1f)]
    [InlineData(false, 0.1f)]
    [InlineData(true, 1.5E-30f)]
    [InlineData(false, 1.5E-30f)]
    [InlineData(true, 1.2345678E-05f)]
    [InlineData(false, 1.2345678E-05f)]
    public async Task set_stores_a_float_exactly(bool nativeJson, float value)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Single, value));

        (await loadAsync(id)).Single.ShouldBe(value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_keeps_a_decimal_at_full_precision(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Decimal, 7922816251426433759354395.0335m));

        (await loadAsync(id)).Decimal.ShouldBe(7922816251426433759354395.0335m);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_stores_every_other_kind_of_integer(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p
            .Set(x => x.Long, long.MinValue)
            .Set(x => x.Unsigned, ulong.MaxValue)
            .Set(x => x.SmallUnsigned, uint.MaxValue)
            .Set(x => x.Short, short.MinValue)
            .Set(x => x.Byte, byte.MaxValue)
            .Set(x => x.SignedByte, sbyte.MinValue)
            .Set(x => x.Huge, (Int128)long.MaxValue * 1000)
            .Set(x => x.HalfSize, (Half)1.5));

        var doc = await loadAsync(id);
        doc.Long.ShouldBe(long.MinValue);
        doc.Unsigned.ShouldBe(ulong.MaxValue);
        doc.SmallUnsigned.ShouldBe(uint.MaxValue);
        doc.Short.ShouldBe(short.MinValue);
        doc.Byte.ShouldBe(byte.MaxValue);
        doc.SignedByte.ShouldBe(sbyte.MinValue);
        doc.Huge.ShouldBe((Int128)long.MaxValue * 1000);
        doc.HalfSize.ShouldBe((Half)1.5);
    }

    [Theory]
    [InlineData(true, 0.1, 0.2)]
    [InlineData(false, 0.1, 0.2)]
    [InlineData(true, 1234567890123456.7, 1.0)]
    [InlineData(false, 1234567890123456.7, 1.0)]
    [InlineData(true, 1E-11, 1E-11)]
    [InlineData(false, 1E-11, 1E-11)]
    [InlineData(true, 1E-20, 0.0)]
    [InlineData(false, 1E-20, 0.0)]
    [InlineData(true, 1E+25, 1E+24)]
    [InlineData(false, 1E+25, 1E+24)]
    [InlineData(true, 1E+30, 1E+29)]
    [InlineData(false, 1E+30, 1E+29)]
    [InlineData(true, 1.5, -1.5)]
    [InlineData(false, 1.5, -1.5)]
    [InlineData(true, -0.1, -0.2)]
    [InlineData(false, -0.1, -0.2)]
    [InlineData(true, 1.1, 1.0)]
    [InlineData(false, 1.1, 1.0)]
    [InlineData(false, 1E+300, 1E+299)]
    public async Task increment_of_a_double_is_exact(bool nativeJson, double start, double increment)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Double = start });
        await patchAsync(id, p => p.Increment(x => x.Double, increment));

        (await loadAsync(id)).Double.ShouldBe(start + increment);
    }

    [Theory]
    [InlineData(true, 0.2f, 0.1f)]
    [InlineData(false, 0.2f, 0.1f)]
    [InlineData(true, 1E-07f, 2E-07f)]
    [InlineData(false, 1E-07f, 2E-07f)]
    public async Task increment_of_a_float_is_exact(bool nativeJson, float start, float increment)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Single = start });
        await patchAsync(id, p => p.Increment(x => x.Single, increment));

        (await loadAsync(id)).Single.ShouldBe(start + increment);
    }

    [Fact]
    public async Task increment_refuses_a_double_that_native_json_cannot_hold()
    {
        ConfigureFor(true);

        var id = await storeAsync(new Doc { Double = 1E+37 });

        await Should.ThrowAsync<Exception>(() => patchAsync(id, p => p.Increment(x => x.Double, 1E+38)));

        (await loadAsync(id)).Double.ShouldBe(1E+37);
    }

    [Fact]
    public async Task increment_refuses_a_tiny_double_that_native_json_cannot_hold()
    {
        ConfigureFor(true);

        // Each fits. Their sum has 39 decimal places, and that one comes back as 0 if it's let through.
        var id = await storeAsync(new Doc { Double = 2E-20 });

        await Should.ThrowAsync<Exception>(() => patchAsync(id, p => p.Increment(x => x.Double, 1.2345678901234567E-23 - 2E-20)));

        (await loadAsync(id)).Double.ShouldBe(2E-20);
    }

    // ---- an absent parent ----------------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "value"));

        (await loadAsync(id)).Middle!.Sibling.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_creates_every_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Text, "value"));

        (await loadAsync(id)).Middle!.Deep!.Text.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_keeps_what_a_parent_already_holds(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Middle = new Middle { Sibling = "kept" } });
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Text, "value"));

        var middle = (await loadAsync(id)).Middle!;
        middle.Sibling.ShouldBe("kept");
        middle.Deep!.Text.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_keeps_the_rest_of_a_parent_that_is_all_there(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Middle = new Middle { Deep = new Deep { OtherText = "kept" } } });
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Text, "value"));

        var deep = (await loadAsync(id)).Middle!.Deep!;
        deep.Text.ShouldBe("value");
        deep.OtherText.ShouldBe("kept");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_creates_a_parent_stored_as_a_json_null(bool nativeJson)
    {
        ConfigureFor(nativeJson, configure: o => o.DefaultIgnoreCondition = JsonIgnoreCondition.Never);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Text, "value"));

        (await loadAsync(id)).Middle!.Deep!.Text.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_of_an_object_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Kid, new Child("kid", 3)));

        (await loadAsync(id)).Middle!.Deep!.Kid.ShouldBe(new Child("kid", 3));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_of_a_double_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        const double amount = 0.1 + 0.2;

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Amount, amount));

        (await loadAsync(id)).Middle!.Deep!.Amount.ShouldBe(amount);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Increment(x => x.Middle!.Deep!.Number, 3));

        (await loadAsync(id)).Middle!.Deep!.Number.ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task increment_of_a_double_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Increment(x => x.Middle!.Deep!.Amount, 0.5));

        (await loadAsync(id)).Middle!.Deep!.Amount.ShouldBe(0.5);
    }

    [Theory]
    [MemberData(nameof(EveryWayOfAdding))]
    public async Task adding_to_a_list_creates_an_absent_parent(bool nativeJson, Adding how)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => Add(p, how, x => x.Middle!.Deep!.Tags!, "tag"));

        (await loadAsync(id)).Middle!.Deep!.Tags.ShouldBe(["tag"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_to_a_dictionary_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Append(x => x.Middle!.Deep!.Labels!, "key", "value"));

        (await loadAsync(id)).Middle!.Deep!.Labels!["key"].ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_to_a_dictionary_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Middle!.Deep!.Labels!, "key", "value"));

        (await loadAsync(id)).Middle!.Deep!.Labels!["key"].ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_creates_an_absent_parent(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Text = "value" });
        await patchAsync(id, p => p.Duplicate(x => x.Text, x => x.Middle!.Deep!.Text));

        (await loadAsync(id)).Middle!.Deep!.Text.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task duplicate_to_two_members_of_the_same_absent_parent_keeps_both(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Text = "value" });
        await patchAsync(id, p => p.Duplicate(x => x.Text, x => x.Middle!.Deep!.Text, x => x.Middle!.Deep!.OtherText));

        var deep = (await loadAsync(id)).Middle!.Deep!;
        deep.Text.ShouldBe("value");
        deep.OtherText.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_absent_parent_under_a_key_that_needs_quoting(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle!.Deep!.Labels!["a.b"], "value"));

        (await loadAsync(id)).Middle!.Deep!.Labels!["a.b"].ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_by_a_where_clause_creates_the_parent_on_every_match(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var absent = await storeAsync(new Doc { Text = "match" });
        var present = await storeAsync(new Doc { Text = "match", Middle = new Middle { Sibling = "kept" } });

        await using (var session = theStore.LightweightSession())
        {
            // The filter reads the parent, so it has to be evaluated once, before it exists
            session.Patch<Doc>(x => x.Text == "match").Set(x => x.Middle!.Deep!.Text, "value");
            await session.SaveChangesAsync(Ct);
        }

        (await loadAsync(absent)).Middle!.Deep!.Text.ShouldBe("value");
        var kept = (await loadAsync(present)).Middle!;
        kept.Sibling.ShouldBe("kept");
        kept.Deep!.Text.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task set_by_a_where_clause_on_the_absent_parent_itself(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<Doc>(x => x.Id == id && x.Middle == null).Set(x => x.Middle!.Sibling, "value");
            await session.SaveChangesAsync(Ct);
        }

        (await loadAsync(id)).Middle!.Sibling.ShouldBe("value");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_parent_that_holds_a_value_is_not_replaced(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Number = 5 });
        await patchAsync(id, p => p.Set("Sibling", x => x.Number, "value"));

        (await loadAsync(id)).Number.ShouldBe(5);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task delete_under_an_absent_parent_creates_nothing(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Delete(x => x.Middle!.Deep!.Text));

        (await loadAsync(id)).Middle.ShouldBeNull();
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_under_an_absent_parent_creates_nothing(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Remove(x => x.Middle!.Deep!.Tags!, "tag", action));

        (await loadAsync(id)).Middle.ShouldBeNull();
    }

    // ---- what a write leaves behind for the next one -------------------------------------------------

    /// <summary>
    ///     SQL Server 2025 leaves a native json value in a state where the next path through an object
    ///     fails or goes to the wrong member, after a JSON_MODIFY that adds a key. The row still reads
    ///     back correctly, so the only way to see it is to write again. See PatchOperation.Document.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_through_an_object_after_one_that_added_a_string(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Middle = new Middle { Sibling = "old" } });
        await patchAsync(id, p => p.Set(x => x.Text, "added"));
        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "new"));

        var doc = await loadAsync(id);
        doc.Text.ShouldBe("added");
        doc.Middle!.Sibling.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_through_an_object_after_one_that_added_a_number(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Middle = new Middle { Sibling = "old" } });
        await patchAsync(id, p => p.Set(x => x.MaybeNumber, 5));
        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "new"));

        var doc = await loadAsync(id);
        doc.MaybeNumber.ShouldBe(5);
        doc.Middle!.Sibling.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_under_an_object_a_patch_just_added(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle, new Middle { Sibling = "old" }));
        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "new"));

        (await loadAsync(id)).Middle!.Sibling.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_write_under_an_empty_object_a_patch_just_added(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc());
        await patchAsync(id, p => p.Set(x => x.Middle, new Middle()));
        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "new"));

        (await loadAsync(id)).Middle!.Sibling.ShouldBe("new");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task both_writes_in_one_patch(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Middle = new Middle { Sibling = "old" } });
        await patchAsync(id, p => p.Set(x => x.Text, "added").Set(x => x.Middle!.Sibling, "new"));

        var doc = await loadAsync(id);
        doc.Text.ShouldBe("added");
        doc.Middle!.Sibling.ShouldBe("new");
    }

    [Fact]
    public async Task a_row_already_left_in_that_state_is_put_right_by_a_patch()
    {
        ConfigureFor(true);

        var id = await storeAsync(new Doc { Middle = new Middle { Sibling = "old" } });

        // What a patch from before this fix left behind
        await using (var connection = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"UPDATE [{theStore.Options.DatabaseSchemaName}].pc_doc_doc SET data = JSON_MODIFY(data, '$.text', 'added') WHERE id = @id";
            command.Parameters.AddWithValue("@id", id);
            await command.ExecuteNonQueryAsync(Ct);
        }

        await patchAsync(id, p => p.Set(x => x.Middle!.Sibling, "new"));

        var doc = await loadAsync(id);
        doc.Text.ShouldBe("added");
        doc.Middle!.Sibling.ShouldBe("new");
    }

    // ---- matching an element ---------------------------------------------------------------------------

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_does_not_take_a_string_that_differs_in_case(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["abc", "ABC", "Abc"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "ABC", action));

        (await loadAsync(id)).Tags.ShouldBe(["abc", "Abc"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_does_not_take_a_string_that_differs_in_trailing_spaces(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["abc ", "abc"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "abc", action));

        (await loadAsync(id)).Tags.ShouldBe(["abc "]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_does_not_take_a_string_that_differs_in_accent_or_width(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["e", "é", "ｅ"] });
        await patchAsync(id, p => p.Remove(x => x.Tags, "é", action));

        (await loadAsync(id)).Tags.ShouldBe(["e", "ｅ"]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_does_not_take_an_object_that_differs_in_case(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Kids = [new Child("abc", 1), new Child("ABC", 1)] });
        await patchAsync(id, p => p.Remove(x => x.Kids, new Child("ABC", 1), action));

        (await loadAsync(id)).Kids.ShouldBe([new Child("abc", 1)]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_finds_an_object_whose_text_the_serializer_escapes(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("clé <a> & 'b' \"c\" +", 1);

        var id = await storeAsync(new Doc { Kids = [new Child("other", 1), kid] });
        await patchAsync(id, p => p.Remove(x => x.Kids, kid, action));

        (await loadAsync(id)).Kids.ShouldBe([new Child("other", 1)]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task if_not_exists_finds_an_object_whose_text_the_serializer_escapes(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var kid = new Child("clé <a> & 'b' \"c\" +", 1);

        var id = await storeAsync(new Doc { Kids = [kid] });
        await patchAsync(id, p =>
        {
            p.AppendIfNotExists(x => x.Kids, kid);
            p.InsertIfNotExists(x => x.Kids, kid);
            p.InsertIfNotExists(x => x.Kids, kid, 0);
        });

        (await loadAsync(id)).Kids.ShouldBe([kid]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task if_not_exists_finds_an_object_with_a_number_in_exponent_form(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var kid = new Measured("kid", 1.5E-30);

        var id = await storeAsync(new Doc { Things = [kid] });
        await patchAsync(id, p => p.AppendIfNotExists<object>(x => x.Things, kid));

        (await loadAsync(id)).Things.Count.ShouldBe(1);
    }

    public record Measured(string Name, double Amount);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_adds_a_string_that_differs_in_case(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["ABC"] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Tags, "abc"));

        (await loadAsync(id)).Tags.ShouldBe(["ABC", "abc"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_adds_a_string_that_differs_in_trailing_spaces(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["abc "] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Tags, "abc"));

        (await loadAsync(id)).Tags.ShouldBe(["abc ", "abc"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task insert_if_not_exists_adds_a_string_that_differs_in_case(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Tags = ["ABC"] });
        await patchAsync(id, p => p
            .InsertIfNotExists(x => x.Tags, "abc")
            .InsertIfNotExists(x => x.Tags, "Abc", 0));

        (await loadAsync(id)).Tags.ShouldBe(["Abc", "ABC", "abc"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task if_not_exists_adds_an_object_that_differs_in_case(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Kids = [new Child("ABC", 1)] });
        await patchAsync(id, p => p.AppendIfNotExists(x => x.Kids, new Child("abc", 1)));

        (await loadAsync(id)).Kids.ShouldBe([new Child("ABC", 1), new Child("abc", 1)]);
    }

    [Theory]
    [InlineData(true, RemoveAction.RemoveFirst)]
    [InlineData(false, RemoveAction.RemoveFirst)]
    [InlineData(true, RemoveAction.RemoveAll)]
    [InlineData(false, RemoveAction.RemoveAll)]
    public async Task remove_of_a_number_does_not_take_a_string_with_the_same_text(bool nativeJson, RemoveAction action)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Things = ["1", 1] });
        await patchAsync(id, p => p.Remove<object>(x => x.Things, 1, action));

        var thing = (JsonElement)(await loadAsync(id)).Things.Single();
        thing.GetString().ShouldBe("1");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task append_if_not_exists_of_a_string_does_not_find_a_number_with_the_same_text(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Things = [1] });
        await patchAsync(id, p => p.AppendIfNotExists<object>(x => x.Things, "1"));

        (await loadAsync(id)).Things.Count.ShouldBe(2);
    }
}
