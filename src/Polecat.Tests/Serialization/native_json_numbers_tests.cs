using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Exceptions;
using Polecat.Patching;
using Polecat.Linq;
using Polecat.Serialization;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.Core;

namespace Polecat.Tests.Serialization;

/// <summary>
///     #879. The native <c>json</c> type does not hold what it is given. A number written with an
///     exponent goes through a double and a <c>numeric(38,10)</c>, and any other number of more than 38
///     digits is rounded to the same ten places, in both cases with no error. <c>nvarchar(max)</c>
///     storage keeps the text.
/// </summary>
public class native_json_numbers_tests: OneOffConfigurationsContext
{
    public class Doc
    {
        public Guid Id { get; set; }
        public double Double { get; set; }
        public double? Maybe { get; set; }
        public float Single { get; set; }
        public decimal Decimal { get; set; }
        public List<double> Amounts { get; set; } = [];
        public Dictionary<string, double> ByName { get; set; } = new();
        public object? Anything { get; set; }
        public JsonElement? Raw { get; set; }

        [JsonNumberHandling(JsonNumberHandling.WriteAsString | JsonNumberHandling.AllowReadingFromString)]
        public double AsText { get; set; }
    }

    public record Measured(Guid StreamId, double Amount);

    public class Total
    {
        public Guid Id { get; set; }
        public double Amount { get; set; }

        public void Apply(Measured e) => Amount = e.Amount;
        public static Total Create(Measured e) => new() { Id = e.StreamId, Amount = e.Amount };
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, Action<StoreOptions>? configure = null)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            configure?.Invoke(opts);
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

    private async Task<Doc> loadAsync(Guid id)
    {
        await using var session = theStore.QuerySession();
        return (await session.LoadAsync<Doc>(id, Ct)).ShouldNotBeNull();
    }

    private static readonly double[] Holdable =
    [
        1.5E-30, -1.5E-30, 1.5E+20, 1.5E+30, 1E+28, 1E+37, 9.99E+37, 1E-05, 1.2345678901234567E-05,
        1.2345678901234567E-21, 1E-38, 6.02214076E+23, 1.602176634E-19, 1E+16, 123456789012345680d, 0.1 + 0.2
    ];

    private static readonly double[] NotHoldable =
    [
        double.MaxValue, double.MinValue, 1E+38, -1E+38, 1E+40, 5E-324, 1E-39, 6.62607015E-34, 1.2345678901234567E-23
    ];

    public static TheoryData<double> HoldableValues() => new(Holdable);
    public static TheoryData<double> NotHoldableValues() => new(NotHoldable);

    public static TheoryData<bool, double> HoldableOnEitherShape()
    {
        var data = new TheoryData<bool, double>();
        foreach (var value in Holdable)
        {
            data.Add(true, value);
            data.Add(false, value);
        }

        return data;
    }

    // ---- what goes in comes back ---------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(HoldableOnEitherShape))]
    public async Task a_double_comes_back_exactly(bool nativeJson, double value)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc
        {
            Double = value, Maybe = value, Amounts = [value], ByName = { ["key"] = value }, Anything = value
        });

        var doc = await loadAsync(id);
        doc.Double.ShouldBe(value);
        doc.Maybe.ShouldBe(value);
        doc.Amounts.ShouldBe([value]);
        doc.ByName["key"].ShouldBe(value);
        ((JsonElement)doc.Anything!).GetDouble().ShouldBe(value);
    }

    [Theory]
    [InlineData(true, 1.5E-30f)]
    [InlineData(false, 1.5E-30f)]
    [InlineData(true, 1.2345678E-05f)]
    [InlineData(false, 1.2345678E-05f)]
    [InlineData(true, 1.5E+20f)]
    [InlineData(false, 1.5E+20f)]
    [InlineData(true, 1E+37f)]
    [InlineData(false, 1E+37f)]
    public async Task a_float_comes_back_exactly(bool nativeJson, float value)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Single = value });

        (await loadAsync(id)).Single.ShouldBe(value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_decimal_keeps_its_digits_and_its_scale(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Decimal = 7922816251426433759354395.0335m });
        (await loadAsync(id)).Decimal.ShouldBe(7922816251426433759354395.0335m);

        var scaled = await storeAsync(new Doc { Decimal = 1.10m });
        (await loadAsync(scaled)).Decimal.ToString(CultureInfo.InvariantCulture).ShouldBe("1.10");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_number_in_a_json_element_comes_back_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        using var raw = JsonDocument.Parse("{\"a\":[1.2345678901234567E-05, 1.5e+30]}");

        var id = await storeAsync(new Doc { Raw = raw.RootElement.Clone() });

        var a = (await loadAsync(id)).Raw!.Value.GetProperty("a");
        a[0].GetDouble().ShouldBe(1.2345678901234567E-05);
        a[1].GetDouble().ShouldBe(1.5E+30);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task bulk_insert_comes_back_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var doc = new Doc { Id = Guid.NewGuid(), Double = 1.2345678901234567E-05 };

        await theStore.Advanced.BulkInsertAsync([doc], token: Ct);

        (await loadAsync(doc.Id)).Double.ShouldBe(1.2345678901234567E-05);
    }

    // ---- reading it from SQL ---------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(HoldableOnEitherShape))]
    public async Task linq_finds_a_double_by_its_value(bool nativeJson, double value)
    {
        ConfigureFor(nativeJson);

        var id = await storeAsync(new Doc { Double = value });
        await storeAsync(new Doc { Double = 7 });

        await using var session = theStore.QuerySession();
        var ids = await session.Query<Doc>().Where(x => x.Double == value).Select(x => x.Id).ToListAsync(Ct);
        ids.ShouldBe([id]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task linq_orders_doubles_by_their_value(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        double[] values = [1.5E+30, 1E-05, -1.5E-30, 1.5E-30, 7, 1.5E+20];
        foreach (var value in values)
        {
            await storeAsync(new Doc { Double = value });
        }

        await using var session = theStore.QuerySession();
        var ordered = await session.Query<Doc>().OrderBy(x => x.Double).Select(x => x.Double).ToListAsync(Ct);
        ordered.ShouldBe(values.Order());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task linq_compares_doubles_by_their_value(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        await storeAsync(new Doc { Double = 1.5E-30 });
        var larger = await storeAsync(new Doc { Double = 2.5E-30 });

        await using var session = theStore.QuerySession();
        var ids = await session.Query<Doc>().Where(x => x.Double > 2E-30).Select(x => x.Id).ToListAsync(Ct);
        ids.ShouldBe([larger]);
    }

    // ---- events ----------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(HoldableOnEitherShape))]
    public async Task an_event_comes_back_exactly(bool nativeJson, double value)
    {
        ConfigureFor(nativeJson);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, value));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        ((Measured)events.Single().Data).Amount.ShouldBe(value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_header_comes_back_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson, opts => opts.Events.EnableHeaders = true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.SetHeader("amount", 1.2345678901234567E-05);
            session.Events.StartStream(streamId, new Measured(streamId, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        ((JsonElement)events.Single().Headers!["amount"]!).GetDouble().ShouldBe(1.2345678901234567E-05);
    }

    private async Task<string> storedHeadersAsync(Guid id)
    {
        await using var connection = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT CAST(headers AS nvarchar(max)) FROM [{theStore.Options.DatabaseSchemaName}].pc_doc_doc WHERE id = @id";
        command.Parameters.AddWithValue("@id", id);
        return (string)(await command.ExecuteScalarAsync(Ct))!;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_document_header_comes_back_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson, opts => opts.Schema.For<Doc>().Metadata(m => m.Headers.Enabled = true));
        var doc = new Doc { Id = Guid.NewGuid() };

        await using (var session = theStore.LightweightSession())
        {
            session.SetHeader("amount", 1.2345678901234567E-05);
            session.Store(doc);
            await session.SaveChangesAsync(Ct);
        }

        using var headers = JsonDocument.Parse(await storedHeadersAsync(doc.Id));
        headers.RootElement.GetProperty("amount").GetDouble().ShouldBe(1.2345678901234567E-05);
    }

    [Fact]
    public async Task a_document_header_native_json_cannot_hold_is_refused()
    {
        ConfigureFor(true, opts => opts.Schema.For<Doc>().Metadata(m => m.Headers.Enabled = true));

        await using var session = theStore.LightweightSession();
        session.SetHeader("amount", double.MaxValue);
        session.Store(new Doc { Id = Guid.NewGuid() });

        await Should.ThrowAsync<NativeJsonNumberException>(() => session.SaveChangesAsync(Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_projected_document_comes_back_exactly(bool nativeJson)
    {
        ConfigureFor(nativeJson, opts => opts.Projections.Snapshot<Total>(SnapshotLifecycle.Inline));
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, 1.2345678901234567E-05));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Total>(streamId, Ct))!.Amount.ShouldBe(1.2345678901234567E-05);
    }

    [Fact]
    public async Task a_replaced_event_comes_back_exactly()
    {
        ConfigureFor(true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stored = await session.Events.FetchStreamAsync(streamId, token: Ct);
            session.Events.CompletelyReplaceEvent(stored.Single().Sequence, new Measured(streamId, 1.2345678901234567E-05));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        ((Measured)events.Single().Data).Amount.ShouldBe(1.2345678901234567E-05);
    }

    [Fact]
    public async Task an_overwritten_event_comes_back_exactly()
    {
        ConfigureFor(true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            var stored = (await session.Events.FetchStreamAsync(streamId, token: Ct)).Single();
            ((Event<Measured>)stored).Data = new Measured(streamId, 1.2345678901234567E-05);
            session.Events.OverwriteEvent(stored);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        ((Measured)events.Single().Data).Amount.ShouldBe(1.2345678901234567E-05);
    }

    // ---- what native json cannot hold ------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(NotHoldableValues))]
    public async Task a_double_native_json_cannot_hold_is_refused(double value)
    {
        ConfigureFor(true);

        var doc = new Doc { Id = Guid.NewGuid(), Double = value };
        await using (var session = theStore.LightweightSession())
        {
            session.Store(doc);
            var failure = await Should.ThrowAsync<NativeJsonNumberException>(() => session.SaveChangesAsync(Ct));

            failure.Number.ShouldBe(value.ToString("R", CultureInfo.InvariantCulture));
            failure.Member.ShouldBe("double");
            failure.Message.ShouldContain(failure.Number);
            failure.Message.ShouldContain("'double'");
            failure.Message.ShouldContain(nameof(StoreOptions.UseNativeJsonType));
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Doc>(doc.Id, Ct)).ShouldBeNull();
    }

    [Theory]
    [MemberData(nameof(NotHoldableValues))]
    public async Task nvarchar_storage_holds_what_native_json_cannot(double value)
    {
        ConfigureFor(false);

        var id = await storeAsync(new Doc { Double = value, Amounts = [value], Anything = value });

        var doc = await loadAsync(id);
        doc.Double.ShouldBe(value);
        doc.Amounts.ShouldBe([value]);
        ((JsonElement)doc.Anything!).GetDouble().ShouldBe(value);
    }

    [Theory]
    [InlineData(float.MaxValue)]
    [InlineData(float.MinValue)]
    [InlineData(float.Epsilon)]
    public async Task a_float_native_json_cannot_hold_is_refused(float value)
    {
        ConfigureFor(true);

        await Should.ThrowAsync<NativeJsonNumberException>(() => storeAsync(new Doc { Single = value }));
    }

    [Fact]
    public async Task a_refused_number_names_the_member_it_is_in_when_it_is_in_a_list()
    {
        ConfigureFor(true);

        var failure = await Should.ThrowAsync<NativeJsonNumberException>(() =>
            storeAsync(new Doc { Amounts = [1, 2, double.MaxValue] }));

        failure.Member.ShouldBe("amounts");
    }

    [Fact]
    public async Task a_refused_event_appends_nothing()
    {
        ConfigureFor(true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, double.MaxValue));
            await Should.ThrowAsync<NativeJsonNumberException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_refused_header_appends_nothing()
    {
        ConfigureFor(true, opts => opts.Events.EnableHeaders = true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.SetHeader("amount", double.MaxValue);
            session.Events.StartStream(streamId, new Measured(streamId, 1));
            await Should.ThrowAsync<NativeJsonNumberException>(() => session.SaveChangesAsync(Ct));
        }

        await using var query = theStore.QuerySession();
        (await query.Events.FetchStreamAsync(streamId, token: Ct)).ShouldBeEmpty();
    }

    [Fact]
    public async Task bulk_insert_refuses_too()
    {
        ConfigureFor(true);
        var doc = new Doc { Id = Guid.NewGuid(), Double = double.MaxValue };

        await Should.ThrowAsync<NativeJsonNumberException>(() => theStore.Advanced.BulkInsertAsync([doc], token: Ct));
    }

    [Fact]
    public async Task a_patch_refuses_too()
    {
        ConfigureFor(true);
        var id = await storeAsync(new Doc { Double = 5 });

        await using var session = theStore.LightweightSession();

        Should.Throw<NativeJsonNumberException>(() => session.Patch<Doc>(id).Set(x => x.Double, double.MaxValue));

        (await loadAsync(id)).Double.ShouldBe(5);
    }

    [Fact]
    public async Task a_member_written_as_a_string_is_a_way_to_hold_it()
    {
        ConfigureFor(true);

        var id = await storeAsync(new Doc { AsText = double.MaxValue });
        (await loadAsync(id)).AsText.ShouldBe(double.MaxValue);

        await using var session = theStore.QuerySession();
        var ids = await session.Query<Doc>().Where(x => x.AsText > 1E+300).Select(x => x.Id).ToListAsync(Ct);
        ids.ShouldBe([id]);
    }

    [Fact]
    public async Task a_member_written_as_a_string_still_sorts_by_its_value()
    {
        ConfigureFor(true);

        var largest = await storeAsync(new Doc { AsText = double.MaxValue });
        var smallest = await storeAsync(new Doc { AsText = 5 });
        var middle = await storeAsync(new Doc { AsText = 1E+300 });

        await using var session = theStore.QuerySession();
        var ordered = await session.Query<Doc>().OrderBy(x => x.AsText).Select(x => x.Id).ToListAsync(Ct);
        ordered.ShouldBe([smallest, middle, largest]);
    }

    // ---- whatever wrote it ------------------------------------------------------------------------------

    /// <summary>A serializer that is not the default one, and has nothing to do with it.</summary>
    public class OtherSerializer: ISerializer
    {
        private readonly Serializer _inner = new();

        public EnumStorage EnumStorage => _inner.EnumStorage;
        public Casing Casing => _inner.Casing;
        public T FromJson<T>(string json) => _inner.FromJson<T>(json);
        public object FromJson(Type type, string json) => _inner.FromJson(type, json);
        public string ToJson(object? document) => _inner.ToJson(document);
        public T FromJson<T>(Stream stream) => _inner.FromJson<T>(stream);
        public T FromJson<T>(System.Data.Common.DbDataReader reader, int index) => _inner.FromJson<T>(reader, index);
        public ValueTask<T> FromJsonAsync<T>(Stream stream, CancellationToken cancellationToken = default)
            => _inner.FromJsonAsync<T>(stream, cancellationToken);
        public ValueTask<T> FromJsonAsync<T>(System.Data.Common.DbDataReader reader, int index,
            CancellationToken cancellationToken = default) => _inner.FromJsonAsync<T>(reader, index, cancellationToken);
        public object FromJson(Type type, Stream stream) => _inner.FromJson(type, stream);
        public object FromJson(Type type, System.Data.Common.DbDataReader reader, int index)
            => _inner.FromJson(type, reader, index);
        public ValueTask<object> FromJsonAsync(Type type, Stream stream, CancellationToken cancellationToken = default)
            => _inner.FromJsonAsync(type, stream, cancellationToken);
        public ValueTask<object> FromJsonAsync(Type type, System.Data.Common.DbDataReader reader, int index,
            CancellationToken cancellationToken = default) => _inner.FromJsonAsync(type, reader, index, cancellationToken);
        public string ToCleanJson(object? document) => _inner.ToCleanJson(document);
    }

    [Fact]
    public async Task a_serializer_of_your_own_is_held_to_the_same()
    {
        ConfigureFor(true, opts => opts.Serializer = new OtherSerializer());

        var id = await storeAsync(new Doc { Double = 1.2345678901234567E-05 });

        // Read by hand, because a serializer that isn't ours can't load a document at all yet: the
        // storage adapter asks SqlClient for a stream, which it won't give for a json column.
        await using (var connection = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT CONVERT(nvarchar(max), data) FROM [{theStore.Options.DatabaseSchemaName}].pc_doc_doc WHERE id = @id";
            command.Parameters.AddWithValue("@id", id);

            var json = (string)(await command.ExecuteScalarAsync(Ct))!;
            using var stored = JsonDocument.Parse(json);
            stored.RootElement.GetProperty("double").GetDouble().ShouldBe(1.2345678901234567E-05);
        }

        await Should.ThrowAsync<NativeJsonNumberException>(() => storeAsync(new Doc { Double = double.MaxValue }));
    }

    [Fact]
    public async Task a_custom_serializer_stays_the_one_the_options_hold()
    {
        var serializer = new OtherSerializer();
        ConfigureFor(true, opts => opts.Serializer = serializer);

        _ = await storeAsync(new Doc());

        theStore.Options.Serializer.ShouldBeSameAs(serializer);
    }

    // ---- only what is stored ------------------------------------------------------------------------------

    [Fact]
    public void writing_json_for_anything_else_is_left_alone()
    {
        ConfigureFor(true);

        var serializer = theStore.Options.Serializer;

        serializer.ToJson(1.5E-30).ShouldBe("1.5E-30");
        serializer.ToJson(double.MaxValue).ShouldBe("1.7976931348623157E+308");
    }

    [Fact]
    public async Task a_live_aggregate_that_could_not_be_stored_can_still_be_written_out()
    {
        ConfigureFor(true);
        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new Measured(streamId, 1));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var total = await query.Events.AggregateStreamAsync<Total>(streamId, token: Ct);
        total!.Amount = double.MaxValue;

        Should.NotThrow(() => theStore.Options.Serializer.ToJson(total));
    }
}

/// <summary>
///     The rewrite on its own, because the cases that matter most here are the ones no serializer in
///     the suite would write.
/// </summary>
public class native_json_number_text_tests
{
    [Theory]
    [InlineData("1E+2", "100")]
    [InlineData("1e2", "100")]
    [InlineData("1E2", "100")]
    [InlineData("1.5E+20", "150000000000000000000")]
    [InlineData("1.5E-30", "0.0000000000000000000000000000015")]
    [InlineData("-1.5E-3", "-0.0015")]
    [InlineData("1.25E+1", "12.5")]
    [InlineData("1.25E+0", "1.25")]
    [InlineData("12.5E-1", "1.25")]
    [InlineData("0.0125E+2", "1.25")]
    [InlineData("0E0", "0")]
    [InlineData("0.0E+0", "0")]
    [InlineData("-0.0E+0", "0")]
    [InlineData("0E+400", "0")]
    [InlineData("1.50E+1", "15")]
    [InlineData("1E+37", "10000000000000000000000000000000000000")]
    [InlineData("1E-38", "0.00000000000000000000000000000000000001")]
    public void an_exponent_is_written_out(string number, string expected)
    {
        NativeJsonNumbers.ForStorage(number).ShouldBe(expected);
        NativeJsonNumbers.ForStorage($"{{\"a\":{number},\"b\":[{number}, {number}]}}")
            .ShouldBe($"{{\"a\":{expected},\"b\":[{expected}, {expected}]}}");
    }

    [Theory]
    [InlineData("1.10")]
    [InlineData("100")]
    [InlineData("-5")]
    [InlineData("0")]
    [InlineData("-0")]
    [InlineData("0.0")]
    [InlineData("0.1000")]
    [InlineData("12345678901234567890123456789012345678")]
    [InlineData("0.12345678901234567890123456789012345678")]
    [InlineData("1.2345678901234567890123456789012345678")]
    public void a_number_it_can_hold_is_not_touched(string number)
    {
        ReferenceEquals(number, NativeJsonNumbers.ForStorage(number)).ShouldBeTrue();
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("\"\"")]
    [InlineData("\"1E+400\"")]
    [InlineData("{\"1E+400\":\"1E+400\"}")]
    [InlineData("[\"a\\\"1E+400\",\"\\\\\",\"1E+400\"]")]
    [InlineData("{\"a\":\"\\u00221E+400\"}")]
    [InlineData("{\"true\":true,\"false\":false,\"null\":null}")]
    public void nothing_in_a_string_is_a_number(string json)
    {
        ReferenceEquals(json, NativeJsonNumbers.ForStorage(json)).ShouldBeTrue();
    }

    [Fact]
    public void the_text_around_a_number_is_kept()
    {
        NativeJsonNumbers.ForStorage("{ \"a\" : \"1E+2\" , \"b\" : 1E+2 , \"c\" : \"é\" }\n")
            .ShouldBe("{ \"a\" : \"1E+2\" , \"b\" : 100 , \"c\" : \"é\" }\n");
    }

    [Theory]
    [InlineData("1E+38")]
    [InlineData("1E-39")]
    [InlineData("1.2E-38")]
    [InlineData("1.7976931348623157E+308")]
    [InlineData("5E-324")]
    [InlineData("1E+99999999999999999999")]
    [InlineData("1E-99999999999999999999")]
    [InlineData("100000000000000000000000000000000000000")]
    [InlineData("0.123456789012345678901234567890123456789")]
    [InlineData("1.23456789012345678901234567890123456789")]
    [InlineData("0.000000000000000000000000000000000000012")]
    [InlineData("123456789012345678901234567.123456789012")]
    public void a_number_it_cannot_hold_is_refused(string number)
    {
        Should.Throw<NativeJsonNumberException>(() => NativeJsonNumbers.ForStorage(number))
            .Number.ShouldBe(number);
    }

    [Theory]
    [InlineData("0.10000000000000000000000000000000000000000000", "0.1")]
    [InlineData("1.0000000000000000000000000000000000000000000", "1")]
    public void digits_that_change_nothing_do_not_count(string number, string expected)
    {
        NativeJsonNumbers.ForStorage(number).ShouldBe(expected);
    }

    [Fact]
    public void a_refusal_names_the_nearest_member()
    {
        Should.Throw<NativeJsonNumberException>(() =>
                NativeJsonNumbers.ForStorage("{\"first\":\"x:y\",\"second\":{\"third\":1E+400}}"))
            .Member.ShouldBe("third");
    }

    [Fact]
    public void a_refusal_with_no_member_says_so()
    {
        var failure = Should.Throw<NativeJsonNumberException>(() => NativeJsonNumbers.ForStorage("[1E+400]"));

        failure.Member.ShouldBeNull();
        failure.Message.ShouldContain("1E+400");
    }

    [Fact]
    public void a_long_document_with_no_numbers_comes_back_as_it_went_in()
    {
        var json = "{\"text\":\"" + new string('e', 100_000) + "\"}";

        ReferenceEquals(json, NativeJsonNumbers.ForStorage(json)).ShouldBeTrue();
    }
}
