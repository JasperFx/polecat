using System.Collections;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

public class ClrOnlyDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public int? N { get; set; }
    public bool? Flag { get; set; }
    public TargetColor? Color { get; set; }
    public DateTimeOffset Created { get; set; }
    public DateTime Stamp { get; set; }
    public DateTime? Due { get; set; }
    public DateOnly Day { get; set; }
    public TimeOnly At { get; set; }
    public TimeSpan Span { get; set; }
    public decimal Price { get; set; }
    public List<string> Items { get; set; } = [];
    public string[] Arr { get; set; } = [];
    public int[] Numbers { get; set; } = [];
    public TargetColor[] Colors { get; set; } = [];
    public IReadOnlyList<int> Readings { get; set; } = [];
    public Dictionary<string, int> Map { get; set; } = new();
    public byte[] Bytes { get; set; } = [];
    public ClrOnlyBag Bag { get; set; } = new();
    public ClrOnlyStats Stats { get; set; } = new();
    public ClrOnlyMoney? Cost { get; set; }
    public ClrOnlyTally Tally { get; set; } = new();
}

/// <summary>A document member whose members share their names with the CLR-only ones.</summary>
public class ClrOnlyStats
{
    public int Count { get; set; }
    public int Value { get; set; }
    public int Length { get; set; }
    public int Year { get; set; }
    public bool HasValue { get; set; }
    public DateTime Date { get; set; }
}

/// <summary>A nullable struct that is a JSON object, so JSON_VALUE reads NULL for it.</summary>
public struct ClrOnlyMoney
{
    public decimal Amount { get; set; }
    public string Currency { get; set; }
}

/// <summary>
///     A collection of its own. System.Text.Json writes the elements and nothing else, so Label has no
///     key, and Count is whatever this class says it is rather than the framework's element count.
/// </summary>
public class ClrOnlyBag : Collection<string>
{
    public string Label { get; set; } = "";
    public new int Count => base.Count * 2;
}

/// <summary>
///     An enumerable whose converter stores it as an object, so its Count is a real key. The attribute
///     is how the rule can tell.
/// </summary>
[JsonConverter(typeof(ClrOnlyTallyConverter))]
public class ClrOnlyTally : IEnumerable<string>
{
    public List<string> Marks { get; set; } = [];
    public int Count => Marks.Count;

    public IEnumerator<string> GetEnumerator() => Marks.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

public class ClrOnlyTallyConverter : JsonConverter<ClrOnlyTally>
{
    public override ClrOnlyTally Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var count = document.RootElement.GetProperty("count").GetInt32();
        return new ClrOnlyTally { Marks = Enumerable.Repeat("x", count).ToList() };
    }

    public override void Write(Utf8JsonWriter writer, ClrOnlyTally value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("count", value.Count);
        writer.WriteEndObject();
    }
}

/// <summary>A document that is itself enumerable, stored as an object by a registered converter.</summary>
public class ClrOnlyEnumerableDoc : IEnumerable<string>
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";

    public IEnumerator<string> GetEnumerator() => Enumerable.Empty<string>().GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

public class ClrOnlyEnumerableDocConverter : JsonConverter<ClrOnlyEnumerableDoc>
{
    public override ClrOnlyEnumerableDoc Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        return new ClrOnlyEnumerableDoc
        {
            Id = document.RootElement.GetProperty("id").GetGuid(),
            Name = document.RootElement.GetProperty("name").GetString()!
        };
    }

    public override void Write(Utf8JsonWriter writer, ClrOnlyEnumerableDoc value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("id", value.Id);
        writer.WriteString("name", value.Name);
        writer.WriteEndObject();
    }
}

/// <summary>
///     #840: a member that exists only in .NET used to become a JSON path that is always missing, so
///     the query ran and matched nothing. Each of these is translated or refused, and which one is
///     decided by the type the member is read off rather than by its name.
/// </summary>
public class clr_only_member_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static readonly ClrOnlyDoc Ann = new()
    {
        Id = Guid.NewGuid(),
        Name = "Ann",
        Age = 30,
        N = 5,
        Flag = true,
        Color = TargetColor.Green,
        // +05:00 on purpose: the year is 2024 here and 2023 in UTC.
        Created = new DateTimeOffset(2024, 1, 1, 1, 20, 30, 456, TimeSpan.FromHours(5)),
        Stamp = new DateTime(2024, 3, 15, 10, 20, 30, 456, DateTimeKind.Utc),
        Due = new DateTime(2023, 12, 31, 0, 0, 0, DateTimeKind.Utc),
        Day = new DateOnly(2024, 2, 29),
        At = new TimeOnly(13, 14, 15, 678),
        Items = ["a", "b"],
        Arr = ["a", "b"],
        Numbers = [1, 2, 3],
        Colors = [TargetColor.Blue],
        Readings = [1, 2],
        Map = new Dictionary<string, int> { ["k"] = 1 },
        Stats = new ClrOnlyStats { Count = 7, Value = 8, Length = 9, Year = 2000, HasValue = true },
        Cost = new ClrOnlyMoney { Amount = 10m, Currency = "USD" },
        Tally = new ClrOnlyTally { Marks = ["x", "x", "x", "x"] }
    };

    private static readonly ClrOnlyDoc Bob = new()
    {
        Id = Guid.NewGuid(),
        Name = "Bob  ",
        Age = 40,
        Created = new DateTimeOffset(2025, 6, 1, 9, 0, 0, TimeSpan.Zero),
        Stamp = new DateTime(2025, 6, 2, 0, 0, 0, DateTimeKind.Utc),
        Day = new DateOnly(2025, 6, 1),
        At = new TimeOnly(1, 2, 3),
        Arr = ["c"]
    };

    private static readonly ClrOnlyDoc Cal = new()
    {
        Id = Guid.NewGuid(),
        Name = "Calvin",
        Age = 50,
        N = 7,
        Created = new DateTimeOffset(2026, 6, 1, 9, 0, 0, TimeSpan.Zero),
        Stamp = new DateTime(2026, 6, 3, 0, 0, 0, DateTimeKind.Utc),
        Day = new DateOnly(2026, 6, 1),
        At = new TimeOnly(1, 2, 3),
        Items = ["x"],
        Arr = ["a"]
    };

    private async Task<IQuerySession> SeededAsync()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Ann, Bob, Cal);
            await session.SaveChangesAsync(Token);
        }

        return theStore.QuerySession();
    }

    private static async Task<string[]> NamesAsync(IQuerySession query, Expression<Func<ClrOnlyDoc, bool>> predicate)
    {
        var docs = await query.Query<ClrOnlyDoc>().Where(predicate).ToListAsync(Token);
        return docs.Select(x => x.Name).Order().ToArray();
    }

    // ── The rows of the issue's table ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task nullable_value_reads_the_stored_value()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.N.Value == 5)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => 5 == x.N!.Value)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.N!.Value > 5)).ShouldBe(["Calvin"]);
    }

    [Fact]
    public async Task nullable_value_keeps_the_stored_form_of_the_member()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Flag!.Value)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Color!.Value == TargetColor.Green)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task has_value_compared_to_a_bool_is_a_null_check()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.N.HasValue == false)).ShouldBe(["Bob  "]);
        (await NamesAsync(query, x => false == x.N.HasValue)).ShouldBe(["Bob  "]);
        (await NamesAsync(query, x => x.N.HasValue != true)).ShouldBe(["Bob  "]);
        (await NamesAsync(query, x => x.N.HasValue == true)).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => x.N.HasValue != false)).ShouldBe(["Ann", "Calvin"]);
    }

    [Fact]
    public async Task has_value_alone_still_translates()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.N.HasValue)).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => !x.N.HasValue)).ShouldBe(["Bob  "]);
        (await NamesAsync(query, x => x.N.HasValue && x.N.Value > 5)).ShouldBe(["Calvin"]);
    }

    [Fact]
    public async Task has_value_selected_is_read_per_row()
    {
        await using var query = await SeededAsync();

        var rows = await query.Query<ClrOnlyDoc>().OrderBy(x => x.Age).Select(x => x.N.HasValue).ToListAsync(Token);

        rows.ShouldBe([true, false, true]);
    }

    [Fact]
    public async Task list_count_counts_the_stored_elements()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Items.Count > 0)).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => x.Items.Count == 2)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => 1 == x.Items.Count)).ShouldBe(["Calvin"]);
    }

    [Fact]
    public async Task an_empty_captured_nullable_answers_the_guard_before_the_query_runs()
    {
        await using var query = await SeededAsync();

        int? empty = null;
        (await NamesAsync(query, x => !empty.HasValue || x.Age != empty)).Length.ShouldBe(3);
        (await NamesAsync(query, x => empty == null || x.Age != empty)).Length.ShouldBe(3);
        (await NamesAsync(query, x => empty.HasValue && x.Age == empty)).ShouldBeEmpty();
    }

    [Fact]
    public async Task a_populated_captured_nullable_reaches_the_right_hand_side()
    {
        await using var query = await SeededAsync();

        int? thirty = 30;
        (await NamesAsync(query, x => !thirty.HasValue || x.Age != thirty)).ShouldBe(["Bob  ", "Calvin"]);
        (await NamesAsync(query, x => thirty.HasValue && x.Age == thirty.Value)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task the_right_side_of_a_decided_guard_is_never_evaluated()
    {
        await using var query = await SeededAsync();

        // captured.Value throws on an empty Nullable, so this only works if the guard short-circuits
        // the way the C# it was written as does.
        int? empty = null;
        (await NamesAsync(query, x => !empty.HasValue || x.Age != empty.Value)).Length.ShouldBe(3);
        (await NamesAsync(query, x => empty.HasValue && x.Age == empty.Value)).ShouldBeEmpty();
    }

    [Fact]
    public async Task an_empty_captured_value_with_no_guard_is_refused()
    {
        await using var query = await SeededAsync();

        int? empty = null;
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(
            () => NamesAsync(query, x => x.Age == empty.Value));

        ex.Message.ShouldContain("could not evaluate");
    }

    [Fact]
    public async Task a_nullable_held_directly_in_a_constant_is_read_without_a_target()
    {
        await using var query = await SeededAsync();

        // The shape a predicate has once something has inlined its captures: there is no closure for
        // the getter to run against, and an empty Nullable boxes to null.
        var x = Expression.Parameter(typeof(ClrOnlyDoc), "x");
        var hasValue = Expression.Property(Expression.Constant(null, typeof(int?)), nameof(Nullable<int>.HasValue));
        var predicate = Expression.Lambda<Func<ClrOnlyDoc, bool>>(
            Expression.OrElse(Expression.Not(hasValue), Expression.LessThan(Expression.Property(x, "Age"), Expression.Constant(0))), x);

        (await NamesAsync(query, predicate)).Length.ShouldBe(3);
    }

    [Fact]
    public async Task array_contains_under_csharp_14()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Arr.Contains("a"))).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => x.Numbers.Contains(2))).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => !x.Arr.Contains("a"))).ShouldBe(["Bob  "]);
    }

    [Fact]
    public async Task array_contains_where_csharp_14_passes_a_null_comparer()
    {
        await using var query = await SeededAsync();

        // An enum is not IEquatable<T>, so this binds to the three argument overload .NET 10 added.
        (await NamesAsync(query, x => x.Colors.Contains(TargetColor.Blue))).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task list_contains_is_the_control_and_still_works()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Items.Contains("a"))).ShouldBe(["Ann"]);
    }

    // ── The same class, found by reading ───────────────────────────────────────────────────────────

    [Fact]
    public async Task array_length_counts_the_stored_elements()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Arr.Length == 2)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Numbers.Length == 0)).ShouldBe(["Bob  ", "Calvin"]);
    }

    [Fact]
    public async Task a_null_collection_counts_as_empty_as_is_empty_does()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ClrOnlyDoc { Id = Guid.NewGuid(), Name = "none", Items = null! });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();

        (await NamesAsync(query, x => x.Items.Count == 0)).ShouldBe(["none"]);
        (await NamesAsync(query, x => x.Items.IsEmpty())).ShouldBe(["none"]);
    }

    [Fact]
    public async Task count_through_an_interface_and_on_a_dictionary()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Readings.Count == 2)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Map.Count == 1)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task date_time_offset_parts_are_the_local_parts_dotnet_reports()
    {
        await using var query = await SeededAsync();

        // In UTC this instant is 2023, so a translation that normalized would miss it.
        (await NamesAsync(query, x => x.Created.Year == 2024)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Created.Month == 1 && x.Created.Day == 1)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Created.Hour == 1)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Created.Minute == 20 && x.Created.Second == 30)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Created.Millisecond == 456)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task date_time_parts()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Stamp.Year == 2024)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stamp.DayOfYear == Ann.Stamp.DayOfYear)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stamp.Date == new DateTime(2025, 6, 2))).ShouldBe(["Bob  "]);
    }

    [Fact]
    public async Task day_of_week_matches_dotnet_whatever_the_server_thinks_a_week_starts_on()
    {
        await using var query = await SeededAsync();

        // Sunday is 0 in .NET, and 2024-03-15 was a Friday.
        Ann.Stamp.DayOfWeek.ShouldBe(DayOfWeek.Friday);
        (await NamesAsync(query, x => x.Stamp.DayOfWeek == DayOfWeek.Friday)).ShouldBe(["Ann"]);

        // 2024-02-29 was a Thursday, and the sunday in here is Bob's 2025-06-01.
        var day = DayOfWeek.Thursday;
        (await NamesAsync(query, x => x.Day.DayOfWeek == day)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Day.DayOfWeek == DayOfWeek.Sunday && x.Age == 40)).ShouldBe(["Bob  "]);

        var rows = await query.Query<ClrOnlyDoc>().OrderBy(x => x.Age).Select(x => x.Stamp.DayOfWeek).ToListAsync(Token);
        rows.ShouldBe([Ann.Stamp.DayOfWeek, Bob.Stamp.DayOfWeek, Cal.Stamp.DayOfWeek]);
    }

    [Fact]
    public async Task date_only_and_time_only_parts()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Day.Year == 2024 && x.Day.Month == 2 && x.Day.Day == 29)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.At.Hour == 13 && x.At.Minute == 14)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.At.Second == 15 && x.At.Millisecond == 678)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task a_date_part_of_a_nullable_date()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Due!.Value.Year == 2023)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task string_length_counts_trailing_spaces_as_dotnet_does()
    {
        await using var query = await SeededAsync();

        // LEN ignores them, so this used to be 3 and match the wrong rows.
        Bob.Name.Length.ShouldBe(5);
        (await NamesAsync(query, x => x.Name.Length == 5)).ShouldBe(["Bob  "]);
        (await NamesAsync(query, x => x.Name.Length == 3)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task string_length_on_a_string_id_reads_the_column()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ClrOnlyStringKeyed { Id = "four" }, new ClrOnlyStringKeyed { Id = "sevenxx" });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();
        var rows = await query.Query<ClrOnlyStringKeyed>().Where(x => x.Id.Length == 4).ToListAsync(Token);

        rows.Select(x => x.Id).ShouldBe(["four"]);
    }

    [Fact]
    public async Task a_nullable_struct_stored_as_an_object()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Cost.HasValue)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Cost.HasValue == false)).ShouldBe(["Bob  ", "Calvin"]);
        (await NamesAsync(query, x => x.Cost!.Value.Amount == 10m)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task arithmetic_over_translated_members()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Created.Year % 2 == 0)).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => x.Items.Count % 2 == 0 && x.Items.Count > 0)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Numbers.Length - x.Items.Count == 1)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task a_list_of_values_over_a_translated_member()
    {
        await using var query = await SeededAsync();

        var years = new[] { 2024, 2026 };
        (await NamesAsync(query, x => years.Contains(x.Created.Year))).ShouldBe(["Ann", "Calvin"]);
        (await NamesAsync(query, x => x.Items.Count.IsOneOf(2, 3))).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task a_cast_in_the_chain_does_not_lose_the_path()
    {
        await using var query = await SeededAsync();

        var rows = await query.Query<ClrOnlyDoc>().OrderByDescending(x => ((ClrOnlyStats)x.Stats).Count)
            .ToListAsync(Token);

        // It used to drop everything to the left of the cast, and order by a $.count nobody has.
        rows.First().Name.ShouldBe("Ann");
    }

    // ── A real member keeps its name ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task a_document_member_called_count_value_or_length_stays_a_json_key()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Stats.Count == 7)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stats.Value == 8)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stats.Length == 9)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task a_document_member_called_year_date_or_has_value_stays_a_json_key()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Stats.Year == 2000)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stats.HasValue)).ShouldBe(["Ann"]);
        (await NamesAsync(query, x => x.Stats.Date == default)).Length.ShouldBe(3);
    }

    [Fact]
    public async Task an_enumerable_with_a_converter_of_its_own_decides_its_own_shape()
    {
        await using var query = await SeededAsync();

        (await NamesAsync(query, x => x.Tally.Count == 4)).ShouldBe(["Ann"]);
    }

    [Fact]
    public async Task a_document_that_is_itself_enumerable_keeps_its_members()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(
            configure: json => json.Converters.Add(new ClrOnlyEnumerableDocConverter())));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new ClrOnlyEnumerableDoc { Id = Guid.NewGuid(), Name = "stored" });
            await session.SaveChangesAsync(Token);
        }

        await using var query = theStore.QuerySession();
        var rows = await query.Query<ClrOnlyEnumerableDoc>().Where(x => x.Name == "stored").ToListAsync(Token);

        rows.Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_real_member_keeps_its_name_in_a_select_and_an_order_by()
    {
        await using var query = await SeededAsync();

        var rows = await query.Query<ClrOnlyDoc>().OrderBy(x => x.Stats.Count).ThenBy(x => x.Age)
            .Select(x => x.Stats.Count).ToListAsync(Token);

        rows.ShouldBe([0, 0, 7]);
    }

    // ── Refused rather than run ────────────────────────────────────────────────────────────────────

    public static TheoryData<string, Expression<Func<ClrOnlyDoc, object>>> Untranslatable => new()
    {
        { "list Capacity", x => x.Items.Capacity },
        { "array Rank", x => x.Arr.Rank },
        { "array LongLength", x => x.Arr.LongLength },
        { "dictionary Keys", x => x.Map.Keys },
        { "dictionary Values", x => x.Map.Values },
        { "byte array Length", x => x.Bytes.Length },
        { "a collection's own Count", x => x.Bag.Count },
        { "a member of a collection", x => x.Bag.Label },
        { "DateTime Ticks", x => x.Stamp.Ticks },
        { "DateTime Kind", x => x.Stamp.Kind },
        { "DateTime TimeOfDay", x => x.Stamp.TimeOfDay },
        { "DateTimeOffset Offset", x => x.Created.Offset },
        { "DateTimeOffset UtcDateTime", x => x.Created.UtcDateTime },
        { "DateOnly DayNumber", x => x.Day.DayNumber },
        { "TimeOnly Ticks", x => x.At.Ticks },
        { "TimeSpan Hours", x => x.Span.Hours },
        { "TimeSpan TotalHours", x => x.Span.TotalHours },
        { "Guid Version", x => x.Id.Version },
        { "decimal Scale", x => x.Price.Scale }
    };

    [Theory]
    [MemberData(nameof(Untranslatable))]
    public async Task a_clr_only_member_with_no_translation_is_refused_in_a_select(
        string shape, Expression<Func<ClrOnlyDoc, object>> member)
    {
        await using var query = await SeededAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(
            () => query.Query<ClrOnlyDoc>().Select(member).ToListAsync(Token), shape);

        ex.Message.ShouldContain("exists only in .NET");
        ex.Message.ShouldContain("AdvancedSql");
    }

    [Theory]
    [MemberData(nameof(Untranslatable))]
    public async Task a_clr_only_member_with_no_translation_is_refused_in_an_order_by(
        string shape, Expression<Func<ClrOnlyDoc, object>> member)
    {
        await using var query = await SeededAsync();

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(
            () => query.Query<ClrOnlyDoc>().OrderBy(member).ToListAsync(Token), shape);

        ex.Message.ShouldContain("exists only in .NET");
    }

    [Fact]
    public async Task an_untranslatable_member_is_refused_in_a_where()
    {
        await using var query = await SeededAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Items.Capacity > 0));
        await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Bytes.Length > 0));
        await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Created.Offset == TimeSpan.Zero));
        await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Map.Keys.Contains("k")));
    }

    [Fact]
    public async Task a_refusal_names_what_does_translate()
    {
        await using var query = await SeededAsync();

        var date = await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Stamp.Ticks > 0));
        date.Message.ShouldContain("Year, Month, Day");
        date.Message.ShouldContain("DateTime.Ticks");

        var list = await Should.ThrowAsync<BadLinqExpressionException>(() => NamesAsync(query, x => x.Items.Capacity > 0));
        list.Message.ShouldContain("Count and Length");
        list.Message.ShouldContain("List.Capacity");
    }

    [Fact]
    public async Task a_member_of_a_scalar_an_earlier_select_projected_is_refused()
    {
        await using var query = await SeededAsync();

        // There is no stored member left to read it off, and $.length matched nothing.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyDoc>().Select(x => x.Name).Where(x => x.Length > 3).ToListAsync(Token));

        ex.Message.ShouldContain("exists only in .NET");
    }

    [Fact]
    public async Task a_chain_that_does_not_start_at_the_document_is_refused()
    {
        await using var query = await SeededAsync();

        var captured = new ClrOnlyStats();

        // Each of these silently became a top-level path: $.count, $.name and $.now.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyDoc>().OrderBy(x => captured.Count).ToListAsync(Token));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyDoc>().OrderBy(x => x.Name.Trim().Length).ToListAsync(Token));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ClrOnlyDoc>().OrderBy(x => DateTime.UtcNow).ToListAsync(Token));
    }
}

public class ClrOnlyStringKeyed
{
    public string Id { get; set; } = "";
}
