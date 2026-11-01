using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Polecat.TestUtils;
using Weasel.Core;

namespace Polecat.Tests.Linq;

public enum GroupedStatus
{
    Open,
    InProgress,

    [JsonStringEnumMemberName("all-done")]
    Done
}

public class GroupedDoc
{
    public Guid Id { get; set; }
    public string? Text { get; set; }
    public Guid Owner { get; set; }
    public DateTime When { get; set; }
    public DateTimeOffset Stamp { get; set; }
    public DateOnly Day { get; set; }
    public TimeOnly Time { get; set; }
    public GroupedStatus Status { get; set; }
    public bool Flag { get; set; }
    public decimal Price { get; set; }
    public double Score { get; set; }
    public long Big { get; set; }
    public int? Maybe { get; set; }
    public DateTime? MaybeWhen { get; set; }
}

/// <summary>
///     #837: GroupBy built its JSON by hand with <c>CONCAT</c>, treated everything but a string as a
///     number, and escaped only the double quote. The projection now comes from <c>JSON_OBJECT</c>, so
///     the server does the encoding. What matters is that every value survives the round trip into
///     the projected type, so that is what these assert.
/// </summary>
public class group_by_json_mapping_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static readonly DateTime Instant = new(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc);

    private async Task SeedAsync(params GroupedDoc[] docs)
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in docs)
        {
            doc.Id = Guid.NewGuid();
            session.Store(doc);
        }

        await session.SaveChangesAsync(Token);
    }

    private IQueryable<GroupedDoc> Docs()
    {
        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);
        return query.Query<GroupedDoc>();
    }

    [Theory]
    [InlineData(@"C:\temp")]                      // the issue's: \t became a tab
    [InlineData(@"trailing\")]                    // an unescaped one here escapes the closing quote
    [InlineData("say \"hi\"")]
    [InlineData("it's")]
    [InlineData("two\nlines")]
    [InlineData("a\r\nb\tc")]
    [InlineData("\u0001 control")]
    [InlineData("héllo wörld")]
    [InlineData("日本語 🐈")]
    [InlineData("</script>&amp;")]
    [InlineData("{\"looks\":\"like json\"}")]
    [InlineData("")]
    public async Task a_string_key_round_trips(string key)
    {
        await SeedAsync(new GroupedDoc { Text = key });

        var row = (await Docs().GroupBy(x => x.Text).Select(g => new { K = g.Key, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.K.ShouldBe(key);
        row.C.ShouldBe(1);
    }

    [Fact]
    public async Task a_guid_key()
    {
        var owner = Guid.NewGuid();
        await SeedAsync(new GroupedDoc { Owner = owner });

        var row = (await Docs().GroupBy(x => x.Owner).Select(g => new { K = g.Key, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.K.ShouldBe(owner);
    }

    [Fact]
    public async Task a_date_time_key_keeps_its_ticks()
    {
        var when = Instant.AddTicks(1234567);
        await SeedAsync(new GroupedDoc { When = when });

        var row = (await Docs().GroupBy(x => x.When).Select(g => new { K = g.Key, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.K.ShouldBe(when);
    }

    [Fact]
    public async Task a_date_time_offset_key_keeps_its_offset()
    {
        var stamp = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.FromHours(2)).AddTicks(1234567);
        await SeedAsync(new GroupedDoc { Stamp = stamp });

        var row = (await Docs().GroupBy(x => x.Stamp).Select(g => new { K = g.Key, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.K.ShouldBe(stamp);
        row.K.Offset.ShouldBe(TimeSpan.FromHours(2));
    }

    [Fact]
    public async Task a_date_only_key()
    {
        var day = new DateOnly(2026, 3, 4);
        await SeedAsync(new GroupedDoc { Day = day });

        (await Docs().GroupBy(x => x.Day).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(day);
    }

    [Fact]
    public async Task a_time_only_key()
    {
        var time = new TimeOnly(9, 30, 15);
        await SeedAsync(new GroupedDoc { Time = time });

        (await Docs().GroupBy(x => x.Time).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(time);
    }

    [Fact]
    public async Task a_bool_key()
    {
        await SeedAsync(new GroupedDoc { Flag = true }, new GroupedDoc { Flag = false }, new GroupedDoc { Flag = true });

        var rows = await Docs().GroupBy(x => x.Flag).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token);

        rows.Single(x => x.K).C.ShouldBe(2);
        rows.Single(x => !x.K).C.ShouldBe(1);
    }

    [Fact]
    public async Task numeric_keys()
    {
        await SeedAsync(new GroupedDoc { Price = 12.5m, Score = 0.1, Big = long.MaxValue });

        (await Docs().GroupBy(x => x.Price).Select(g => new { K = g.Key }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(12.5m);
        (await Docs().GroupBy(x => x.Score).Select(g => new { K = g.Key }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(0.1);
        (await Docs().GroupBy(x => x.Big).Select(g => new { K = g.Key }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(long.MaxValue);
    }

    [Fact]
    public async Task an_enum_key_stored_as_an_integer()
    {
        await SeedAsync(new GroupedDoc { Status = GroupedStatus.InProgress });

        (await Docs().GroupBy(x => x.Status).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(GroupedStatus.InProgress);

        (await Docs().GroupBy(x => x.Status).Select(g => g.Key).ToListAsync(Token))
            .ShouldHaveSingleItem().ShouldBe(GroupedStatus.InProgress);
    }

    [Theory]
    [InlineData(GroupedStatus.InProgress)]
    [InlineData(GroupedStatus.Done)]         // [JsonStringEnumMemberName]
    public async Task an_enum_key_stored_as_a_string(GroupedStatus status)
    {
        ConfigureStore(opts => opts.ConfigureSerialization(enumStorage: EnumStorage.AsString));

        await SeedAsync(new GroupedDoc { Status = status });

        (await Docs().GroupBy(x => x.Status).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBe(status);

        (await Docs().GroupBy(x => x.Status).Select(g => g.Key).ToListAsync(Token))
            .ShouldHaveSingleItem().ShouldBe(status);
    }

    [Fact]
    public async Task an_enum_stored_as_a_string_under_a_snake_case_policy()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(
            enumStorage: EnumStorage.AsString, casing: Casing.SnakeCase));

        await SeedAsync(new GroupedDoc { Status = GroupedStatus.InProgress });

        (await Docs().GroupBy(x => x.Status).Select(g => g.Key).ToListAsync(Token))
            .ShouldHaveSingleItem().ShouldBe(GroupedStatus.InProgress);
    }

    [Fact]
    public async Task a_null_nullable_key_is_one_group()
    {
        await SeedAsync(new GroupedDoc { Maybe = null }, new GroupedDoc { Maybe = null }, new GroupedDoc { Maybe = 3 });

        var rows = await Docs().GroupBy(x => x.Maybe).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token);

        rows.Count.ShouldBe(2);
        rows.Single(x => x.K == null).C.ShouldBe(2);
        rows.Single(x => x.K == 3).C.ShouldBe(1);
    }

    [Fact]
    public async Task a_null_string_key()
    {
        await SeedAsync(new GroupedDoc { Text = null });

        (await Docs().GroupBy(x => x.Text).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBeNull();
    }

    [Fact]
    public async Task a_null_date_time_key()
    {
        await SeedAsync(new GroupedDoc { MaybeWhen = null });

        (await Docs().GroupBy(x => x.MaybeWhen).Select(g => new { K = g.Key, C = g.Count() }).ToListAsync(Token))
            .ShouldHaveSingleItem().K.ShouldBeNull();
    }

    [Fact]
    public async Task a_scalar_null_key()
    {
        await SeedAsync(new GroupedDoc { Maybe = null });

        (await Docs().GroupBy(x => x.Maybe).Select(g => g.Key).ToListAsync(Token))
            .ShouldHaveSingleItem().ShouldBeNull();
    }

    [Fact]
    public async Task null_aggregates_over_a_group_of_nulls()
    {
        await SeedAsync(new GroupedDoc { Text = "x", Maybe = null }, new GroupedDoc { Text = "x", Maybe = null });

        var row = (await Docs().GroupBy(x => x.Text)
            .Select(g => new
            {
                Min = g.Min(x => x.Maybe),
                Max = g.Max(x => x.Maybe),
                Sum = g.Sum(x => x.Maybe),
                Average = g.Average(x => x.Maybe)
            })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Min.ShouldBeNull();
        row.Max.ShouldBeNull();
        row.Sum.ShouldBe(0);    // the same answer Enumerable.Sum gives for a sequence of nulls
        row.Average.ShouldBeNull();
    }

    [Fact]
    public async Task a_null_aggregate_into_a_non_nullable_member_is_the_default()
    {
        await SeedAsync(new GroupedDoc { Text = "x", Big = 5 });

        // A row written before the member existed has no such key, so MIN over it is NULL, and null is
        // not a value a long can hold. The same answer the whole-document read gives.
        await using (var conn = await OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"UPDATE {theStore.Options.DatabaseSchemaName}.pc_doc_groupeddoc "
                              + "SET data = JSON_MODIFY(CAST(data AS nvarchar(max)), '$.big', NULL)";
            await cmd.ExecuteNonQueryAsync(Token);
        }

        var row = (await Docs().GroupBy(x => x.Text)
            .Select(g => new { K = g.Key, Smallest = g.Min(x => x.Big) })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.K.ShouldBe("x");
        row.Smallest.ShouldBe(0L);
    }

    [Fact]
    public async Task a_date_aggregate_into_a_non_nullable_member()
    {
        // JSON_MODIFY, which is what a non-nullable member goes through, refuses a datetime2 outright.
        var when = Instant.AddTicks(1234567);
        await SeedAsync(new GroupedDoc { Text = "x", When = when });

        var row = (await Docs().GroupBy(x => x.Text)
            .Select(g => new { Latest = g.Max(x => x.When) })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Latest.ShouldBe(when);
    }

    [Fact]
    public async Task a_float_average_keeps_its_precision()
    {
        await SeedAsync(new GroupedDoc { Text = "x", Score = 2.1234567891 });

        var row = (await Docs().GroupBy(x => x.Text).Select(g => new { Average = g.Average(x => x.Score) })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Average.ShouldBe(2.1234567891, tolerance: 1e-12);
    }

    [Fact]
    public async Task a_decimal_sum_and_average()
    {
        await SeedAsync(new GroupedDoc { Text = "x", Price = 1.25m }, new GroupedDoc { Text = "x", Price = 2.25m });

        var row = (await Docs().GroupBy(x => x.Text)
            .Select(g => new { Sum = g.Sum(x => x.Price), Average = g.Average(x => x.Price) })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Sum.ShouldBe(3.5m);
        row.Average.ShouldBe(1.75m);
    }

    [Fact]
    public async Task a_composite_key_of_awkward_types()
    {
        var owner = Guid.NewGuid();
        await SeedAsync(new GroupedDoc { Owner = owner, When = Instant, Text = @"a\b", Status = GroupedStatus.Done, Maybe = null });

        var row = (await Docs().GroupBy(x => new { x.Owner, x.When, x.Text, x.Status, x.Maybe })
            .Select(g => new { g.Key.Owner, g.Key.When, g.Key.Text, g.Key.Status, g.Key.Maybe, Count = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Owner.ShouldBe(owner);
        row.When.ShouldBe(Instant);
        row.Text.ShouldBe(@"a\b");
        row.Status.ShouldBe(GroupedStatus.Done);
        row.Maybe.ShouldBeNull();
        row.Count.ShouldBe(1);
    }

    public class NamedRow
    {
        [JsonPropertyName("how_many")]
        public int Count { get; set; }

        public string? Key { get; set; }
    }

    public record PositionalRow(Guid Owner, int Count);

    [Fact]
    public async Task an_explicit_json_property_name_on_the_projection()
    {
        await SeedAsync(new GroupedDoc { Text = "x" });

        var row = (await Docs().GroupBy(x => x.Text)
            .Select(g => new NamedRow { Key = g.Key, Count = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Count.ShouldBe(1);
        row.Key.ShouldBe("x");
    }

    [Fact]
    public async Task a_positional_record_projection()
    {
        var owner = Guid.NewGuid();
        await SeedAsync(new GroupedDoc { Owner = owner });

        var row = (await Docs().GroupBy(x => x.Owner).Select(g => new PositionalRow(g.Key, g.Count()))
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.ShouldBe(new PositionalRow(owner, 1));
    }

    [Fact]
    public async Task a_scalar_date_only_and_time_only_key()
    {
        var day = new DateOnly(2026, 3, 4);
        var time = new TimeOnly(9, 30, 15);
        await SeedAsync(new GroupedDoc { Day = day, Time = time });

        (await Docs().GroupBy(x => x.Day).Select(g => g.Key).ToListAsync(Token)).ShouldHaveSingleItem().ShouldBe(day);
        (await Docs().GroupBy(x => x.Time).Select(g => g.Key).ToListAsync(Token)).ShouldHaveSingleItem().ShouldBe(time);
    }

    [Fact]
    public async Task a_store_without_the_native_json_type()
    {
        ConfigureStore(opts => opts.UseNativeJsonType = false);

        var owner = Guid.NewGuid();
        await SeedAsync(new GroupedDoc { Owner = owner, Text = @"C:\temp" });

        var row = (await Docs().GroupBy(x => new { x.Owner, x.Text })
            .Select(g => new { g.Key.Owner, g.Key.Text, C = g.Count() })
            .ToListAsync(Token)).ShouldHaveSingleItem();

        row.Owner.ShouldBe(owner);
        row.Text.ShouldBe(@"C:\temp");
    }
}

/// <summary>
///     The same JSON builder serves the streaming paths, which never met a non-nullable Guid or a bool
///     on the way in: JSON_MODIFY refuses the first outright (Msg 8116), and the second came back as
///     the string <c>"true"</c>.
/// </summary>
public class projected_json_value_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private async Task<GroupedDoc> SeedAsync()
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "Streaming a projection needs native JSON.");

        var doc = new GroupedDoc
        {
            Id = Guid.NewGuid(),
            Owner = Guid.NewGuid(),
            When = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(1234567),
            Stamp = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.FromHours(2)),
            Day = new DateOnly(2026, 3, 4),
            Time = new TimeOnly(9, 30, 15),
            Flag = true
        };

        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Token);

        return doc;
    }

    [Fact]
    public async Task a_non_nullable_guid_and_date_project_into_json()
    {
        var doc = await SeedAsync();

        await using var query = theStore.QuerySession();
        var json = await query.Query<GroupedDoc>()
            .Select(x => new { x.Id, x.Owner, x.When, x.Stamp, x.Day, x.Time })
            .ToJsonArrayAsync(Token);

        using var parsed = System.Text.Json.JsonDocument.Parse(json);
        var row = parsed.RootElement.EnumerateArray().Single();

        row.GetProperty("id").GetGuid().ShouldBe(doc.Id);
        row.GetProperty("owner").GetGuid().ShouldBe(doc.Owner);
        row.GetProperty("when").GetDateTime().ShouldBe(doc.When);
        row.GetProperty("stamp").GetDateTimeOffset().ShouldBe(doc.Stamp);
        row.GetProperty("day").GetString().ShouldBe("2026-03-04");
        row.GetProperty("time").GetString()!.ShouldStartWith("09:30:15");
    }

    [Fact]
    public async Task a_bool_projects_as_a_json_literal()
    {
        await SeedAsync();

        await using var query = theStore.QuerySession();
        var json = await query.Query<GroupedDoc>().Select(x => new { x.Flag }).ToJsonArrayAsync(Token);

        json.ShouldContain("\"flag\":true");
    }
}
