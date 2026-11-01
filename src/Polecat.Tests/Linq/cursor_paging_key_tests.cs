using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Linq.Members;
using Polecat.Tests.Harness;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Tests.Linq;

public enum KeyedStatus
{
    Alpha,
    Beta,

    [JsonStringEnumMemberName("gamma-ray")]
    Gamma
}

public class KeyedDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public DateTime When { get; set; }
    public DateTime? MaybeWhen { get; set; }
    public DateTimeOffset Stamp { get; set; }
    public DateOnly Day { get; set; }
    public TimeOnly Time { get; set; }
    public TimeSpan Span { get; set; }
    public KeyedStatus Status { get; set; }
    public bool Flag { get; set; }
    public byte Tiny { get; set; }
    public float Single { get; set; }
    public double Double { get; set; }
    public decimal Decimal { get; set; }
    public int? MaybeNumber { get; set; }
    public string? MaybeText { get; set; }
    public Address? Address { get; set; }
    public List<string> Tags { get; set; } = [];
    public Polecat.Tests.StrongTypedId.TeacherRef? Teacher { get; set; }
}

public class StringKeyedDoc
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

public class IntKeyedDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
///     #838 (the twin of marten#5630): a cursor's values have to come back as the same type the key
///     is stored as. The seek was bound without a type and the slot was read by the member's CLR type,
///     so anything those disagreed on paged forever, lost rows, or passed page 1 and threw on page 2.
/// </summary>
public class cursor_paging_key_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static readonly DateTime Instant = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private async Task SeedAsync(params KeyedDoc[] docs)
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in docs)
        {
            doc.Id = Guid.NewGuid();
            session.Store(doc);
        }

        await session.SaveChangesAsync(Token);
    }

    /// <summary>
    ///     Walks every page and returns the names in the order they arrived. The guard is what turns
    ///     "repeats the first page forever" into a failed assertion rather than a hung test.
    /// </summary>
    private async Task<List<string>> PageAllAsync(Func<IQueryable<KeyedDoc>, IQueryable<KeyedDoc>> ordered, int pageSize)
    {
        var seen = new List<string>();
        string? cursor = null;

        for (var guard = 0; guard < 40; guard++)
        {
            await using var query = theStore.QuerySession();
            var page = await ordered(query.Query<KeyedDoc>()).ToJsonPageByCursorAsync(cursor, pageSize, Token);

            using var items = JsonDocument.Parse(page.ItemsJson);
            seen.AddRange(items.RootElement.EnumerateArray().Select(x => x.GetProperty("name").GetString()!));

            cursor = page.NextCursor;
            if (cursor == null) return seen;
        }

        throw new ShouldAssertException($"Paging never finished. Saw: {string.Join(", ", seen.Take(12))}");
    }

    private static KeyedDoc[] Numbered(int count, Action<KeyedDoc, int> arrange)
        => Enumerable.Range(0, count).Select(i =>
        {
            var doc = new KeyedDoc { Id = Guid.NewGuid(), Name = $"n{i}" };
            arrange(doc, i);
            return doc;
        }).ToArray();

    // ---- the issue's table --------------------------------------------------------------------------

    [Fact]
    public async Task a_sub_millisecond_date_time_key_advances()
    {
        await SeedAsync(Numbered(4, (doc, i) => doc.When = Instant.AddTicks(1234567 + i * 1234567L)));

        (await PageAllAsync(q => q.OrderBy(x => x.When).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2", "n3"]);
    }

    [Fact]
    public async Task a_date_time_key_that_rounds_up_skips_nothing()
    {
        // datetime rounds .9999999 up to the next second, past the ticks of the row in between.
        await SeedAsync(
            new KeyedDoc { Name = "a", When = Instant.AddTicks(9999999) },
            new KeyedDoc { Name = "b", When = Instant.AddTicks(9999999 + 1) },
            new KeyedDoc { Name = "c", When = Instant.AddTicks(9999999 + 2) });

        (await PageAllAsync(q => q.OrderBy(x => x.When).ThenBy(x => x.Id), 1)).ShouldBe(["a", "b", "c"]);
    }

    [Fact]
    public async Task a_whole_second_date_time_key_still_pages()
    {
        await SeedAsync(Numbered(4, (doc, i) => doc.When = Instant.AddSeconds(i)));

        (await PageAllAsync(q => q.OrderBy(x => x.When).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2", "n3"]);
    }

    [Fact]
    public async Task a_descending_sub_millisecond_key()
    {
        await SeedAsync(Numbered(4, (doc, i) => doc.When = Instant.AddTicks(i * 3)));

        (await PageAllAsync(q => q.OrderByDescending(x => x.When).ThenBy(x => x.Id), 1))
            .ShouldBe(["n3", "n2", "n1", "n0"]);
    }

    [Fact]
    public async Task ties_on_a_sub_millisecond_key_fall_to_the_identity()
    {
        var when = Instant.AddTicks(1234567);
        await SeedAsync(Numbered(5, (doc, _) => doc.When = when));

        var seen = await PageAllAsync(q => q.OrderBy(x => x.When).ThenBy(x => x.Id), 2);

        seen.OrderBy(x => x).ShouldBe(["n0", "n1", "n2", "n3", "n4"]);
    }

    [Fact]
    public async Task a_sub_millisecond_date_time_offset_key()
    {
        var stamp = new DateTimeOffset(Instant).AddTicks(1234567);
        await SeedAsync(Numbered(4, (doc, i) => doc.Stamp = stamp.AddTicks(i)));

        (await PageAllAsync(q => q.OrderBy(x => x.Stamp).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2", "n3"]);
    }

    // The same root cause outside paging: the comparison is against a rounded instant.
    [Fact]
    public async Task a_where_clause_compares_a_date_time_at_its_full_precision()
    {
        var when = Instant.AddTicks(1234567);
        await SeedAsync(new KeyedDoc { Name = "x", When = when });

        await using var query = theStore.QuerySession();

        (await query.Query<KeyedDoc>().CountAsync(x => x.When == when, Token)).ShouldBe(1);
        (await query.Query<KeyedDoc>().CountAsync(x => x.When > when, Token)).ShouldBe(0);
    }

    // ---- key types that passed page 1 and threw on page 2 -----------------------------------------------

    [Fact]
    public async Task an_enum_key_stored_as_an_integer()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Status = (KeyedStatus)i));

        (await PageAllAsync(q => q.OrderBy(x => x.Status).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task an_enum_key_stored_as_a_string()
    {
        ConfigureStore(opts => opts.ConfigureSerialization(enumStorage: EnumStorage.AsString));
        await SeedAsync(Numbered(3, (doc, i) => doc.Status = (KeyedStatus)i));

        // By the stored names: alpha, beta, gamma-ray, which here is also declared order.
        (await PageAllAsync(q => q.OrderBy(x => x.Status).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_date_only_key()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Day = new DateOnly(2026, 3, 1 + i)));

        (await PageAllAsync(q => q.OrderBy(x => x.Day).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_time_only_key()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Time = new TimeOnly(9, 30 + i, 15).Add(TimeSpan.FromTicks(1234))));

        (await PageAllAsync(q => q.OrderBy(x => x.Time).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_time_span_key()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Span = TimeSpan.FromMinutes(10 + i)));

        (await PageAllAsync(q => q.OrderBy(x => x.Span).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_bool_key()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Flag = i == 2));

        var seen = await PageAllAsync(q => q.OrderBy(x => x.Flag).ThenBy(x => x.Id), 1);

        seen.Last().ShouldBe("n2");
        seen.Count.ShouldBe(3);
    }

    [Fact]
    public async Task a_byte_key_goes_by_the_same_text_comparison_the_order_by_uses()
    {
        // SqlTypeMap has no column type for a byte, so its locator is uncast and it sorts as text.
        await SeedAsync(
            new KeyedDoc { Name = "a", Tiny = 10 }, new KeyedDoc { Name = "b", Tiny = 2 },
            new KeyedDoc { Name = "c", Tiny = 200 });

        (await PageAllAsync(q => q.OrderBy(x => x.Tiny).ThenBy(x => x.Id), 1)).ShouldBe(["a", "b", "c"]);
    }

    [Fact]
    public async Task a_float_key_compares_as_stored()
    {
        await SeedAsync(Numbered(3, (doc, i) => doc.Single = 0.1f + i));

        (await PageAllAsync(q => q.OrderBy(x => x.Single).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task ties_on_a_float_key_stay_ties()
    {
        await SeedAsync(Numbered(4, (doc, _) => doc.Single = 0.1f));

        var seen = await PageAllAsync(q => q.OrderBy(x => x.Single).ThenBy(x => x.Id), 1);

        seen.OrderBy(x => x).ShouldBe(["n0", "n1", "n2", "n3"]);
    }

    [Fact]
    public async Task a_double_and_a_decimal_key()
    {
        await SeedAsync(Numbered(3, (doc, i) =>
        {
            doc.Double = 0.1 + i;
            doc.Decimal = 1.123456m + i;
        }));

        (await PageAllAsync(q => q.OrderBy(x => x.Double).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
        (await PageAllAsync(q => q.OrderBy(x => x.Decimal).ThenBy(x => x.Id), 1)).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_strong_typed_member_key()
    {
        var ids = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid()).ToArray();
        await SeedAsync(Numbered(3, (doc, i) => doc.Teacher = Polecat.Tests.StrongTypedId.TeacherRef.From(ids[i])));

        var seen = await PageAllAsync(q => q.OrderBy(x => x.Teacher).ThenBy(x => x.Id), 1);

        seen.OrderBy(x => x).ShouldBe(["n0", "n1", "n2"]);
    }

    [Fact]
    public async Task a_string_identity()
    {
        await using (var session = theStore.LightweightSession())
        {
            foreach (var id in new[] { "a", "b", "c" })
            {
                session.Store(new StringKeyedDoc { Id = id, Name = id });
            }

            await session.SaveChangesAsync(Token);
        }

        var seen = new List<string>();
        string? cursor = null;
        do
        {
            await using var query = theStore.QuerySession();
            var page = await query.Query<StringKeyedDoc>().OrderBy(x => x.Id).ToJsonPageByCursorAsync(cursor, 1, Token);
            seen.AddRange(JsonDocument.Parse(page.ItemsJson).RootElement.EnumerateArray()
                .Select(x => x.GetProperty("name").GetString()!));
            cursor = page.NextCursor;
        } while (cursor != null);

        seen.ShouldBe(["a", "b", "c"]);
    }

    [Fact]
    public async Task an_int_identity()
    {
        await using (var session = theStore.LightweightSession())
        {
            foreach (var id in new[] { 10, 20, 30 })
            {
                session.Store(new IntKeyedDoc { Id = id, Name = $"d{id}" });
            }

            await session.SaveChangesAsync(Token);
        }

        var seen = new List<string>();
        string? cursor = null;
        do
        {
            await using var query = theStore.QuerySession();
            var page = await query.Query<IntKeyedDoc>().OrderBy(x => x.Id).ToJsonPageByCursorAsync(cursor, 1, Token);
            seen.AddRange(JsonDocument.Parse(page.ItemsJson).RootElement.EnumerateArray()
                .Select(x => x.GetProperty("name").GetString()!));
            cursor = page.NextCursor;
        } while (cursor != null);

        seen.ShouldBe(["d10", "d20", "d30"]);
    }

    // ---- a key that cannot seek is refused on page 1 ---------------------------------------------------

    [Fact]
    public async Task a_complex_key_is_refused_on_the_first_page()
    {
        await SeedAsync(Numbered(2, (_, _) => { }));

        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<KeyedDoc>().OrderBy(x => x.Address).ThenBy(x => x.Id)
                .ToJsonPageByCursorAsync(null, 1, Token));
    }

    [Fact]
    public async Task a_collection_key_is_refused_on_the_first_page()
    {
        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(async () =>
            await query.Query<KeyedDoc>().OrderBy(x => x.Tags).ThenBy(x => x.Id)
                .ToJsonPageByCursorAsync(null, 1, Token));
    }

    // ---- a cursor is client input ------------------------------------------------------------------------

    private static string Cursor(string json) => "v1:" + Convert.ToBase64String(Encoding.UTF8.GetBytes(json));

    private Task<CursorPageResult> RequestAsync(string cursor)
    {
        var query = theStore.QuerySession();
        AsyncDisposables.Add(query);

        return query.Query<KeyedDoc>().OrderBy(x => x.Decimal).ThenBy(x => x.When).ThenBy(x => x.Id)
            .ToJsonPageByCursorAsync(cursor, 1, Token);
    }

    public static IEnumerable<object[]> MalformedSlots() =>
    [
        ["[1.5,\"2026-01-01T00:00:00\",\"not-a-guid\"]"],          // FormatException from GetGuid
        ["[1.5,\"not-a-date\",\"6f9619ff-8b86-d011-b42d-00cf4fc964ff\"]"],   // FormatException from GetDateTime
        ["[\"text\",\"2026-01-01T00:00:00\",\"6f9619ff-8b86-d011-b42d-00cf4fc964ff\"]"], // InvalidOperationException
        ["[{\"a\":1},\"2026-01-01T00:00:00\",\"6f9619ff-8b86-d011-b42d-00cf4fc964ff\"]"], // an object in a slot
        ["[1.5,\"2026-01-01T00:00:00\",null]"],                     // the identity is never NULL
        ["[1.5,\"2026-01-01T00:00:00\",7]"]
    ];

    [Theory]
    [MemberData(nameof(MalformedSlots))]
    public async Task a_malformed_slot_is_an_argument_exception(string payload)
    {
        var ex = await Should.ThrowAsync<ArgumentException>(() => RequestAsync(Cursor(payload)));

        ex.Message.ShouldStartWith("Malformed cursor payload.");
    }
}

/// <summary>
///     #838: <c>k &gt; NULL OR (k = NULL AND ...)</c> matches nothing, so the page after a NULL key was
///     empty and paging stopped. NULL ordering is SQL Server's own, first ascending and last descending;
///     these pin that every row arrives exactly once under it.
/// </summary>
public class cursor_paging_null_key_tests : OneOffConfigurationsContext
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static readonly DateTime Instant = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private async Task SeedAsync(params KeyedDoc[] docs)
    {
        await using var session = theStore.LightweightSession();
        foreach (var doc in docs)
        {
            doc.Id = Guid.NewGuid();
            session.Store(doc);
        }

        await session.SaveChangesAsync(Token);
    }

    private async Task<List<string>> PageAllAsync(Func<IQueryable<KeyedDoc>, IQueryable<KeyedDoc>> ordered, int pageSize)
    {
        var seen = new List<string>();
        string? cursor = null;

        for (var guard = 0; guard < 40; guard++)
        {
            await using var query = theStore.QuerySession();
            var page = await ordered(query.Query<KeyedDoc>()).ToJsonPageByCursorAsync(cursor, pageSize, Token);

            using var items = JsonDocument.Parse(page.ItemsJson);
            seen.AddRange(items.RootElement.EnumerateArray().Select(x => x.GetProperty("name").GetString()!));

            cursor = page.NextCursor;
            if (cursor == null) return seen;
        }

        throw new ShouldAssertException($"Paging never finished. Saw: {string.Join(", ", seen.Take(12))}");
    }

    private Task SeedTwoNullsAndTwoValuesAsync() => SeedAsync(
        new KeyedDoc { Name = "null1" },
        new KeyedDoc { Name = "null2" },
        new KeyedDoc { Name = "v1", MaybeWhen = Instant },
        new KeyedDoc { Name = "v2", MaybeWhen = Instant.AddDays(1) });

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task ascending_sees_every_row_once_with_the_nulls_first(int pageSize)
    {
        await SeedTwoNullsAndTwoValuesAsync();

        var seen = await PageAllAsync(q => q.OrderBy(x => x.MaybeWhen).ThenBy(x => x.Id), pageSize);

        seen.Take(2).OrderBy(x => x).ShouldBe(["null1", "null2"]);
        seen.Skip(2).ShouldBe(["v1", "v2"]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task descending_sees_every_row_once_with_the_nulls_last(int pageSize)
    {
        await SeedTwoNullsAndTwoValuesAsync();

        var seen = await PageAllAsync(q => q.OrderByDescending(x => x.MaybeWhen).ThenBy(x => x.Id), pageSize);

        seen.Take(2).ShouldBe(["v2", "v1"]);
        seen.Skip(2).OrderBy(x => x).ShouldBe(["null1", "null2"]);
    }

    [Fact]
    public async Task a_descending_identity_under_a_null_key()
    {
        await SeedTwoNullsAndTwoValuesAsync();

        var seen = await PageAllAsync(q => q.OrderBy(x => x.MaybeWhen).ThenByDescending(x => x.Id), 1);

        seen.OrderBy(x => x).ShouldBe(["null1", "null2", "v1", "v2"]);
    }

    [Fact]
    public async Task every_key_null()
    {
        await SeedAsync(new KeyedDoc { Name = "a" }, new KeyedDoc { Name = "b" }, new KeyedDoc { Name = "c" });

        var seen = await PageAllAsync(q => q.OrderBy(x => x.MaybeWhen).ThenBy(x => x.Id), 1);

        seen.OrderBy(x => x).ShouldBe(["a", "b", "c"]);
    }

    public static IEnumerable<object[]> Directions() =>
    [
        [false, false],
        [false, true],
        [true, false],
        [true, true]
    ];

    // Two nullable keys in all four direction combinations, over a grid with NULLs at both levels. One
    // page holding everything is the server's own order, and paging has to reproduce it exactly.
    [Theory]
    [MemberData(nameof(Directions))]
    public async Task two_nullable_keys_in_every_direction(bool firstDescending, bool secondDescending)
    {
        int?[] firsts = [null, 1, 2];
        string?[] seconds = [null, "a", "b"];

        var docs = (from first in firsts
            from second in seconds
            from copy in new[] { 0, 1 }
            select new KeyedDoc { Name = $"{first?.ToString() ?? "_"}{second ?? "_"}{copy}", MaybeNumber = first, MaybeText = second })
            .ToArray();

        await SeedAsync(docs);

        IQueryable<KeyedDoc> Ordered(IQueryable<KeyedDoc> q)
        {
            var once = firstDescending ? q.OrderByDescending(x => x.MaybeNumber) : q.OrderBy(x => x.MaybeNumber);
            var twice = secondDescending ? once.ThenByDescending(x => x.MaybeText) : once.ThenBy(x => x.MaybeText);
            return twice.ThenBy(x => x.Id);
        }

        var all = await PageAllAsync(Ordered, 1000);
        var paged = await PageAllAsync(Ordered, 1);
        var pagedInThrees = await PageAllAsync(Ordered, 3);

        all.Count.ShouldBe(docs.Length);
        paged.ShouldBe(all);
        pagedInThrees.ShouldBe(all);
    }

    [Fact]
    public async Task a_null_cursor_slot_on_a_nullable_key_is_a_real_seek()
    {
        await SeedTwoNullsAndTwoValuesAsync();

        await using var query = theStore.QuerySession();
        var first = await query.Query<KeyedDoc>().OrderBy(x => x.MaybeWhen).ThenBy(x => x.Id)
            .ToJsonPageByCursorAsync(null, 1, Token);

        var second = await query.Query<KeyedDoc>().OrderBy(x => x.MaybeWhen).ThenBy(x => x.Id)
            .ToJsonPageByCursorAsync(first.NextCursor, 1, Token);

        second.Count.ShouldBe(1);
        second.ItemsJson.ShouldContain("null");
    }
}

/// <summary>
///     The seek predicate's shape, rendered without a database, for what the paging tests can only see
///     indirectly: which parameter type went out.
/// </summary>
public class cursor_seek_predicate_tests
{
    private static (string Sql, Microsoft.Data.SqlClient.SqlParameter[] Parameters) Render(
        params (IQueryableMember Member, bool Descending, object? Value)[] keys)
    {
        var orderBy = keys.Select(x => (x.Member, x.Descending)).ToList();
        var predicate = CursorPagination.BuildSeekPredicate(orderBy, keys.Select(x => x.Value).ToArray());

        var builder = new BatchBuilder();
        predicate.Apply(builder);
        var command = builder.Compile().BatchCommands[0];

        return (command.CommandText, command.Parameters.Cast<Microsoft.Data.SqlClient.SqlParameter>().ToArray());
    }

    private static IQueryableMember Key(string locator, Type type, string? sqlType)
        => new QueryableMember(locator, locator, type, locatorSqlType: sqlType);

    private static readonly IQueryableMember Identity = new IdMember(typeof(Guid), locatorSqlType: "uniqueidentifier");

    [Fact]
    public void an_ascending_null_seeks_to_the_values()
    {
        var (sql, _) = Render((Key("k", typeof(int?), "int"), false, null), (Identity, false, Guid.NewGuid()));

        sql.ShouldContain("k IS NOT NULL");
        sql.ShouldContain("k IS NULL AND id >");
    }

    [Fact]
    public void a_descending_value_seeks_on_into_the_nulls()
    {
        var (sql, _) = Render((Key("k", typeof(int?), "int"), true, 5), (Identity, false, Guid.NewGuid()));

        sql.ShouldContain("k < @");
        sql.ShouldContain("OR k IS NULL");
    }

    [Fact]
    public void a_descending_null_only_has_its_ties_left()
    {
        var (sql, _) = Render((Key("k", typeof(int?), "int"), true, null), (Identity, false, Guid.NewGuid()));

        sql.ShouldNotContain("IS NOT NULL");
        sql.ShouldContain("k IS NULL AND id >");
    }

    [Theory]
    [InlineData("datetime2", System.Data.SqlDbType.DateTime2)]
    [InlineData("datetime", System.Data.SqlDbType.DateTime)]
    [InlineData("date", System.Data.SqlDbType.Date)]
    public void a_date_key_binds_its_own_type(string sqlType, System.Data.SqlDbType expected)
    {
        var (_, parameters) = Render(
            (Key("k", typeof(DateTime), sqlType), false, new DateTime(2026, 1, 1)), (Identity, false, Guid.NewGuid()));

        parameters[0].SqlDbType.ShouldBe(expected);
    }

    [Fact]
    public void a_datetime2_key_binds_every_digit()
    {
        var (_, parameters) = Render(
            (Key("k", typeof(DateTime), "datetime2"), false, new DateTime(2026, 1, 1)), (Identity, false, Guid.NewGuid()));

        parameters[0].Scale.ShouldBe((byte)7);
    }

    [Fact]
    public void a_declared_scale_is_kept()
    {
        var (_, parameters) = Render(
            (Key("k", typeof(DateTime), "datetime2(3)"), false, new DateTime(2026, 1, 1)), (Identity, false, Guid.NewGuid()));

        parameters[0].Scale.ShouldBe((byte)3);
    }

    [Fact]
    public void a_decimal_key_binds_its_precision_and_scale()
    {
        var (_, parameters) = Render(
            (Key("k", typeof(decimal), "decimal(18,6)"), false, 1.5m), (Identity, false, Guid.NewGuid()));

        parameters[0].SqlDbType.ShouldBe(System.Data.SqlDbType.Decimal);
        parameters[0].Precision.ShouldBe((byte)18);
        parameters[0].Scale.ShouldBe((byte)6);
    }

    [Fact]
    public void a_varchar_identity_binds_as_varchar()
    {
        var identity = new IdMember(typeof(string), locatorSqlType: "varchar(250)");

        var (_, parameters) = Render((identity, false, "abc"));

        // #363: an nvarchar probe against a varchar column is an implicit-conversion scan.
        parameters[0].SqlDbType.ShouldBe(System.Data.SqlDbType.VarChar);
        parameters[0].Size.ShouldBe(250);
    }

    [Fact]
    public void an_uncast_locator_binds_as_nvarchar()
    {
        var (_, parameters) = Render((Key("k", typeof(string), null), false, "abc"), (Identity, false, Guid.NewGuid()));

        parameters[0].SqlDbType.ShouldBe(System.Data.SqlDbType.NVarChar);
    }

    [Fact]
    public void a_type_it_cannot_bind_is_refused()
    {
        Should.Throw<BadLinqExpressionException>(() =>
            CursorPagination.ValidateOrdering([(Key("k", typeof(decimal), "money"), false), (Identity, false)]));
    }
}
