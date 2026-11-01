using System.Linq.Expressions;
using System.Text.Json;
using JasperFx.Events.Projections;
using Polecat.Linq;
using Polecat.Linq.SqlGeneration;
using Polecat.Patching;
using Polecat.Projections.Flattened;
using Polecat.Tests.Harness;

namespace Polecat.Tests;

/// <summary>
///     #848. Whatever Polecat renders into SQL text, a JSON path or an identifier has to come out the
///     same on every host. .NET formats with the thread's culture unless told otherwise, and the three
///     ways that goes wrong are all in here: <c>th-TH</c>, <c>ar-SA</c> and <c>fa-IR</c> count years
///     differently, <c>sv-SE</c> and friends write a negative number with U+2212 rather than a hyphen,
///     and <c>tr-TR</c> lowers <c>I</c> to a dotless <c>ı</c>.
/// </summary>
/// <remarks>
///     Each test opens its <see cref="CultureScope" /> before it touches the store, so the whole thing
///     runs as a host in that culture would. An empty name is the invariant culture and is the control:
///     a row that fails on the invariant culture too is not a culture bug.
/// </remarks>
public class culture_independence_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateOnly TheDay = new(2026, 10, 31);
    private static readonly TimeOnly TheTime = new(14, 30, 15);

    private async Task<CultureDoc> StoreAsync(Action<CultureDoc> configure)
    {
        var doc = new CultureDoc { Id = Guid.NewGuid(), Day = TheDay, At = TheTime };
        configure(doc);

        await using var session = theStore.LightweightSession();
        session.Store(doc);
        await session.SaveChangesAsync(Ct);

        return doc;
    }

    // ---- large IN lists -----------------------------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("th-TH")]    // Buddhist calendar: 2026 is 2569
    [InlineData("ar-SA")]    // Hijri calendar
    [InlineData("fa-IR")]    // Persian calendar
    public async Task a_date_list_above_the_json_threshold_matches_in_any_calendar(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var match = await StoreAsync(_ => { });

        var days = Enumerable.Range(0, JsonValueList.Threshold * 3)
            .Select(i => TheDay.AddDays(-i))
            .ToList();
        days.Count.ShouldBeGreaterThan(JsonValueList.Threshold);

        await using var query = theStore.QuerySession();
        var found = await query.Query<CultureDoc>().Where(x => days.Contains(x.Day)).ToListAsync(Ct);

        found.ShouldHaveSingleItem().Id.ShouldBe(match.Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData("fi-FI")]    // the time separator is a period
    [InlineData("da-DK")]    // and here
    [InlineData("th-TH")]
    public async Task a_time_list_above_the_json_threshold_matches_with_any_separator(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var match = await StoreAsync(_ => { });

        var times = Enumerable.Range(0, JsonValueList.Threshold * 2)
            .Select(i => TheTime.AddMinutes(-i))
            .ToList();
        times.Count.ShouldBeGreaterThan(JsonValueList.Threshold);

        await using var query = theStore.QuerySession();
        var found = await query.Query<CultureDoc>().Where(x => times.Contains(x.At)).ToListAsync(Ct);

        found.ShouldHaveSingleItem().Id.ShouldBe(match.Id);
    }

    // ---- literals in SQL text -----------------------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("sv-SE")]    // U+2212 MINUS SIGN
    [InlineData("ar-SA")]    // U+061C ARABIC LETTER MARK, then a hyphen
    [InlineData("fa-IR")]    // U+200E LEFT-TO-RIGHT MARK, then U+2212
    public async Task a_negative_operand_of_a_modulo_comparison_is_valid_sql(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var match = await StoreAsync(x => x.Number = -7);
        await StoreAsync(x => x.Number = 7);

        await using var query = theStore.QuerySession();

        // T-SQL's % keeps the sign of the dividend, the same as C#'s
        var left = await query.Query<CultureDoc>().Where(x => x.Number % 3 == -1).ToListAsync(Ct);
        left.ShouldHaveSingleItem().Id.ShouldBe(match.Id);

        var right = await query.Query<CultureDoc>().Where(x => -1 == x.Number % 3).ToListAsync(Ct);
        right.ShouldHaveSingleItem().Id.ShouldBe(match.Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData("de-DE")]    // a decimal comma, which in SQL text is a second column
    [InlineData("ar-SA")]    // U+066B ARABIC DECIMAL SEPARATOR
    public async Task a_fractional_operand_of_a_modulo_comparison_is_valid_sql(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var match = await StoreAsync(x => x.Price = 6m);
        await StoreAsync(x => x.Price = 7m);

        await using var query = theStore.QuerySession();
        var found = await query.Query<CultureDoc>().Where(x => x.Price % 2.5m == 1m).ToListAsync(Ct);

        found.ShouldHaveSingleItem().Id.ShouldBe(match.Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sv-SE")]
    public async Task a_negative_insert_index_in_a_patch_is_valid_sql(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var doc = await StoreAsync(x => x.Numbers = [1, 2]);

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<CultureDoc>(doc.Id).Insert(x => x.Numbers, 9, index: -1);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<CultureDoc>(doc.Id, Ct))!.Numbers.ShouldBe([9, 1, 2]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sv-SE")]
    public async Task a_negative_index_on_insert_if_not_exists_is_valid_sql(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        var scalars = await StoreAsync(x => x.Numbers = [1, 2]);
        var complex = await StoreAsync(x => x.Children = [new CultureChild("one")]);

        await using (var session = theStore.LightweightSession())
        {
            session.Patch<CultureDoc>(scalars.Id).InsertIfNotExists(x => x.Numbers, 9, index: -1);
            session.Patch<CultureDoc>(complex.Id).InsertIfNotExists(x => x.Children, new CultureChild("zero"), index: -1);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<CultureDoc>(scalars.Id, Ct))!.Numbers.ShouldBe([9, 1, 2]);
        (await query.LoadAsync<CultureDoc>(complex.Id, Ct))!.Children.Select(x => x.Name).ShouldBe(["zero", "one"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("sv-SE")]
    [InlineData("ar-SA")]
    public async Task a_negative_literal_in_a_flat_table_is_valid_sql(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        ConfigureStore(opts => opts.Projections.Add(new CultureFlatProjection(), ProjectionLifecycle.Inline));
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var stream = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(stream, new CultureInvoiceIssued(Guid.NewGuid(), 3));
            await session.SaveChangesAsync(Ct);
        }

        // Twice, because the literal is rendered once for the INSERT arm and once for the UPDATE arm
        await using (var session = theStore.LightweightSession())
        {
            session.Events.Append(stream, new CultureInvoiceIssued(Guid.NewGuid(), 4));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var balance = await query.AdvancedSql.QueryAsync<int>(
            $"select balance from {CultureFlatProjection.Schema}.culture_flat", Ct);

        balance.ShouldHaveSingleItem().ShouldBe(-1);
    }

    [Fact]
    public void a_literal_column_value_renders_the_same_on_every_host()
    {
        var map = new SetIntValueMap("balance", -1);

        using var scope = CultureScope.Formatting("sv-SE");

        map.InsertExpression("@p0").ShouldBe("-1");
        map.UpdateExpression("@p0").ShouldBe("[balance] = -1");
    }

    // ---- JSON paths ---------------------------------------------------------------------------------

    [Theory]
    [InlineData("sv-SE")]
    [InlineData("ar-SA")]
    public void a_negative_dictionary_key_makes_the_same_patch_path_on_every_host(string culture)
    {
        // At the path, and not against the database: a key that starts with a digit or a sign is not
        // quoted, so JSON_MODIFY refuses it on every host. That is a separate gap.
        Expression<Func<CultureDoc, string>> negative = x => x.ByNumber[-1];
        var invariant = JsonPathHelper.ToPath(negative, JsonNamingPolicy.CamelCase);

        using var scope = CultureScope.Formatting(culture);

        JsonPathHelper.ToPath(negative, JsonNamingPolicy.CamelCase).ShouldBe(invariant);
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("ar-SA")]
    public void a_fractional_dictionary_key_makes_the_same_patch_path_on_every_host(string culture)
    {
        Expression<Func<CultureDoc, string>> fractional = x => x.ByPrice[1.5m];

        using var scope = CultureScope.Formatting(culture);

        // System.Text.Json writes a dictionary key the invariant way, so this is what is in the document
        JsonPathHelper.ToPath(fractional, JsonNamingPolicy.CamelCase).ShouldEndWith("1.5");
    }

    // ---- identifiers --------------------------------------------------------------------------------

    [Fact]
    public void a_default_column_name_is_the_same_on_every_host()
    {
        var invariant = new CultureFlatProjection().Table.Columns.Select(x => x.Name).ToArray();
        invariant.ShouldContain("invoice_id");
        invariant.ShouldContain("item_count");

        using var scope = CultureScope.Formatting("tr-TR");

        // Control: this is the culture the mapping has to survive
        char.ToLower('I').ShouldBe('ı');

        new CultureFlatProjection().Table.Columns.Select(x => x.Name).ToArray().ShouldBe(invariant);
    }

    // ---- reading numbers back -----------------------------------------------------------------------

    [Theory]
    [InlineData("")]
    [InlineData("de-DE")]    // a period is the group separator here, so "1.5" parses as 15
    public async Task a_scalar_projection_reads_the_same_number_on_every_host(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        await StoreAsync(x =>
        {
            x.Price = 1.5m;
            x.Ratio = 2.5;
        });

        await using var query = theStore.QuerySession();

        (await query.Query<CultureDoc>().Select(x => x.Price).ToListAsync(Ct)).ShouldBe([1.5m]);
        (await query.Query<CultureDoc>().Select(x => x.Ratio).ToListAsync(Ct)).ShouldBe([2.5]);
        (await query.Query<CultureDoc>().MaxAsync(x => x.Price, Ct)).ShouldBe(1.5m);
        (await query.Query<CultureDoc>().SumAsync(x => x.Price, Ct)).ShouldBe(1.5m);
    }

    [Theory]
    [InlineData("")]
    [InlineData("de-DE")]
    public async Task a_raw_sql_number_read_from_json_text_is_the_same_on_every_host(string culture)
    {
        using var scope = CultureScope.Formatting(culture);

        await using var query = theStore.QuerySession();

        // JSON_VALUE hands back text, so this is the conversion rather than the cast
        const string sql = """select JSON_VALUE('{"price":1.5}', '$.price')""";

        (await query.AdvancedSql.QueryAsync<decimal>(sql, Ct)).ShouldBe([1.5m]);

        var streamed = new List<decimal>();
        await foreach (var price in query.AdvancedSql.StreamAsync<decimal>(sql, Ct))
        {
            streamed.Add(price);
        }

        streamed.ShouldBe([1.5m]);
    }
}

public record CultureChild(string Name);

public class CultureDoc
{
    public Guid Id { get; set; }
    public int Number { get; set; }
    public decimal Price { get; set; }
    public double Ratio { get; set; }
    public DateOnly Day { get; set; }
    public TimeOnly At { get; set; }
    public List<int> Numbers { get; set; } = [];
    public List<CultureChild> Children { get; set; } = [];
    public Dictionary<int, string> ByNumber { get; set; } = [];
    public Dictionary<decimal, string> ByPrice { get; set; } = [];
}

public record CultureInvoiceIssued(Guid InvoiceId, int ItemCount);

public class CultureFlatProjection : FlatTableProjection
{
    // The one the harness drops between facts, because it is named after the test class
    public const string Schema = nameof(culture_independence_tests);

    public CultureFlatProjection() : base("culture_flat", Schema)
    {
        Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
        Table.AddColumn("balance", "int");

        Project<CultureInvoiceIssued>(map =>
        {
            // No column names, so these take the default one the mapping derives
            map.Map(x => x.InvoiceId);
            map.Map(x => x.ItemCount);
            map.SetValue("balance", -1);
        });
    }
}
