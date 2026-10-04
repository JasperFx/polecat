using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;
using Polecat.Tests.Harness;
using Shouldly;
using Weasel.SqlServer;
using Xunit;

namespace Polecat.Tests.Linq;

/// <summary>
///     #710 — a <c>Contains()</c> / <c>IsOneOf()</c> over a large value list.
/// </summary>
/// <remarks>
///     <para>
///         SQL Server rejects a command carrying more than 2100 parameters, and these rendered one
///         parameter per value, so the query threw rather than running. The same LINQ worked on Marten
///         and Fisher — PostgreSQL binds an array parameter — so it failed only on SQL Server, and only
///         once production data crossed the threshold. Found from CritterWatch, whose store-agnostic
///         core batches agent-health reads by id: one monitored node can run thousands of agents, so
///         the batch crosses 2100 in the field and never in a test fleet.
///     </para>
///     <para>
///         The ceiling was also lower than 2100 and query-dependent, because the budget belongs to the
///         command — 2000 values passed and 2099 failed purely because the rest of the query carried
///         parameters too. That is why the threshold here is low rather than near the limit; see
///         <see cref="JsonValueList" />.
///     </para>
/// </remarks>
public class large_in_list_tests: OneOffConfigurationsContext
{
    public class Doc
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    // ---- unit: the rendered shape, which is where the sargability guarantee lives --------------

    /// <summary>A member whose locator and SQL type are whatever the test needs them to be.</summary>
    private sealed record StubMember(string TypedLocator, string? LocatorSqlType, Type MemberType)
        : IQueryableMember
    {
        public string RawLocator => TypedLocator;
        public bool IsBoolean => false;
        public object? ConvertValue(object? value) => value;
    }

    private static (string Sql, object?[] Parameters) Render(InFilter filter)
    {
        var builder = new BatchBuilder();
        filter.Apply(builder);
        var cmd = builder.Compile().BatchCommands[0];
        return (cmd.CommandText,
            cmd.Parameters.Cast<SqlParameter>().Select(p => p.Value).ToArray());
    }

    private static InFilter FilterOver(int count, string? sqlType = "varchar(250)")
    {
        var member = new StubMember("id", sqlType, typeof(string));
        var values = Enumerable.Range(0, count).Select(i => (object?)$"doc-{i}").ToList();
        return new InFilter(member.TypedLocator, member, values);
    }

    [Fact]
    public void below_the_threshold_still_binds_one_parameter_per_value()
    {
        // Deliberately unchanged below the threshold: a short IN list hands the optimizer real
        // literals and a real row count, where OPENJSON is costed at a fixed guess. Switching
        // unconditionally would change the plan of every small IsOneOf for no correctness gain.
        var (sql, parameters) = Render(FilterOver(JsonValueList.Threshold));

        parameters.Length.ShouldBe(JsonValueList.Threshold);
        sql.ShouldNotContain("OPENJSON");
    }

    [Fact]
    public void above_the_threshold_binds_one_json_array_parameter()
    {
        var (sql, parameters) = Render(FilterOver(JsonValueList.Threshold + 1));

        parameters.Length.ShouldBe(1);
        sql.ShouldContain("OPENJSON");
        parameters[0].ShouldBeOfType<string>().ShouldStartWith("[\"doc-0\"");
    }

    /// <summary>
    ///     The unpacked column is typed to what the LOCATOR produces.
    /// </summary>
    /// <remarks>
    ///     ⚠️ This is the #363 guarantee and the assertion most worth having. OPENJSON's untyped
    ///     <c>value</c> is <c>nvarchar(4000)</c>; compared against a <c>varchar(250)</c> computed
    ///     column that puts <c>CONVERT_IMPLICIT</c> on the COLUMN and scans, which makes the
    ///     computed-column indexes of #223/#684 dead weight. Nothing else here would notice: the query
    ///     returns the right rows either way, just slowly, on exactly the large lists this feature
    ///     exists for.
    /// </remarks>
    [Fact]
    public void the_unpacked_column_is_typed_to_the_locators_own_type()
    {
        Render(FilterOver(JsonValueList.Threshold + 1, "varchar(250)"))
            .Sql.ShouldContain("WITH ([value] varchar(250) '$')");

        Render(FilterOver(JsonValueList.Threshold + 1, "int"))
            .Sql.ShouldContain("WITH ([value] int '$')");

        // An uncast locator -- a bare JSON_VALUE over a string or bool member -- falls back to
        // OPENJSON's own nvarchar, which is also what JSON_VALUE returns. Both sides agree, and
        // neither is converted. Guessing varchar(250) here would TRUNCATE a long value, which is a
        // wrong answer rather than a slow one.
        Render(FilterOver(JsonValueList.Threshold + 1, null))
            .Sql.ShouldContain("WITH ([value] nvarchar(4000) '$')");
    }

    [Fact]
    public void a_type_the_json_writer_cannot_bind_is_refused_with_a_reason()
    {
        var member = new StubMember("id", "varchar(250)", typeof(object));
        var values = Enumerable.Range(0, JsonValueList.Threshold + 1)
            .Select(_ => (object?)new Uri("https://example.com")).ToList();

        // A refusal rather than ToString(): an untyped value would otherwise compare as whatever its
        // ToString happens to produce, and this path is reached only above the threshold -- so it
        // would appear in the field and never in a small test.
        Should.Throw<BadLinqExpressionException>(
                () => Render(new InFilter(member.TypedLocator, member, values)))
            .Message.ShouldContain("large IN list");
    }

    [Fact]
    public void an_empty_list_is_still_always_false()
    {
        var member = new StubMember("id", "varchar(250)", typeof(string));
        Render(new InFilter(member.TypedLocator, member, new List<object?>())).Sql.ShouldContain("1=0");
    }

    // ---- integration: the repro, and that the answers are right ------------------------------

    private async Task<string[]> StoredIdsAsync()
    {
        ConfigureStore(opts => opts.Schema.For<Doc>());

        var stored = Enumerable.Range(0, 10).Select(i => $"doc-{i}").ToArray();
        await using var session = theStore.LightweightSession();
        foreach (var id in stored) session.Store(new Doc { Id = id, Name = id });
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);

        return stored;
    }

    [Fact]
    public async Task contains_over_three_thousand_ids()
    {
        var stored = await StoredIdsAsync();
        var probe = Enumerable.Range(0, 3000).Select(i => $"doc-{i}").ToArray();

        await using var query = theStore.QuerySession();

        // Threw "The incoming request has too many parameters" before #710 -- and at 2099, not 2100,
        // because the rest of the query carried parameters too.
        var found = await query.Query<Doc>().Where(x => probe.Contains(x.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Select(x => x.Id).OrderBy(x => x).ShouldBe(stored.OrderBy(x => x));
    }

    [Fact]
    public async Task is_one_of_over_three_thousand_ids()
    {
        var stored = await StoredIdsAsync();
        var probe = Enumerable.Range(0, 3000).Select(i => $"doc-{i}").ToArray();

        await using var query = theStore.QuerySession();
        var found = await query.Query<Doc>().Where(x => x.Id.IsOneOf(probe))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Select(x => x.Id).OrderBy(x => x).ShouldBe(stored.OrderBy(x => x));
    }

    /// <summary>
    ///     A large list that matches NOTHING still matches nothing — the fact that would catch an
    ///     OPENJSON form accidentally rendering as an unconstrained join.
    /// </summary>
    [Fact]
    public async Task a_large_list_with_no_matches_returns_nothing()
    {
        await StoredIdsAsync();
        var probe = Enumerable.Range(5000, 3000).Select(i => $"doc-{i}").ToArray();

        await using var query = theStore.QuerySession();
        (await query.Query<Doc>().Where(x => probe.Contains(x.Id))
            .ToListAsync(TestContext.Current.CancellationToken)).ShouldBeEmpty();
    }

    /// <summary>
    ///     A null in the list matches nothing rather than erroring, which is what the
    ///     one-parameter-per-value form did too.
    /// </summary>
    [Fact]
    public async Task nulls_in_a_large_list_match_nothing_without_failing()
    {
        var stored = await StoredIdsAsync();
        var probe = Enumerable.Range(0, 3000).Select(i => i % 500 == 0 ? null : $"doc-{i}").ToArray();

        await using var query = theStore.QuerySession();
        var found = await query.Query<Doc>().Where(x => probe.Contains(x.Id))
            .ToListAsync(TestContext.Current.CancellationToken);

        // doc-0 is one of the nulled-out slots, so it drops out; everything else still matches.
        found.Select(x => x.Id).OrderBy(x => x).ShouldBe(stored.Where(x => x != "doc-0").OrderBy(x => x));
    }
}
