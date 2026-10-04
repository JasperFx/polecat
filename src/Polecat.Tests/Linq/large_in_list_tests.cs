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

    // ---- #721: the command-level ceiling ------------------------------------------------------

    /// <summary>
    ///     A builder that reports no information about its parameter count, the way one compiled
    ///     against a Weasel older than 9.40.0 does.
    /// </summary>
    private sealed class UncountedBuilder : Weasel.Core.ICommandBuilder
    {
        public string TenantId { get; set; } = string.Empty;
        public string? LastParameterName => null;

        // The interface's own default. "No information", NOT "none bound".
        public int ParameterCount => Weasel.Core.ICommandBuilder.UnknownParameterCount;

        public void Append(string sql) { }
        public void Append(char character) { }
        public void AppendParameters(params object[] parameters) { }
        public System.Data.Common.DbParameter AppendParameter(object value) => throw new NotSupportedException();
        public Weasel.Core.IGroupedParameterBuilder CreateGroupedParameterBuilder(char? separator = null) => throw new NotSupportedException();
        public System.Data.Common.DbParameter[] AppendWithDbParameters(string text) => [];
        public System.Data.Common.DbParameter[] AppendWithDbParameters(string text, char placeholder) => [];
        public void StartNewCommand() { }
        public void AddParameters(object parameters) { }
        public void AddParameters(IDictionary<string, object?> parameters) { }
        public void AddParameters<T>(IDictionary<string, T> parameters) { }
    }

    [Fact]
    public void the_budget_is_weasels_rather_than_a_restated_constant()
    {
        // Read from SqlServerMigrator so a Weasel change moves both together, and deliberately below
        // SQL Server's hard 2100 -- the slack is for parameters bound AFTER this fragment, which it
        // cannot see.
        JsonValueList.Budget.ShouldBe(new SqlServerMigrator().MaxParametersPerCommand);
        JsonValueList.Budget.ShouldBeLessThan(2100);
    }

    [Fact]
    public void a_small_list_on_a_fresh_command_is_still_one_parameter_per_value()
    {
        // The ceiling must not disturb the floor's answer when there is budget to spare.
        JsonValueList.ShouldBindAsJsonArray(new BatchBuilder(), 10).ShouldBeFalse();
    }

    [Fact]
    public void a_small_list_becomes_an_array_once_the_command_has_spent_its_budget()
    {
        // ⚠️ THE #721 fact. 50 values is far under the floor, so before this the fragment bound 50
        // more parameters onto a command already holding 1,990 -- 2,040, and climbing toward the
        // server's refusal with every further filter.
        var builder = new BatchBuilder();
        for (var i = 0; i < JsonValueList.Budget - 10; i++) builder.AppendParameter($"x{i}");

        builder.ParameterCount.ShouldBe(JsonValueList.Budget - 10);
        JsonValueList.ShouldBindAsJsonArray(builder, 50).ShouldBeTrue();

        // And exactly at the budget it is still allowed through, so the boundary is not off by one.
        var atBudget = new BatchBuilder();
        for (var i = 0; i < JsonValueList.Budget - 10; i++) atBudget.AppendParameter($"x{i}");
        JsonValueList.ShouldBindAsJsonArray(atBudget, 10).ShouldBeFalse();
    }

    [Fact]
    public void a_builder_that_cannot_count_falls_back_to_the_floor_alone()
    {
        // -1 means "no information", so the ceiling is skipped and behaviour is exactly as it was
        // before #721. Treating -1 as zero would read as "nothing bound" on a full command; treating
        // it as full would turn every small IsOneOf into an OPENJSON join.
        var uncounted = new UncountedBuilder();

        JsonValueList.ShouldBindAsJsonArray(uncounted, 10).ShouldBeFalse();
        JsonValueList.ShouldBindAsJsonArray(uncounted, JsonValueList.Threshold + 1).ShouldBeTrue();
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

    /// <summary>
    ///     ⚠️ <b>The #721 repro: MANY SMALL lists on one command.</b>
    /// </summary>
    /// <remarks>
    ///     Twenty-five filters of a hundred values. Every list is at or under the floor, so each one
    ///     looks harmless on its own and no per-fragment threshold can see the problem — but together
    ///     they bound 2,500 parameters on a single command and SQL Server refuses over 2,100. This is
    ///     the case the low threshold could not reach at any value, because lowering it far enough to
    ///     cover twenty fragments would turn every single-digit <c>IsOneOf</c> in the store into an
    ///     OPENJSON join.
    /// </remarks>
    [Fact]
    public async Task many_small_lists_on_one_command_stay_under_the_server_limit()
    {
        var stored = await StoredIdsAsync();

        await using var query = theStore.QuerySession();
        IQueryable<Doc> queryable = query.Query<Doc>();

        for (var f = 0; f < 25; f++)
        {
            // Each probe carries every stored id plus distinct filler, so the intersection of all
            // twenty-five is exactly the stored set and an over-wide render would be visible.
            var probe = stored
                .Concat(Enumerable.Range(f * 1000, JsonValueList.Threshold - stored.Length)
                    .Select(i => $"filler-{i}"))
                .ToArray();

            probe.Length.ShouldBe(JsonValueList.Threshold);
            queryable = queryable.Where(x => probe.Contains(x.Id));
        }

        var found = await queryable.ToListAsync(TestContext.Current.CancellationToken);

        found.Select(x => x.Id).OrderBy(x => x).ShouldBe(stored.OrderBy(x => x));
    }

    /// <summary>
    ///     The example weasel#675 was filed for, pinned because it is NOT what #721 fixes.
    /// </summary>
    /// <remarks>
    ///     Two filters of 1,500 values were the motivating case for exposing the parameter count —
    ///     "both under any per-fragment ceiling and together over the server's". That stopped being
    ///     true the moment #710 shipped with a low threshold: 1,500 is over the floor, so each
    ///     fragment already became a single array parameter on its own. Kept as a fact so the claim in
    ///     <see cref="JsonValueList" />'s remarks stays checkable rather than remembered.
    /// </remarks>
    [Fact]
    public async Task two_large_lists_on_one_command_were_already_safe()
    {
        var stored = await StoredIdsAsync();

        var ids = stored.Concat(Enumerable.Range(0, 1500).Select(i => $"other-{i}")).ToArray();
        var names = stored.Concat(Enumerable.Range(0, 1500).Select(i => $"other-{i}")).ToArray();

        await using var query = theStore.QuerySession();
        var found = await query.Query<Doc>()
            .Where(x => ids.Contains(x.Id) && names.Contains(x.Name))
            .ToListAsync(TestContext.Current.CancellationToken);

        found.Select(x => x.Id).OrderBy(x => x).ShouldBe(stored.OrderBy(x => x));
    }
}
