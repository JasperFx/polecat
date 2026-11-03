using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: a default <c>Index(x =&gt; x.Name)</c> is a <c>varchar(250)</c> computed column, which holds
///     what the database's code page holds. Under the default collation <c>東京</c> and <c>大阪</c> are both
///     <c>??</c> in it, and <c>ā</c> is <c>a</c>.
/// </summary>
/// <remarks>
///     <para>
///         #877 kept a value like that out of the column going in, so looking for <c>東京</c> reads the
///         document. It didn't keep one out coming back. A value that <i>is</i> plain ASCII seeks the column
///         and can land on one of these, and <c>DATALENGTH</c> can't tell them apart, because <c>??</c> and
///         <c>東京</c> are the same length. Everything that reads the value back through the column reads
///         the <c>??</c>.
///     </para>
///     <para>
///         An <c>IN</c> needed nothing: the sentinel that keeps it from padding compares the document
///         already, and <c>a_list_was_never_fooled</c> holds that.
///     </para>
/// </remarks>
public class varchar_index_lossy_value_tests: OneOffConfigurationsContext
{
    public class LossyDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    public class WideDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const string Tokyo = "東京";
    private const string Osaka = "大阪";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<LossyDoc>().Index(x => x.Name);
            opts.Schema.For<WideDoc>().Index(x => x.Name, x => x.SqlType = "nvarchar(250)");
        });

        await using var session = theStore.LightweightSession();
        session.Store(new LossyDoc { Id = Guid.NewGuid(), Name = Tokyo, Age = 1 },
            new LossyDoc { Id = Guid.NewGuid(), Name = Osaka, Age = 2 },
            new LossyDoc { Id = Guid.NewGuid(), Name = "ā", Age = 3 },
            new LossyDoc { Id = Guid.NewGuid(), Name = "plain", Age = 4 },
            new LossyDoc { Id = Guid.NewGuid(), Name = "??", Age = 5 });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<int[]> agesAsync(IQueryable<LossyDoc> query)
        => (await query.OrderBy(x => x.Age).ToListAsync(Ct)).Select(x => x.Age).ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_ascii_value_finds_what_is_stored_and_not_what_the_column_made_of_it(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name == "??"))).ShouldBe([5]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name == "a"))).ShouldBeEmpty();
        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name == "plain"))).ShouldBe([4]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_inequality_keeps_what_the_column_only_looks_like(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name != "??"))).ShouldBe([1, 2, 3, 4]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name != "a"))).ShouldBe([1, 2, 3, 4, 5]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => !(x.Name == "??")))).ShouldBe([1, 2, 3, 4]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_value_outside_the_code_page_is_found(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name == Tokyo))).ShouldBe([1]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name == "ā"))).ShouldBe([3]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name != Tokyo))).ShouldBe([2, 3, 4, 5]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_list_was_never_fooled(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var names = new[] { "??", "nobody" };
        var many = Enumerable.Range(0, 150).Select(x => $"v{x}").Append("??").ToArray();

        (await agesAsync(query.Query<LossyDoc>().Where(x => x.Name.IsOneOf("??", "nobody")))).ShouldBe([5]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => names.Contains(x.Name)))).ShouldBe([5]);
        (await agesAsync(query.Query<LossyDoc>().Where(x => many.Contains(x.Name)))).ShouldBe([5]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_scalar_reads_back_what_is_stored(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<LossyDoc>().OrderBy(x => x.Age).Select(x => x.Name).ToListAsync(Ct))
            .ShouldBe([Tokyo, Osaka, "ā", "plain", "??"]);

        (await query.Query<LossyDoc>().Where(x => x.Age == 1).Select(x => x.Name).FirstAsync(Ct)).ShouldBe(Tokyo);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_aggregate_reads_what_is_stored(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var tokyo = query.Query<LossyDoc>().Where(x => x.Age == 1);

        (await tokyo.MaxAsync(x => x.Name, Ct)).ShouldBe(Tokyo);
        (await tokyo.MinAsync(x => x.Name, Ct)).ShouldBe(Tokyo);
        (await tokyo.GroupBy(x => x.Age).Select(g => g.Max(x => x.Name)).ToListAsync(Ct)).ShouldBe([Tokyo]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task distinct_values_stay_distinct(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        string[] all = [Tokyo, Osaka, "ā", "plain", "??"];

        (await query.Query<LossyDoc>().Select(x => x.Name).Distinct().ToListAsync(Ct)).ShouldBe(all, ignoreOrder: true);

        (await query.Query<LossyDoc>().Select(x => new { x.Name }).Distinct().ToListAsync(Ct))
            .Select(x => x.Name).ShouldBe(all, ignoreOrder: true);

        (await query.Query<LossyDoc>().Select(x => x.Name).Distinct().CountAsync(Ct)).ShouldBe(5);
        (await query.Query<LossyDoc>().DistinctBy(x => x.Name).ToListAsync(Ct)).Count.ShouldBe(5);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_distinct_scalar_can_still_be_ordered_either_side(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var before = await query.Query<LossyDoc>().Where(x => x.Age == 4).OrderBy(x => x.Name)
            .Select(x => x.Name).Distinct().ToListAsync(Ct);
        before.ShouldBe(["plain"]);

        var after = await query.Query<LossyDoc>().Where(x => x.Age == 4).Select(x => x.Name).Distinct()
            .OrderBy(x => x).ToListAsync(Ct);
        after.ShouldBe(["plain"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_group_is_what_is_stored(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var groups = await query.Query<LossyDoc>().GroupBy(x => x.Name).Select(g => new { g.Key, Count = g.Count() })
            .ToListAsync(Ct);

        groups.Select(x => x.Key).ShouldBe([Tokyo, Osaka, "ā", "plain", "??"], ignoreOrder: true);
        groups.ShouldAllBe(x => x.Count == 1);

        (await query.Query<LossyDoc>().GroupBy(x => x.Name).Where(g => g.Key == "??").Select(g => g.Key).ToListAsync(Ct))
            .ShouldBe(["??"]);
    }

    [Fact]
    public async Task a_streamed_projection_writes_what_is_stored()
    {
        await using var query = await seededAsync(true);

        var json = await query.Query<LossyDoc>().Where(x => x.Age == 1).Select(x => new { x.Name }).ToJsonArrayAsync(Ct);

        System.Text.Json.JsonDocument.Parse(json).RootElement[0].GetProperty("name").GetString().ShouldBe(Tokyo);
    }

    [Fact]
    public async Task the_seek_survives_it()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Enumerable.Range(0, 1500).Select(i => new LossyDoc { Id = Guid.NewGuid(), Name = $"name{i}" })
                .ToArray());
            await session.SaveChangesAsync(Ct);
        }

        var queryable = query.Query<LossyDoc>().Where(x => x.Name == "name42");
        (await queryable.ToListAsync(Ct)).ShouldHaveSingleItem();

        var accesses = await QueryPlanProbe.AccessesAsync(query, queryable, Ct);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Table == "pc_doc_lossydoc");
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_nvarchar_index_had_none_of_this_to_lose(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new WideDoc { Id = Guid.NewGuid(), Name = Tokyo }, new WideDoc { Id = Guid.NewGuid(), Name = Osaka });
            await session.SaveChangesAsync(Ct);
        }

        (await query.Query<WideDoc>().CountAsync(x => x.Name == "??", Ct)).ShouldBe(0);
        (await query.Query<WideDoc>().Select(x => x.Name).ToListAsync(Ct)).ShouldBe([Tokyo, Osaka], ignoreOrder: true);
        (await query.Query<WideDoc>().GroupBy(x => x.Name).Select(g => g.Key).ToListAsync(Ct))
            .ShouldBe([Tokyo, Osaka], ignoreOrder: true);
    }
}
