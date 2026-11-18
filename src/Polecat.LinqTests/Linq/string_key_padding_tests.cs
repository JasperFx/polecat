using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: what #877's fix for <c>==</c> left over. <c>=</c> pads, so an id compared through its column
///     matched <c>"abc"</c> for <c>"abc "</c>, and a <c>GROUP BY</c> took <c>"abc"</c> and <c>"abc  "</c>
///     for one key.
/// </summary>
public class string_key_padding_tests: OneOffConfigurationsContext
{
    public class PaddedDoc
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    public class PaddedIndexedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class PaddedOwner
    {
        public string Id { get; set; } = string.Empty;
        public string Label { get; set; } = string.Empty;
    }

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
            opts.Schema.For<PaddedIndexedDoc>().Index(x => x.Name, x => x.SqlType = "nvarchar(250)");
        });

        await using var session = theStore.LightweightSession();
        session.Store(new PaddedDoc { Id = "abc", Name = "abc", Age = 1 },
            new PaddedDoc { Id = "xyz  ", Name = "abc  ", Age = 2 },
            new PaddedDoc { Id = "other", Name = "abc", Age = 3 });
        session.Store(new PaddedIndexedDoc { Id = Guid.NewGuid(), Name = "abc" },
            new PaddedIndexedDoc { Id = Guid.NewGuid(), Name = "abc  " });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<int[]> agesAsync(IQueryable<PaddedDoc> query)
        => (await query.OrderBy(x => x.Age).ToListAsync(Ct)).Select(x => x.Age).ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_id_is_only_equal_to_itself(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var padded = "abc ";

        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id == "abc"))).ShouldBe([1]);
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id == padded))).ShouldBeEmpty();
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id == "xyz"))).ShouldBeEmpty();
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id == "xyz  "))).ShouldBe([2]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_id_is_different_from_anything_else(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id != "abc "))).ShouldBe([1, 2, 3]);
        (await agesAsync(query.Query<PaddedDoc>().Where(x => !(x.Id == "abc ")))).ShouldBe([1, 2, 3]);
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id != "abc"))).ShouldBe([2, 3]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_list_of_ids_does_not_pad_either(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        string[] padded = ["abc ", "nobody"];
        var many = Enumerable.Range(0, 150).Select(x => $"v{x}").Append("abc ").ToArray();

        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id.IsOneOf("abc ", "nobody")))).ShouldBeEmpty();
        (await agesAsync(query.Query<PaddedDoc>().Where(x => padded.Contains(x.Id)))).ShouldBeEmpty();
        (await agesAsync(query.Query<PaddedDoc>().Where(x => many.Contains(x.Id)))).ShouldBeEmpty();
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id.IsOneOf("abc", "xyz  ")))).ShouldBe([1, 2]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_non_ascii_id_is_refused_by_the_column_it_could_not_be_stored_in(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        // Bound as nvarchar, which the length has to allow for.
        (await agesAsync(query.Query<PaddedDoc>().Where(x => x.Id == "é"))).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_id_comparison_in_a_join_is_on_the_right_table(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new PaddedOwner { Id = "abc", Label = "owner" });
            await session.SaveChangesAsync(Ct);
        }

        var rows = await query.Query<PaddedDoc>().Where(x => x.Id == "abc")
            .GroupJoin(query.Query<PaddedOwner>(), x => x.Id, x => x.Id, (d, o) => new { d, o })
            .SelectMany(x => x.o.DefaultIfEmpty(), (x, o) => new { x.d.Age, o!.Label })
            .ToListAsync(Ct);

        rows.ShouldHaveSingleItem().Label.ShouldBe("owner");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_group_key_keeps_its_spaces(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var groups = await query.Query<PaddedDoc>().GroupBy(x => x.Name).Select(g => new { g.Key, Count = g.Count() })
            .OrderBy(x => x.Count).ToListAsync(Ct);

        groups.Select(x => (x.Key, x.Count)).ShouldBe([("abc  ", 1), ("abc", 2)]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_composite_key_keeps_its_spaces(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var groups = await query.Query<PaddedDoc>().GroupBy(x => new { x.Name }).Select(g => new { g.Key.Name, Count = g.Count() })
            .ToListAsync(Ct);

        groups.Select(x => (x.Name, x.Count)).ShouldBe([("abc", 2), ("abc  ", 1)], ignoreOrder: true);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_key_over_an_nvarchar_index_keeps_its_spaces(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<PaddedIndexedDoc>().GroupBy(x => x.Name).Select(g => g.Key).ToListAsync(Ct))
            .ShouldBe(["abc", "abc  "], ignoreOrder: true);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_having_means_the_same_as_a_where(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<PaddedDoc>().GroupBy(x => x.Name).Where(g => g.Key == "abc").Select(g => g.Count()).ToListAsync(Ct))
            .ShouldBe([2]);
        (await query.Query<PaddedDoc>().GroupBy(x => x.Name).Where(g => g.Key != "abc").Select(g => g.Count()).ToListAsync(Ct))
            .ShouldBe([1]);
        (await query.Query<PaddedDoc>().GroupBy(x => x.Name).Where(g => g.Key == "abc ").Select(g => g.Count()).ToListAsync(Ct))
            .ShouldBeEmpty();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task pages_of_groups_do_not_lose_one(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<PaddedDoc>().GroupBy(x => x.Name).Select(g => g.Key).Skip(1).Take(5).ToListAsync(Ct))
            .ShouldHaveSingleItem();
        (await query.Query<PaddedDoc>().GroupBy(x => x.Name).Select(g => g.Key).CountAsync(Ct)).ShouldBe(2);
    }

    [Fact]
    public async Task a_group_of_ids_needs_no_length()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        (await query.Query<PaddedDoc>().GroupBy(x => x.Id).Select(g => g.Key).ToListAsync(Ct))
            .ShouldBe(["abc", "xyz  ", "other"], ignoreOrder: true);
        (await query.Query<PaddedDoc>().GroupBy(x => x.Id).Where(g => g.Key == "abc").Select(g => g.Count()).ToListAsync(Ct))
            .ShouldBe([1]);
    }

    [Fact]
    public async Task an_id_still_seeks()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(Enumerable.Range(0, 1500).Select(i => new PaddedDoc { Id = $"id{i}", Name = "x" }).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        var queryable = query.Query<PaddedDoc>().Where(x => x.Id == "id42");
        (await queryable.ToListAsync(Ct)).ShouldHaveSingleItem();

        var accesses = await QueryPlanProbe.AccessesAsync(query, queryable, Ct);

        accesses.ShouldContain(x => x.Operator.EndsWith("Seek") && x.Table == "pc_doc_paddeddoc");
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan"));
    }
}
