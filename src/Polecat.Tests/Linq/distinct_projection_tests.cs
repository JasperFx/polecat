using Polecat.Linq;
using Polecat.Linq.CursorPaging;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: a <c>Select</c> that is not one scalar reads the document, so a <c>Distinct</c> after it
///     compared whole documents. That is an error on a <c>json</c> column and, on <c>nvarchar(max)</c>, no
///     distinct at all, because each row has its own id. Ordering it was Msg 145, and counting it counted
///     the rows of the table.
/// </summary>
public class distinct_projection_tests: OneOffConfigurationsContext
{
    public class DistinctAddress
    {
        public string City { get; set; } = string.Empty;
    }

    public enum DistinctColor
    {
        Red,
        Blue
    }

    public class DistinctDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public int? Rank { get; set; }
        public bool Active { get; set; }
        public DistinctColor Color { get; set; }
        public Guid Owner { get; set; }
        public DateTimeOffset When { get; set; }
        public DistinctAddress? Address { get; set; }
    }

    public class DistinctDto
    {
        public string? Label { get; set; }
        public int Years { get; set; }
    }

    public record DistinctRecord(string Label);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly Guid SharedOwner = Guid.NewGuid();
    private static readonly DateTimeOffset SharedWhen = new(2026, 11, 2, 8, 30, 0, TimeSpan.Zero);

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(
            doc("a", 1), doc("a", 1), doc("a", 2),
            doc("b", 2), doc("b", 2),
            doc("c", 3));
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static DistinctDoc doc(string name, int age) => new()
    {
        Id = Guid.NewGuid(), Name = name, Age = age, Active = age % 2 == 0,
        Rank = age == 3 ? null : age, Color = age == 1 ? DistinctColor.Blue : DistinctColor.Red,
        Owner = SharedOwner, When = SharedWhen, Address = new DistinctAddress { City = $"city {name}" }
    };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_anonymous_projection_is_distinct_across_all_of_its_members(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct().ToListAsync(Ct);

        rows.OrderBy(x => x.Name).ThenBy(x => x.Age).ShouldBe(
        [
            new { Name = "a", Age = 1 }, new { Name = "a", Age = 2 }, new { Name = "b", Age = 2 },
            new { Name = "c", Age = 3 }
        ]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task it_filters_first(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<DistinctDoc>().Where(x => x.Name == "a").Select(x => new { x.Age }).Distinct()
            .OrderBy(y => y.Age).ToListAsync(Ct);

        rows.Select(x => x.Age).ShouldBe([1, 2]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task every_kind_of_member_survives_the_trip(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<DistinctDoc>()
            .Select(x => new { x.Age, x.Rank, x.Active, x.Color, x.Owner, x.When, x.Address!.City })
            .Distinct().OrderBy(y => y.Age).ThenBy(y => y.City).ToListAsync(Ct);

        rows.Count.ShouldBe(4);

        rows[0].ShouldBe(new
        {
            Age = 1, Rank = (int?)1, Active = false, Color = DistinctColor.Blue, Owner = SharedOwner,
            When = SharedWhen, City = "city a"
        });

        rows[3].Rank.ShouldBeNull();
        rows[3].City.ShouldBe("city c");
        rows[1].Active.ShouldBeTrue();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_object_initializer_works_like_an_anonymous_type(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<DistinctDoc>().Select(x => new DistinctDto { Label = x.Name, Years = x.Age })
            .Distinct().OrderBy(y => y.Label).ThenByDescending(y => y.Years).ToListAsync(Ct);

        rows.Select(x => (x.Label, x.Years)).ShouldBe([("a", 2), ("a", 1), ("b", 2), ("c", 3)]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task it_can_be_ordered_either_side_of_the_distinct(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var after = await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct()
            .OrderByDescending(y => y.Name).ThenBy(y => y.Age).ToListAsync(Ct);
        after.Select(x => x.Name + x.Age).ShouldBe(["c3", "b2", "a1", "a2"]);

        var before = await query.Query<DistinctDoc>().OrderBy(x => x.Age).ThenBy(x => x.Name)
            .Select(x => new { x.Name, x.Age }).Distinct().ToListAsync(Ct);
        before.Select(x => x.Name + x.Age).ShouldBe(["a1", "a2", "b2", "c3"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task it_pages_the_distinct_rows(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var rows = query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct()
            .OrderBy(y => y.Name).ThenBy(y => y.Age);

        (await rows.Take(2).ToListAsync(Ct)).Select(x => x.Name + x.Age).ShouldBe(["a1", "a2"]);
        (await rows.Skip(1).Take(2).ToListAsync(Ct)).Select(x => x.Name + x.Age).ShouldBe(["a2", "b2"]);
        (await rows.Skip(3).ToListAsync(Ct)).Select(x => x.Name + x.Age).ShouldBe(["c3"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task one_row_comes_back_as_itself(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var rows = query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct()
            .OrderBy(y => y.Name).ThenBy(y => y.Age);

        (await rows.FirstAsync(Ct)).ShouldBe(new { Name = "a", Age = 1 });
        (await rows.LastAsync(Ct)).ShouldBe(new { Name = "c", Age = 3 });

        var single = query.Query<DistinctDoc>().Where(x => x.Name == "b").Select(x => new { x.Name, x.Age }).Distinct();
        (await single.SingleAsync(Ct)).ShouldBe(new { Name = "b", Age = 2 });
        (await single.SingleOrDefaultAsync(Ct)).ShouldBe(new { Name = "b", Age = 2 });

        var none = query.Query<DistinctDoc>().Where(x => x.Name == "z").Select(x => new { x.Name }).Distinct();
        (await none.FirstOrDefaultAsync(Ct)).ShouldBeNull();

        await Should.ThrowAsync<InvalidOperationException>(() =>
            query.Query<DistinctDoc>().Select(x => new { x.Age }).Distinct().SingleAsync(Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task counting_counts_the_distinct_rows(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        // Both of these answered 6: DISTINCT applied to the one row COUNT(*) returned.
        (await query.Query<DistinctDoc>().Select(x => x.Name).Distinct().CountAsync(Ct)).ShouldBe(3);
        (await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct().CountAsync(Ct)).ShouldBe(4);

        (await query.Query<DistinctDoc>().Select(x => x.Name).Distinct().LongCountAsync(Ct)).ShouldBe(3L);
        (await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct().LongCountAsync(Ct)).ShouldBe(4L);

        (await query.Query<DistinctDoc>().OrderBy(x => x.Name).Select(x => x.Name).Distinct().CountAsync(Ct)).ShouldBe(3);
        (await query.Query<DistinctDoc>().OrderBy(x => x.Age).Select(x => new { x.Age }).Distinct().CountAsync(Ct)).ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task counting_a_page_counts_the_page(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct().OrderBy(y => y.Age)
            .Take(2).CountAsync(Ct)).ShouldBe(2);
        (await query.Query<DistinctDoc>().Select(x => x.Name).Distinct().OrderBy(x => x).Take(2).CountAsync(Ct))
            .ShouldBe(2);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task any_is_about_whether_there_is_a_row(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<DistinctDoc>().Select(x => new { x.Name }).Distinct().AnyAsync(Ct)).ShouldBeTrue();
        (await query.Query<DistinctDoc>().Where(x => x.Age == 99).Select(x => x.Name).Distinct().AnyAsync(Ct)).ShouldBeFalse();
        (await query.Query<DistinctDoc>().Where(x => x.Age == 99).Select(x => x.Address).Distinct().AnyAsync(Ct)).ShouldBeFalse();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_scalar_is_still_distinct_and_ordered(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<DistinctDoc>().Select(x => x.Name).Distinct().OrderByDescending(x => x).ToListAsync(Ct))
            .ShouldBe(["c", "b", "a"]);
        (await query.Query<DistinctDoc>().OrderBy(x => x.Name).Select(x => x.Name).Distinct().ToListAsync(Ct))
            .ShouldBe(["a", "b", "c"]);
        (await query.Query<DistinctDoc>().Select(x => x.Age).Distinct().OrderBy(x => x).FirstAsync(Ct)).ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task distinct_documents_are_the_documents(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<DistinctDoc>().Distinct().ToListAsync(Ct)).Count.ShouldBe(6);
        (await query.Query<DistinctDoc>().Distinct().CountAsync(Ct)).ShouldBe(6);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_scalar_cannot_be_ordered_by_what_it_did_not_select(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().OrderBy(x => x.Age).Select(x => x.Name).Distinct().ToListAsync(Ct));

        ex.Message.ShouldContain("Distinct");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_projection_cannot_be_ordered_by_what_it_did_not_project(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().OrderBy(x => x.Age).Select(x => new { x.Name }).Distinct().ToListAsync(Ct));

        ex.Message.ShouldContain("Distinct");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_projection_sql_cannot_compare_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => new { x.Name, x.Address }).Distinct().ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => x.Address).Distinct().ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => x.Age + 1).Distinct().ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => new { Upper = x.Name.ToUpper() }).Distinct().ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => new DistinctRecord(x.Name)).Distinct().ToListAsync(Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => x.Address).Distinct().CountAsync(Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_aggregate_over_the_distinct_rows_is_refused(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        // Each of these would have aggregated the rows of the table and made the answer distinct.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => new { x.Age }).Distinct().SumAsync(y => y.Age, Ct));
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().Select(x => new { x.Age }).Distinct().AverageAsync(y => y.Age, Ct));
    }

    [Fact]
    public async Task a_distinct_projection_streams_as_json()
    {
        await using var query = await seededAsync(true);

        var json = await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct()
            .OrderBy(y => y.Name).ThenBy(y => y.Age).ToJsonArrayAsync(Ct);

        var rows = System.Text.Json.JsonDocument.Parse(json).RootElement;
        rows.GetArrayLength().ShouldBe(4);
        rows[1].GetProperty("name").GetString().ShouldBe("a");
        rows[1].GetProperty("age").GetInt32().ShouldBe(2);
    }

    [Fact]
    public async Task distinct_documents_stream_as_the_documents()
    {
        await using var query = await seededAsync(true);

        var json = await query.Query<DistinctDoc>().Distinct().ToJsonArrayAsync(Ct);

        System.Text.Json.JsonDocument.Parse(json).RootElement.GetArrayLength().ShouldBe(6);
    }

    [Fact]
    public async Task a_page_of_a_distinct_projection_counts_the_distinct_rows()
    {
        await using var query = await seededAsync(true);

        using var stream = new MemoryStream();
        await query.Query<DistinctDoc>().Select(x => new { x.Name, x.Age }).Distinct().OrderBy(y => y.Name)
            .ThenBy(y => y.Age).StreamPagedJsonArray(1, 3, stream, Ct);

        var page = System.Text.Json.JsonDocument.Parse(stream.ToArray()).RootElement;
        page.GetProperty("totalItemCount").GetInt32().ShouldBe(4);
        page.GetProperty("items").GetArrayLength().ShouldBe(3);
    }

    [Fact]
    public async Task a_cursor_page_refuses_it_rather_than_drop_it()
    {
        await using var query = await seededAsync(true);

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<DistinctDoc>().OrderBy(x => x.Name).Select(x => new { x.Name }).Distinct()
                .ToJsonPageByCursorAsync(null, 2, Ct));
    }
}
