using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #875: an operator written after a <c>Select</c> reads the projection, not the document. Its lambda
///     names a member of the projected shape, and that member used to become a JSON path on the document:
///     <c>Select(x =&gt; x.Address).Where(a =&gt; a.City == "x")</c> read <c>$.city</c> and
///     <c>Select(x =&gt; new { N = x.Name }).OrderBy(y =&gt; y.N)</c> read <c>$.n</c>. Neither key exists,
///     so the filter matched nothing and the ordering did nothing, with no error.
/// </summary>
public class projected_member_tests: OneOffConfigurationsContext
{
    public class ProjectedAddress
    {
        public string City { get; set; } = string.Empty;
    }

    public class ProjectedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public ProjectedAddress? Address { get; set; }
        public List<string> Tags { get; set; } = [];
    }

    public class NameDto
    {
        public string Label { get; set; } = string.Empty;
        public int NotProjected { get; set; }
    }

    public record PositionalDto(string Label);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<IQuerySession> seededAsync(bool nativeJson = true)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(new ProjectedDoc
        {
            Id = Guid.NewGuid(), Name = "cal", Age = 30, Address = new ProjectedAddress { City = "Oslo" }, Tags = ["a"]
        });
        session.Store(new ProjectedDoc
        {
            Id = Guid.NewGuid(), Name = "ann", Age = 40, Address = new ProjectedAddress { City = "Rome" }
        });
        session.Store(new ProjectedDoc { Id = Guid.NewGuid(), Name = "bob", Age = 50 });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_whole_object_member_can_be_projected(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        // JSON_VALUE is NULL for an object, so this read a NULL for every row.
        var addresses = await query.Query<ProjectedDoc>().OrderBy(x => x.Age).Select(x => x.Address).ToListAsync(Ct);

        addresses.Select(x => x?.City).ShouldBe(["Oslo", "Rome", null]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_where_reads_a_member_of_a_projected_object(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var addresses = await query.Query<ProjectedDoc>().Select(x => x.Address).Where(a => a!.City == "Oslo")
            .ToListAsync(Ct);

        addresses.Single()!.City.ShouldBe("Oslo");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_ordering_reads_a_member_of_a_projected_object(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var addresses = await query.Query<ProjectedDoc>().Select(x => x.Address).Where(a => a != null)
            .OrderByDescending(a => a!.City).ToListAsync(Ct);

        addresses.Select(x => x!.City).ShouldBe(["Rome", "Oslo"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_ordering_reads_a_member_of_an_anonymous_projection(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ascending = await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).OrderBy(y => y.N)
            .ToListAsync(Ct);
        ascending.Select(x => x.N).ShouldBe(["ann", "bob", "cal"]);

        var descending = await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).OrderByDescending(y => y.N)
            .ToListAsync(Ct);
        descending.Select(x => x.N).ShouldBe(["cal", "bob", "ann"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_second_ordering_key_reads_the_projection_too(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<ProjectedDoc>().Select(x => new { N = x.Name, A = x.Age })
            .OrderByDescending(y => y.A).ThenBy(y => y.N).ToListAsync(Ct);

        rows.Select(x => x.N).ShouldBe(["bob", "ann", "cal"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_where_reads_an_anonymous_projection_and_a_dto(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var anonymous = await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).Where(y => y.N == "ann")
            .ToListAsync(Ct);
        anonymous.Single().N.ShouldBe("ann");

        var dto = await query.Query<ProjectedDoc>().Select(x => new NameDto { Label = x.Name })
            .Where(y => y.Label == "ann").ToListAsync(Ct);
        dto.Single().Label.ShouldBe("ann");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_nested_projection_is_followed_all_the_way_down(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var rows = await query.Query<ProjectedDoc>()
            .Select(x => new { Outer = new { x.Address, Label = x.Name } })
            .Where(y => y.Outer.Address!.City == "Rome" && y.Outer.Label == "ann")
            .ToListAsync(Ct);

        rows.Single().Outer.Label.ShouldBe("ann");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_terminal_operator_reads_the_projection(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).CountAsync(y => y.N == "ann", Ct))
            .ShouldBe(1);
        (await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).AnyAsync(y => y.N == "nobody", Ct))
            .ShouldBeFalse();
        (await query.Query<ProjectedDoc>().Select(x => new { A = x.Age }).SumAsync(y => y.A, Ct)).ShouldBe(120);
        (await query.Query<ProjectedDoc>().Select(x => new { A = x.Age }).MaxAsync(y => y.A, Ct)).ShouldBe(50);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_second_select_reads_the_first(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var ages = await query.Query<ProjectedDoc>().OrderBy(x => x.Age).Select(x => new { A = x.Age }).Select(y => y.A)
            .ToListAsync(Ct);
        ages.ShouldBe([30, 40, 50]);

        var cities = await query.Query<ProjectedDoc>().OrderBy(x => x.Age).Select(x => x.Address).Select(a => a!.City)
            .ToListAsync(Ct);
        cities.ShouldBe(["Oslo", "Rome", null]);

        var shaped = await query.Query<ProjectedDoc>().OrderBy(x => x.Age).Select(x => new { x.Name, x.Age })
            .Select(y => new { Label = y.Name + "!", Next = y.Age + 1 }).ToListAsync(Ct);
        shaped.Select(x => x.Label).ShouldBe(["cal!", "ann!", "bob!"]);
        shaped.Select(x => x.Next).ShouldBe([31, 41, 51]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_clr_only_member_of_a_projected_collection_still_translates(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var tags = await query.Query<ProjectedDoc>().Select(x => x.Tags).Where(t => t.Count == 1).ToListAsync(Ct);

        tags.Single().ShouldBe(["a"]);
    }

    [Fact]
    public async Task a_projection_streamed_as_json_is_ordered_by_what_it_projected()
    {
        await using var query = await seededAsync();

        var json = await query.Query<ProjectedDoc>().Select(x => new { N = x.Name }).OrderByDescending(y => y.N)
            .ToJsonArrayAsync(Ct);

        json.ShouldBe("""[{"n":"cal"},{"n":"bob"},{"n":"ann"}]""");
    }

    [Fact]
    public async Task a_member_the_projection_did_not_set_is_refused()
    {
        await using var query = await seededAsync();

        // Its value on the projected object is the default, which no row in the table holds.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ProjectedDoc>().Select(x => new NameDto { Label = x.Name }).Where(y => y.NotProjected == 0)
                .ToListAsync(Ct));

        ex.Message.ShouldContain("NotProjected");
    }

    [Fact]
    public async Task a_member_of_a_constructor_projection_is_refused()
    {
        await using var query = await seededAsync();

        // Nothing in the tree says which argument the property was assigned from.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ProjectedDoc>().Select(x => new PositionalDto(x.Name)).Where(y => y.Label == "ann")
                .ToListAsync(Ct));

        ex.Message.ShouldContain("Label");
    }

    [Fact]
    public async Task a_computed_projection_member_is_translated_when_its_expression_is()
    {
        await using var query = await seededAsync();

        var rows = await query.Query<ProjectedDoc>().Select(x => new { Lower = x.Name.ToLower() })
            .Where(y => y.Lower == "ann").ToListAsync(Ct);

        rows.Single().Lower.ShouldBe("ann");
    }

    [Fact]
    public async Task a_computed_projection_member_with_no_translation_is_refused()
    {
        await using var query = await seededAsync();

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ProjectedDoc>().Select(x => new { Sub = x.Name.Substring(1) }).OrderBy(y => y.Sub)
                .ToListAsync(Ct));
    }
}
