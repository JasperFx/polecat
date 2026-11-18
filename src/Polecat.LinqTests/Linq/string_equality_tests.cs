using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #877: T-SQL's <c>=</c> pads the shorter side with spaces, so <c>x.Text == ""</c> matched a value of
///     nothing but spaces and <c>x.Text == "abc"</c> matched <c>"abc  "</c>. The same class as the
///     <c>IsNullOrEmpty</c> half of #839.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>The seekable <c>=</c> stays, and a residual on the byte count does the rest.</b> Reading
///         the value through a padding-free expression alone would be right and would cost every index.
///         The plan facts at the bottom hold that to what SQL Server really does.
///     </para>
///     <para>
///         <b>The same residual closes the truncation half of #877.</b> On <c>nvarchar(max)</c> storage a
///         computed column truncates a value wider than itself, so a query value equal to the kept prefix
///         matched a row that does not hold it. Over an index the residual reads the document, not the
///         column.
///     </para>
/// </remarks>
public class string_equality_tests: OneOffConfigurationsContext
{
    public class SpacedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Text { get; set; }
    }

    public class TaggedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<string> Tags { get; set; } = [];
        public int[] Numbers { get; set; } = [];
    }

    public class IndexedSpacedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Code { get; set; }
    }

    public class WideIndexedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Code { get; set; }
    }

    public class SpacedEvent
    {
        public string Text { get; set; } = string.Empty;
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void configure(bool nativeJson, Action<StoreOptions>? also = null)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            also?.Invoke(opts);
        });
    }

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        configure(nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(new SpacedDoc { Id = Guid.NewGuid(), Name = "exact", Text = "abc" });
        session.Store(new SpacedDoc { Id = Guid.NewGuid(), Name = "trailing", Text = "abc  " });
        session.Store(new SpacedDoc { Id = Guid.NewGuid(), Name = "spaces", Text = "   " });
        session.Store(new SpacedDoc { Id = Guid.NewGuid(), Name = "empty", Text = "" });
        session.Store(new SpacedDoc { Id = Guid.NewGuid(), Name = "null", Text = null });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<string[]> namesAsync(IQueryable<SpacedDoc> query)
        => (await query.OrderBy(x => x.Name).ToListAsync(Ct)).Select(x => x.Name).ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task equality_does_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text == "abc"))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text == ""))).ShouldBe(["empty"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text == "   "))).ShouldBe(["spaces"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text == "abc  "))).ShouldBe(["trailing"]);

        // Reversed operands, and the query's own value carrying the extra spaces.
        (await namesAsync(query.Query<SpacedDoc>().Where(x => "abc" == x.Text))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text == "abc "))).ShouldBeEmpty();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task inequality_does_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text != "abc")))
            .ShouldBe(["empty", "spaces", "trailing"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => !(x.Text == "abc"))))
            .ShouldBe(["empty", "spaces", "trailing"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => !(x.Text != "abc")))).ShouldBe(["exact"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_equals_methods_do_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.Equals("abc")))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.Equals("abc", StringComparison.Ordinal))))
            .ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>()
                .Where(x => x.Text!.Equals("ABC", StringComparison.OrdinalIgnoreCase))))
            .ShouldBe(["exact"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_transformed_value_does_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.ToLower() == "abc"))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.ToUpper() == "ABC"))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.ToLower() != "abc")))
            .ShouldBe(["empty", "spaces", "trailing"]);

        // Trimming is what makes these two equal, so here the padded value belongs.
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.Trim() == "abc")))
            .ShouldBe(["exact", "trailing"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text!.Trim() == "")))
            .ShouldBe(["empty", "spaces"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_list_does_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text.IsOneOf("abc", "nothing")))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text.IsOneOf("")))).ShouldBe(["empty"]);

        var values = new[] { "abc", "nothing" };
        (await namesAsync(query.Query<SpacedDoc>().Where(x => values.Contains(x.Text)))).ShouldBe(["exact"]);
        (await namesAsync(query.Query<SpacedDoc>().Where(x => !values.Contains(x.Text))))
            .ShouldBe(["empty", "spaces", "trailing"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_list_long_enough_to_travel_as_one_parameter_does_not_pad(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var values = Enumerable.Range(0, 150).Select(i => $"value{i}").Append("abc").ToArray();

        (await namesAsync(query.Query<SpacedDoc>().Where(x => x.Text.IsOneOf(values)))).ShouldBe(["exact"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_collection_element_does_not_pad(bool nativeJson)
    {
        configure(nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new TaggedDoc { Id = Guid.NewGuid(), Name = "exact", Tags = ["abc"], Numbers = [1] });
            session.Store(new TaggedDoc { Id = Guid.NewGuid(), Name = "trailing", Tags = ["abc  "], Numbers = [2] });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Query<TaggedDoc>().Where(x => x.Tags.Contains("abc")).ToListAsync(Ct);
        rows.Single().Name.ShouldBe("exact");

        var numbers = await query.Query<TaggedDoc>().Where(x => x.Numbers.Contains(2)).ToListAsync(Ct);
        numbers.Single().Name.ShouldBe("trailing");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_event_body_does_not_pad(bool nativeJson)
    {
        configure(nativeJson);
        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new SpacedEvent { Text = "abc" },
                new SpacedEvent { Text = "abc  " });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Events.QueryRawEventDataOnly<SpacedEvent>().Where(x => x.Text == "abc").ToListAsync(Ct);

        rows.Single().Text.ShouldBe("abc");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_indexed_member_does_not_pad(bool nativeJson)
    {
        configure(nativeJson, opts => opts.Schema.For<IndexedSpacedDoc>().Index(x => x.Code));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedSpacedDoc { Id = Guid.NewGuid(), Name = "exact", Code = "abc" });
            session.Store(new IndexedSpacedDoc { Id = Guid.NewGuid(), Name = "trailing", Code = "abc  " });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var equal = await query.Query<IndexedSpacedDoc>().Where(x => x.Code == "abc").ToListAsync(Ct);
        equal.Single().Name.ShouldBe("exact");

        var unequal = await query.Query<IndexedSpacedDoc>().Where(x => x.Code != "abc").ToListAsync(Ct);
        unequal.Single().Name.ShouldBe("trailing");

        var list = await query.Query<IndexedSpacedDoc>().Where(x => x.Code.IsOneOf("abc", "other")).ToListAsync(Ct);
        list.Single().Name.ShouldBe("exact");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_value_past_the_width_of_its_index_does_not_match_the_part_that_fits(bool nativeJson)
    {
        configure(nativeJson, opts => opts.Schema.For<IndexedSpacedDoc>().Index(x => x.Code));

        var prefix = new string('a', 250);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedSpacedDoc { Id = Guid.NewGuid(), Name = "long", Code = prefix + "tail" });
            session.Store(new IndexedSpacedDoc { Id = Guid.NewGuid(), Name = "prefix", Code = prefix });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var rows = await query.Query<IndexedSpacedDoc>().Where(x => x.Code == prefix).ToListAsync(Ct);

        rows.Single().Name.ShouldBe("prefix");

        var list = await query.Query<IndexedSpacedDoc>().Where(x => x.Code.IsOneOf(prefix, "other")).ToListAsync(Ct);

        list.Single().Name.ShouldBe("prefix");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task the_residual_keeps_the_seek(bool nativeJson)
    {
        configure(nativeJson, opts =>
            opts.Schema.For<WideIndexedDoc>().Index(x => x.Code, x => x.SqlType = "nvarchar(250)"));

        await using (var session = theStore.LightweightSession())
        {
            // Enough rows that a scan is not the cheap answer by itself.
            for (var i = 0; i < 2000; i++)
            {
                session.Store(new WideIndexedDoc { Id = Guid.NewGuid(), Name = $"n{i}", Code = $"code-{i}" });
            }

            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var queryable = query.Query<WideIndexedDoc>().Where(x => x.Code == "code-42");
        (await queryable.ToListAsync(Ct)).Single().Name.ShouldBe("n42");

        var accesses = await QueryPlanProbe.AccessesAsync(query, queryable, Ct);

        accesses.ShouldContain(x => x.Operator == "Index Seek" && x.Index != null && x.Index.Contains("code"));
        accesses.ShouldNotContain(x => x.Operator.EndsWith("Scan") && x.Table == "pc_doc_wideindexeddoc");
    }
}
