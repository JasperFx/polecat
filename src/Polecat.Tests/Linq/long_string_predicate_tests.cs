using Polecat.Linq;
using Polecat.Linq.SqlGeneration;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #835: a bare <c>JSON_VALUE</c> is typed <c>nvarchar(4000)</c> and, in lax mode, answers NULL for
///     anything longer. So a string member over 4000 characters looked NULL to every predicate.
/// </summary>
/// <remarks>
///     <para>
///         Every fact runs on both storage shapes because they need different SQL and fail differently:
///         <c>RETURNING nvarchar(max)</c> is a syntax error on <c>nvarchar(max)</c> storage, and
///         <c>OPENJSON</c> is 25x the CPU on a native <c>json</c> column.
///     </para>
///     <para>
///         The negated facts are the ones the fast path has to survive. A bare locator is correct for a
///         positive equality because UNKNOWN drops the row, which is the right answer when the stored
///         value is longer than the sought one. Put a <c>NOT</c> around it and UNKNOWN still drops the
///         row, where the right answer is now to keep it.
///     </para>
/// </remarks>
public class long_string_predicate_tests: OneOffConfigurationsContext
{
    public class LongDoc
    {
        public Guid Id { get; set; }
        public string? Text { get; set; }
        public LongDocAddress? Address { get; set; }
    }

    public class LongDocAddress
    {
        public string? City { get; set; }
    }

    public class IndexedLongDoc
    {
        public Guid Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }

    public class OwnerDoc
    {
        public Guid Id { get; set; }
        public string? Text { get; set; }
    }

    public class PetDoc
    {
        public Guid Id { get; set; }
        public Guid OwnerId { get; set; }
        public string? Text { get; set; }
    }

    private const int JsonValueLimit = 4000;

    private static readonly string Long = new string('x', 2500) + " needle " + new string('y', 2500);
    private static readonly Guid LongId = Guid.NewGuid();
    private static readonly Guid ShortId = Guid.NewGuid();
    private static readonly Guid NullId = Guid.NewGuid();
    private static readonly Guid EmptyId = Guid.NewGuid();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson, Action<StoreOptions>? configure = null)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            configure?.Invoke(opts);
        });
    }

    private async Task aStoreWithDocumentsAsync(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(new LongDoc { Id = LongId, Text = Long });
        session.Store(new LongDoc { Id = ShortId, Text = "short" });
        session.Store(new LongDoc { Id = NullId, Text = null });
        session.Store(new LongDoc { Id = EmptyId, Text = "" });
        await session.SaveChangesAsync(Ct);
    }

    private async Task<Guid[]> idsAsync(Func<IQueryable<LongDoc>, IQueryable<LongDoc>> where)
    {
        await using var session = theStore.QuerySession();
        var docs = await where(session.Query<LongDoc>()).ToListAsync(Ct);
        return docs.Select(x => x.Id).OrderBy(x => x).ToArray();
    }

    private static Guid[] Sorted(params Guid[] ids) => ids.OrderBy(x => x).ToArray();

    private string SqlFor<T>(Func<IQueryable<T>, IQueryable<T>> where)
    {
        // Pure text generation: the session never opens a connection, so there is nothing to release.
        var session = theStore.QuerySession();
        var query = where(session.Query<T>());
        return ((PolecatLinqQueryProvider)query.Provider).BuildSql(query.Expression, session.TenantId);
    }

    // ---- the issue's table ----------------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task is_null_or_empty_does_not_match_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => string.IsNullOrEmpty(x.Text))))
            .ShouldBe(Sorted(NullId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task is_null_or_white_space_does_not_match_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => string.IsNullOrWhiteSpace(x.Text))))
            .ShouldBe(Sorted(NullId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task equality_matches_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        var sought = Long;
        (await idsAsync(q => q.Where(x => x.Text == sought))).ShouldBe([LongId]);
    }

    // ---- the rest of the predicate surface ------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_check_sees_a_long_value_as_present(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => x.Text == null))).ShouldBe([NullId]);
        (await idsAsync(q => q.Where(x => x.Text != null)))
            .ShouldBe(Sorted(LongId, ShortId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task like_predicates_reach_into_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => x.Text!.Contains("needle")))).ShouldBe([LongId]);
        (await idsAsync(q => q.Where(x => x.Text!.StartsWith("xxx")))).ShouldBe([LongId]);
        (await idsAsync(q => q.Where(x => x.Text!.EndsWith("yyy")))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task string_equals_with_a_comparison_matches_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        var sought = Long.ToUpperInvariant();
        (await idsAsync(q => q.Where(x => x.Text!.Equals(sought, StringComparison.OrdinalIgnoreCase))))
            .ShouldBe([LongId]);

        var exact = Long;
        (await idsAsync(q => q.Where(x => x.Text!.Equals(exact)))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_case_transform_sees_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        var lower = Long.ToLowerInvariant();
        var upper = Long.ToUpperInvariant();
        (await idsAsync(q => q.Where(x => x.Text!.ToLower() == lower))).ShouldBe([LongId]);
        (await idsAsync(q => q.Where(x => x.Text!.ToUpper() == upper))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task length_sees_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => x.Text!.Length > 4000))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task is_one_of_and_list_contains_match_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        var values = new[] { Long, "short" };
        (await idsAsync(q => q.Where(x => x.Text!.IsOneOf(values)))).ShouldBe(Sorted(LongId, ShortId));
        (await idsAsync(q => q.Where(x => values.Contains(x.Text)))).ShouldBe(Sorted(LongId, ShortId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_list_too_big_for_parameters_still_carries_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        // Over JsonValueList.Threshold, so this binds one JSON array and unpacks it with OPENJSON, whose
        // default column was nvarchar(4000): a long value came back out of the list NULL.
        var values = Enumerable.Range(0, JsonValueList.Threshold + 5).Select(i => $"v{i}").Append(Long).ToArray();

        (await idsAsync(q => q.Where(x => x.Text!.IsOneOf(values)))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_nested_member_is_read_the_same_way(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LongDoc { Id = LongId, Address = new LongDocAddress { City = Long } });
            await session.SaveChangesAsync(Ct);
        }

        var sought = Long;
        (await idsAsync(q => q.Where(x => x.Address!.City == sought))).ShouldBe([LongId]);
        (await idsAsync(q => q.Where(x => x.Address!.City!.Contains("needle")))).ShouldBe([LongId]);
    }

    [Theory]
    [InlineData(true, JsonValueLimit)]
    [InlineData(true, JsonValueLimit + 1)]
    [InlineData(false, JsonValueLimit)]
    [InlineData(false, JsonValueLimit + 1)]
    public async Task either_side_of_the_json_value_limit(bool nativeJson, int length)
    {
        ConfigureFor(nativeJson);

        var value = new string('z', length);
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new LongDoc { Id = LongId, Text = value });
            await session.SaveChangesAsync(Ct);
        }

        (await idsAsync(q => q.Where(x => x.Text == value))).ShouldBe([LongId]);
        (await idsAsync(q => q.Where(x => string.IsNullOrEmpty(x.Text)))).ShouldBeEmpty();
        (await idsAsync(q => q.Where(x => x.Text == null))).ShouldBeEmpty();
    }

    // ---- a negation has to see a long value, which is what limits the fast path ---------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task not_equal_keeps_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => x.Text != "short"))).ShouldBe(Sorted(LongId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_negated_equality_keeps_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => !(x.Text == "short")))).ShouldBe(Sorted(LongId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_negation_of_a_compound_keeps_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        // The NOT is two levels up from the equality, so looking at the immediate parent is not enough.
        (await idsAsync(q => q.Where(x => !(x.Text == "short" || x.Text == "other"))))
            .ShouldBe(Sorted(LongId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_negated_is_one_of_keeps_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        var values = new[] { "short", "other" };
        (await idsAsync(q => q.Where(x => !x.Text!.IsOneOf(values)))).ShouldBe(Sorted(LongId, EmptyId));
        (await idsAsync(q => q.Where(x => !values.Contains(x.Text)))).ShouldBe(Sorted(LongId, EmptyId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_negated_is_null_or_empty_keeps_a_long_value(bool nativeJson)
    {
        await aStoreWithDocumentsAsync(nativeJson);

        (await idsAsync(q => q.Where(x => !string.IsNullOrEmpty(x.Text)))).ShouldBe(Sorted(LongId, ShortId));
    }

    // ---- the fast paths that must survive -----------------------------------------------------------

    [Fact]
    public void a_positive_equality_keeps_the_bare_locator_on_native_json()
    {
        ConfigureFor(nativeJson: true);

        // 1.7x the CPU to go through nvarchar(max) on a 200k row scan, for a read that cannot change the
        // answer: a stored value over 4000 characters reads NULL, cannot equal a value that fits, and
        // UNKNOWN drops the row.
        var sql = SqlFor<LongDoc>(q => q.Where(x => x.Text == "short"));

        sql.ShouldContain("JSON_VALUE(data, '$.text') = ");
        sql.ShouldNotContain("nvarchar(max)");
    }

    [Fact]
    public void a_value_at_the_limit_keeps_the_bare_locator_and_one_over_it_does_not()
    {
        ConfigureFor(nativeJson: true);

        var fits = new string('z', JsonValueLimit);
        var over = new string('z', JsonValueLimit + 1);

        SqlFor<LongDoc>(q => q.Where(x => x.Text == fits)).ShouldNotContain("RETURNING nvarchar(max)");
        SqlFor<LongDoc>(q => q.Where(x => x.Text == over)).ShouldContain("RETURNING nvarchar(max)");
    }

    [Fact]
    public void a_positive_list_keeps_the_bare_locator()
    {
        ConfigureFor(nativeJson: true);

        var values = new[] { "a", "b" };

        SqlFor<LongDoc>(q => q.Where(x => x.Text!.IsOneOf(values))).ShouldNotContain("nvarchar(max)");
        SqlFor<LongDoc>(q => q.Where(x => values.Contains(x.Text))).ShouldNotContain("nvarchar(max)");
    }

    [Fact]
    public void a_negated_equality_does_not_use_the_bare_locator()
    {
        ConfigureFor(nativeJson: true);

        SqlFor<LongDoc>(q => q.Where(x => !(x.Text == "short"))).ShouldContain("RETURNING nvarchar(max)");
        SqlFor<LongDoc>(q => q.Where(x => x.Text != "short")).ShouldContain("RETURNING nvarchar(max)");
    }

    [Fact]
    public void a_native_read_never_goes_through_openjson()
    {
        ConfigureFor(nativeJson: true);

        // 5417ms against 368ms over 200k rows.
        SqlFor<LongDoc>(q => q.Where(x => x.Text!.Contains("a"))).ShouldNotContain("OPENJSON");
    }

    [Fact]
    public void nvarchar_storage_never_renders_returning()
    {
        ConfigureFor(nativeJson: false);

        // RETURNING on an nvarchar(max) column is Msg 102.
        SqlFor<LongDoc>(q => q.Where(x => x.Text!.Contains("a"))).ShouldNotContain("' RETURNING ");
    }

    // ---- an index keeps its expression, unless it cannot hold the value -----------------------------

    private void ConfigureIndexed(bool nativeJson)
        => ConfigureFor(nativeJson, opts => opts.Schema.For<IndexedLongDoc>().Index(x => x.Code));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void an_indexed_member_keeps_its_computed_column_expression(bool nativeJson)
    {
        ConfigureIndexed(nativeJson);

        var expected = Polecat.Storage.DocumentIndex.ComputedColumnExpression(
            "$.code", "varchar(250)", Polecat.Storage.IndexCasing.Default, nativeJson);

        var values = new[] { "a", "b" };

        // Every one of these stays on the persisted column. It is the whole value of the index, and
        // a range or a negation over it reads the narrow index rather than parsing every document.
        SqlFor<IndexedLongDoc>(q => q.Where(x => x.Code == "abc")).ShouldContain(expected + " = ");
        SqlFor<IndexedLongDoc>(q => q.Where(x => x.Code != "abc")).ShouldContain(expected + " != ");
        SqlFor<IndexedLongDoc>(q => q.Where(x => !(x.Code == "abc"))).ShouldContain(expected + " = ");
        SqlFor<IndexedLongDoc>(q => q.Where(x => x.Code.IsOneOf(values))).ShouldContain(expected + " IN ");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_value_wider_than_the_index_column_is_answered_from_the_document(bool nativeJson)
    {
        ConfigureIndexed(nativeJson);

        // 300 characters in a varchar(250) column. The column cannot hold it: native storage reads NULL
        // and nvarchar storage truncates, so asking the column can never find it.
        var wide = new string('w', 300);
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new IndexedLongDoc { Id = LongId, Code = wide });
            session.Store(new IndexedLongDoc { Id = ShortId, Code = "abc" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Query<IndexedLongDoc>().Where(x => x.Code == wide).ToListAsync(Ct))
            .Single().Id.ShouldBe(LongId);

        (await query.Query<IndexedLongDoc>().Where(x => x.Code == "abc").ToListAsync(Ct))
            .Single().Id.ShouldBe(ShortId);

        var values = new[] { wide, "nope" };
        (await query.Query<IndexedLongDoc>().Where(x => x.Code.IsOneOf(values)).ToListAsync(Ct))
            .Single().Id.ShouldBe(LongId);

        // Over the threshold, so through the OPENJSON path: the unpacked column has to widen too, or
        // the list side is truncated to the varchar(250) it was typed for.
        var many = Enumerable.Range(0, JsonValueList.Threshold + 5).Select(i => $"v{i}").Append(wide).ToArray();
        (await query.Query<IndexedLongDoc>().Where(x => x.Code.IsOneOf(many)).ToListAsync(Ct))
            .Single().Id.ShouldBe(LongId);
    }

    // ---- the other readers of a locator ---------------------------------------------------------------

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_join_qualifies_the_data_column(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        var ownerId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Store(new OwnerDoc { Id = ownerId, Text = Long });
            session.Store(new PetDoc { Id = Guid.NewGuid(), OwnerId = ownerId, Text = "pet" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        // Both tables have a `data` column, so an unqualified read inside the predicate is Msg 209 on
        // nvarchar storage, where the read is an OPENJSON subquery rather than a JSON_VALUE.
        var results = await query.Query<OwnerDoc>()
            .Where(x => x.Text!.Contains("needle"))
            .GroupJoin(query.Query<PetDoc>().Where(x => x.Text!.StartsWith("pe")),
                o => o.Id, p => p.OwnerId, (o, pets) => new { o, pets })
            .SelectMany(x => x.pets, (x, p) => new { OwnerId = x.o.Id, Pet = p.Text })
            .ToListAsync(Ct);

        results.Single().OwnerId.ShouldBe(ownerId);
    }

    public record LongEvent(string Payload);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task event_data_predicates_read_a_long_value(bool nativeJson)
    {
        ConfigureFor(nativeJson);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new LongEvent(Long), new LongEvent("short"));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        (await query.Events.QueryRawEventDataOnly<LongEvent>().Where(x => x.Payload.Contains("needle"))
                .ToListAsync(Ct))
            .Single().Payload.ShouldBe(Long);

        (await query.Events.QueryRawEventDataOnly<LongEvent>().Where(x => string.IsNullOrEmpty(x.Payload))
                .ToListAsync(Ct))
            .ShouldBeEmpty();
    }
}
