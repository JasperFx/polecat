using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #875: <c>JSON_VALUE</c> is NULL for an object and for an array, present or not, so a null check on
///     either read every row as null. <c>x.Address == null</c> matched the whole table and
///     <c>x.Address != null</c> matched none of it.
/// </summary>
public class object_valued_member_tests: OneOffConfigurationsContext
{
    public class ObjectAddress
    {
        public string City { get; set; } = string.Empty;
    }

    public struct ObjectMoney
    {
        public decimal Amount { get; set; }
    }

    public class ObjectDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ObjectAddress? Address { get; set; }
        public ObjectMoney? Cost { get; set; }
        public List<string>? Tags { get; set; }
        public int[]? Numbers { get; set; }
        public Dictionary<string, int>? Map { get; set; }
        public object? Anything { get; set; }
    }

    public record ObjectEvent(string Label, ObjectAddress? Address);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly ObjectDoc Full = new()
    {
        Id = Guid.NewGuid(),
        Name = "full",
        Address = new ObjectAddress { City = "Oslo" },
        Cost = new ObjectMoney { Amount = 5 },
        Tags = ["a"],
        Numbers = [1],
        Map = new Dictionary<string, int> { ["a"] = 1 },
        Anything = new ObjectAddress { City = "Rome" }
    };

    // Present but empty is not null: this is the row a check on the element count would get wrong.
    private static readonly ObjectDoc Empty = new()
    {
        Id = Guid.NewGuid(), Name = "empty", Address = new ObjectAddress(), Cost = new ObjectMoney(), Tags = [],
        Numbers = [], Map = [], Anything = "text"
    };

    private static readonly ObjectDoc Absent = new() { Id = Guid.NewGuid(), Name = "absent" };

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(Full, Empty, Absent);
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static async Task<string[]> namesAsync(IQueryable<ObjectDoc> query)
        => (await query.OrderBy(x => x.Name).ToListAsync(Ct)).Select(x => x.Name).ToArray();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_object_is_null_only_when_it_is_null(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Address == null))).ShouldBe(["absent"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Address != null))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => !(x.Address == null)))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Address!.Equals(null)))).ShouldBe(["absent"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_nullable_struct_stored_as_an_object_is_null_only_when_it_is_null(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Cost == null))).ShouldBe(["absent"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Cost != null))).ShouldBe(["empty", "full"]);

        // The same answer HasValue gives, which already looked for an object.
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Cost.HasValue))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => !x.Cost.HasValue))).ShouldBe(["absent"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_collection_is_null_only_when_it_is_null(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Tags == null))).ShouldBe(["absent"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Tags != null))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Numbers != null))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Map != null))).ShouldBe(["empty", "full"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_member_typed_object_is_checked_whichever_shape_it_holds(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Anything != null))).ShouldBe(["empty", "full"]);
        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Anything == null))).ShouldBe(["absent"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_captured_null_is_the_same_check(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        ObjectAddress? none = null;

        (await namesAsync(query.Query<ObjectDoc>().Where(x => x.Address == none))).ShouldBe(["absent"]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_check_inside_a_group_aggregate_sees_the_object(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        var counts = await query.Query<ObjectDoc>().GroupBy(x => x.Name)
            .Select(g => new { g.Key, Nulls = g.Count(x => x.Address == null) }).ToListAsync(Ct);

        counts.Single(x => x.Key == "full").Nulls.ShouldBe(0);
        counts.Single(x => x.Key == "absent").Nulls.ShouldBe(1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_check_on_an_event_body_sees_the_object(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);
        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(),
                new ObjectEvent("with", new ObjectAddress { City = "Oslo" }), new ObjectEvent("without", null));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        var with = await query.Events.QueryRawEventDataOnly<ObjectEvent>().Where(x => x.Address != null).ToListAsync(Ct);
        with.Single().Label.ShouldBe("with");

        var without = await query.Events.QueryRawEventDataOnly<ObjectEvent>().Where(x => x.Address == null)
            .ToListAsync(Ct);
        without.Single().Label.ShouldBe("without");
    }

    [Fact]
    public async Task projecting_more_than_a_scalar_from_an_event_body_is_refused()
    {
        ConfigureStore(_ => { });
        await theStore.Advanced.CleanAllEventDataAsync(Ct);

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new ObjectEvent("with", new ObjectAddress { City = "Oslo" }));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();

        // Nothing on this path projects in memory, and the member can no longer read as a null per row.
        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryRawEventDataOnly<ObjectEvent>().Select(x => x.Address).ToListAsync(Ct));

        var labels = await query.Events.QueryRawEventDataOnly<ObjectEvent>().Select(x => x.Label).ToListAsync(Ct);
        labels.ShouldBe(["with"]);
    }

    [Fact]
    public async Task comparing_an_object_member_to_an_object_is_refused()
    {
        await using var query = await seededAsync(ConnectionSource.SupportsNativeJson);

        var address = new ObjectAddress { City = "Oslo" };

        // This reached SqlClient, which said only that it had no mapping for the type.
        var ex = await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Query<ObjectDoc>().Where(x => x.Address == address).ToListAsync(Ct));

        ex.Message.ShouldContain("Address");
    }
}
