using JasperFx;
using Polecat.Linq;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Linq;

/// <summary>
///     #916: <c>Select(...).First()</c> read its row as the document whatever the Select had asked for,
///     so a scalar failed as malformed JSON and an anonymous type as a bad cast.
/// </summary>
public class projected_single_value_tests: OneOffConfigurationsContext
{
    public class ProjectedAddress
    {
        public string City { get; set; } = string.Empty;
    }

    public enum ProjectedColor
    {
        Red,
        Blue
    }

    public class ProjectedDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public int? Rank { get; set; }
        public bool Active { get; set; }
        public ProjectedColor Color { get; set; }
        public DateTimeOffset When { get; set; }
        public ProjectedAddress? Address { get; set; }
    }

    public class ProjectedDto
    {
        public string? Label { get; set; }
    }

    public record ProjectedEvent(string Label, int Number);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly Guid OneId = Guid.NewGuid();
    private static readonly DateTimeOffset OneWhen = new(2026, 11, 2, 8, 30, 0, TimeSpan.Zero);

    private async Task<IQuerySession> seededAsync(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts => opts.UseNativeJsonType = nativeJson);

        await using var session = theStore.LightweightSession();
        session.Store(
            new ProjectedDoc
            {
                Id = OneId, Name = "one", Age = 1, Rank = 7, Active = true, Color = ProjectedColor.Blue,
                When = OneWhen, Address = new ProjectedAddress { City = "Oslo" }
            },
            new ProjectedDoc { Id = Guid.NewGuid(), Name = "two", Age = 2 });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static IQueryable<ProjectedDoc> ordered(IQuerySession query) => query.Query<ProjectedDoc>().OrderBy(x => x.Age);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_scalar_comes_back_as_itself_from_every_operator(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var one = ordered(query).Where(x => x.Age == 1);

        (await ordered(query).Select(x => x.Name).FirstAsync(Ct)).ShouldBe("one");
        (await ordered(query).Select(x => x.Name).FirstOrDefaultAsync(Ct)).ShouldBe("one");
        (await ordered(query).Select(x => x.Name).LastAsync(Ct)).ShouldBe("two");
        (await ordered(query).Select(x => x.Name).LastOrDefaultAsync(Ct)).ShouldBe("two");
        (await one.Select(x => x.Name).SingleAsync(Ct)).ShouldBe("one");
        (await one.Select(x => x.Name).SingleOrDefaultAsync(Ct)).ShouldBe("one");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task every_kind_of_scalar_reads_as_its_own_type(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var one = ordered(query).Where(x => x.Age == 1);

        (await one.Select(x => x.Age).FirstAsync(Ct)).ShouldBe(1);
        (await one.Select(x => x.Rank).FirstAsync(Ct)).ShouldBe(7);
        (await one.Select(x => x.Active).FirstAsync(Ct)).ShouldBeTrue();
        (await one.Select(x => x.Color).FirstAsync(Ct)).ShouldBe(ProjectedColor.Blue);
        (await one.Select(x => x.Id).FirstAsync(Ct)).ShouldBe(OneId);
        (await one.Select(x => x.When).FirstAsync(Ct)).ShouldBe(OneWhen);
        (await one.Select(x => x.Address!.City).SingleAsync(Ct)).ShouldBe("Oslo");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_null_is_a_value_and_not_a_missing_row(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        // Found, and null: First does not throw, and Single counts it.
        (await query.Query<ProjectedDoc>().Where(x => x.Age == 2).Select(x => x.Rank).FirstAsync(Ct)).ShouldBeNull();
        (await query.Query<ProjectedDoc>().Where(x => x.Age == 2).Select(x => x.Rank).SingleAsync(Ct)).ShouldBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_projected_object_comes_back_as_itself(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var one = ordered(query).Where(x => x.Age == 1);

        var anonymous = await one.Select(x => new { x.Name, x.Age }).FirstAsync(Ct);
        anonymous.Name.ShouldBe("one");
        anonymous.Age.ShouldBe(1);

        (await one.Select(x => new { x.Name, x.Age }).SingleOrDefaultAsync(Ct))!.Name.ShouldBe("one");
        (await ordered(query).Select(x => new { x.Name }).LastAsync(Ct)).Name.ShouldBe("two");
        (await one.Select(x => new ProjectedDto { Label = x.Name }).SingleAsync(Ct)).Label.ShouldBe("one");
        (await one.Select(x => x.Address).FirstAsync(Ct))!.City.ShouldBe("Oslo");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_projection_computed_in_memory_is_computed(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var one = ordered(query).Where(x => x.Age == 1);

        (await one.Select(x => x.Age + 10).FirstAsync(Ct)).ShouldBe(11);
        (await one.Select(x => x.Name + "!").SingleAsync(Ct)).ShouldBe("one!");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_inline_predicate_reads_the_projection(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);

        (await query.Query<ProjectedDoc>().Select(x => new { x.Name, x.Age }).FirstAsync(y => y.Age == 2, Ct))
            .Name.ShouldBe("two");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task no_rows_is_what_each_operator_says_it_is(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var none = query.Query<ProjectedDoc>().Where(x => x.Age == 99);

        (await none.Select(x => x.Name).FirstOrDefaultAsync(Ct)).ShouldBeNull();
        (await none.Select(x => x.Age).FirstOrDefaultAsync(Ct)).ShouldBe(0);
        (await none.Select(x => x.Rank).SingleOrDefaultAsync(Ct)).ShouldBeNull();
        (await none.Select(x => new { x.Name }).FirstOrDefaultAsync(Ct)).ShouldBeNull();
        (await none.Select(x => x.Age + 1).SingleOrDefaultAsync(Ct)).ShouldBe(0);

        (await Should.ThrowAsync<InvalidOperationException>(() => none.Select(x => x.Name).FirstAsync(Ct)))
            .Message.ShouldBe("Sequence contains no elements");
        (await Should.ThrowAsync<InvalidOperationException>(() => none.Select(x => x.Age).SingleAsync(Ct)))
            .Message.ShouldBe("Sequence contains no elements");
        (await Should.ThrowAsync<InvalidOperationException>(() => none.Select(x => new { x.Name }).SingleAsync(Ct)))
            .Message.ShouldBe("Sequence contains no elements");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task two_rows_is_too_many_for_single(bool nativeJson)
    {
        await using var query = await seededAsync(nativeJson);
        var both = query.Query<ProjectedDoc>();

        (await Should.ThrowAsync<InvalidOperationException>(() => both.Select(x => x.Name).SingleAsync(Ct)))
            .Message.ShouldBe("Sequence contains more than one element");
        (await Should.ThrowAsync<InvalidOperationException>(() => both.Select(x => x.Age).SingleOrDefaultAsync(Ct)))
            .Message.ShouldBe("Sequence contains more than one element");
        (await Should.ThrowAsync<InvalidOperationException>(() => both.Select(x => new { x.Name }).SingleAsync(Ct)))
            .Message.ShouldBe("Sequence contains more than one element");
        (await Should.ThrowAsync<InvalidOperationException>(
                () => both.Select(x => new { x.Name }).SingleOrDefaultAsync(Ct)))
            .Message.ShouldBe("Sequence contains more than one element");
    }

    [Fact]
    public async Task a_scalar_of_an_event_comes_back_as_itself()
    {
        ConfigureStore(_ => { });

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new ProjectedEvent("first", 1));
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var events = query.Events.QueryRawEventDataOnly<ProjectedEvent>();

        (await events.Select(x => x.Label).FirstAsync(Ct)).ShouldBe("first");
        (await events.Select(x => x.Number).SingleAsync(Ct)).ShouldBe(1);
        (await events.Where(x => x.Number == 2).Select(x => x.Label).FirstOrDefaultAsync(Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task an_event_query_still_refuses_a_projection_it_has_no_way_to_make()
    {
        ConfigureStore(_ => { });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using var query = theStore.QuerySession();

        await Should.ThrowAsync<BadLinqExpressionException>(() =>
            query.Events.QueryRawEventDataOnly<ProjectedEvent>().Select(x => new { x.Label }).FirstAsync(Ct));
    }
}
