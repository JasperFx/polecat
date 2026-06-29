using Polecat.Patching;
using Polecat.Tests.Harness;
using Shouldly;

namespace Polecat.Tests.Patching;

/// <summary>
///     #263: Patch().Set(...) on DateTime/DateTimeOffset/DateOnly/TimeOnly properties bound the
///     raw CLR value into JSON_MODIFY. DateTime/DateTimeOffset bound a SQL datetimeoffset, which
///     JSON_MODIFY rejects ("Argument data type datetimeoffset is invalid for argument 3"), and
///     DateOnly/TimeOnly fell through to the complex JSON_QUERY path and produced malformed JSON.
///     All four must now serialize to their System.Text.Json string form and round-trip cleanly.
/// </summary>
[Collection("integration")]
public class patching_datetime : IntegrationContext
{
    public patching_datetime(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    public override async Task InitializeAsync()
    {
        await StoreOptions(opts => { opts.DatabaseSchemaName = "patching_dates"; });
    }

    [Fact]
    public async Task set_datetime_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new DateTime(2026, 6, 29, 12, 13, 26, DateTimeKind.Utc);
        theSession.Patch<Target>(target.Id).Set(x => x.DateTime, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.DateTime.ShouldBe(expected);
    }

    [Fact]
    public async Task set_datetimeoffset_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new DateTimeOffset(2026, 6, 29, 12, 13, 26, TimeSpan.FromHours(2));
        theSession.Patch<Target>(target.Id).Set(x => x.DateTimeOffset, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.DateTimeOffset.ShouldBe(expected);
    }

    [Fact]
    public async Task set_dateonly_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new DateOnly(2026, 6, 29);
        theSession.Patch<Target>(target.Id).Set(x => x.DateOnly, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.DateOnly.ShouldBe(expected);
    }

    [Fact]
    public async Task set_timeonly_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new TimeOnly(12, 13, 26);
        theSession.Patch<Target>(target.Id).Set(x => x.TimeOnly, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.TimeOnly.ShouldBe(expected);
    }

    [Fact]
    public async Task set_nullable_datetimeoffset_property()
    {
        var target = Target.Random();
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new DateTimeOffset(2026, 6, 29, 12, 13, 26, TimeSpan.FromHours(2));
        theSession.Patch<Target>(target.Id).Set(x => x.NullableDateTimeOffset, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.NullableDateTimeOffset.ShouldBe(expected);
    }

    [Fact]
    public async Task set_datetimeoffset_by_where_clause()
    {
        var target = new Target { Id = Guid.NewGuid(), Color = "Blue" };
        theSession.Store(target);
        await theSession.SaveChangesAsync();

        var expected = new DateTimeOffset(2026, 6, 29, 12, 13, 26, TimeSpan.FromHours(2));
        theSession.Patch<Target>(x => x.Color == "Blue").Set(x => x.DateTimeOffset, expected);
        await theSession.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        (await query.LoadAsync<Target>(target.Id))!.DateTimeOffset.ShouldBe(expected);
    }
}
