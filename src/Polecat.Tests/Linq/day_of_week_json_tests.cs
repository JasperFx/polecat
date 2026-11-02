using System.Text.Json;
using Polecat.Linq;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Linq;

/// <summary>
///     #875: <c>x.Created.DayOfWeek</c> is the one computed value with an enum on the CLR side, and SQL
///     computes it as a number. Under <see cref="EnumStorage.AsString" /> every enum in a stored document is
///     a name, so a JSON projection wrote the one number a consumer of that JSON would not expect, and in a
///     form a strict enum converter refuses.
/// </summary>
public class day_of_week_json_tests: OneOffConfigurationsContext
{
    public class DayDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTimeOffset Created { get; set; }
        public DayOfWeek Stored { get; set; }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // A Monday.
    private static readonly DateTimeOffset Monday = new(2026, 1, 5, 12, 0, 0, TimeSpan.Zero);

    private async Task<IQuerySession> seededAsync(EnumStorage storage, Casing casing = Casing.CamelCase)
    {
        Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "JSON streaming needs the native json type.");

        ConfigureStore(opts => opts.ConfigureSerialization(storage, casing));
        await theStore.Advanced.CleanAsync<DayDoc>(Ct);

        await using var session = theStore.LightweightSession();
        session.Store(new DayDoc { Id = Guid.NewGuid(), Name = "a", Created = Monday, Stored = Monday.DayOfWeek });
        session.Store(new DayDoc
        {
            Id = Guid.NewGuid(), Name = "b", Created = Monday.AddDays(6), Stored = Monday.AddDays(6).DayOfWeek
        });
        await session.SaveChangesAsync(Ct);

        return theStore.QuerySession();
    }

    private static JsonElement[] rows(string json) => JsonDocument.Parse(json).RootElement.EnumerateArray().ToArray();

    [Theory]
    [InlineData(Casing.Default, "Day", "Stored", "monday", "sunday")]
    [InlineData(Casing.CamelCase, "day", "stored", "monday", "sunday")]
    [InlineData(Casing.SnakeCase, "day", "stored", "monday", "sunday")]
    public async Task a_projection_writes_the_name_a_stored_enum_would_have(Casing casing, string dayKey,
        string storedKey, string monday, string sunday)
    {
        await using var query = await seededAsync(EnumStorage.AsString, casing);

        var json = await query.Query<DayDoc>().OrderBy(x => x.Name)
            .Select(x => new { Day = x.Created.DayOfWeek, x.Stored }).ToJsonArrayAsync(Ct);

        var read = rows(json);

        // Whatever the casing writes, the computed value and the stored one have to agree.
        read[0].GetProperty(dayKey).GetString().ShouldBe(read[0].GetProperty(storedKey).GetString());
        read[1].GetProperty(dayKey).GetString().ShouldBe(read[1].GetProperty(storedKey).GetString());

        read[0].GetProperty(dayKey).GetString()!.ToLowerInvariant().ShouldBe(monday);
        read[1].GetProperty(dayKey).GetString()!.ToLowerInvariant().ShouldBe(sunday);
    }

    [Fact]
    public async Task a_group_key_writes_the_name()
    {
        await using var query = await seededAsync(EnumStorage.AsString);

        var json = await query.Query<DayDoc>().GroupBy(x => x.Created.DayOfWeek)
            .Select(g => new { g.Key, Count = g.Count() }).ToJsonArrayAsync(Ct);

        rows(json).Select(x => x.GetProperty("key").GetString()!.ToLowerInvariant()).OrderBy(x => x)
            .ShouldBe(["monday", "sunday"]);
    }

    [Fact]
    public async Task a_materialized_projection_reads_the_name_back()
    {
        await using var query = await seededAsync(EnumStorage.AsString);

        var groups = await query.Query<DayDoc>().GroupBy(x => x.Created.DayOfWeek)
            .Select(g => new { g.Key, Count = g.Count() }).ToListAsync(Ct);

        groups.Select(x => x.Key).OrderBy(x => x).ShouldBe([DayOfWeek.Sunday, DayOfWeek.Monday]);
    }

    [Fact]
    public async Task a_predicate_still_compares_the_number()
    {
        await using var query = await seededAsync(EnumStorage.AsString);

        var rows = await query.Query<DayDoc>().Where(x => x.Created.DayOfWeek == DayOfWeek.Sunday).ToListAsync(Ct);

        rows.Single().Name.ShouldBe("b");
    }

    [Fact]
    public async Task integer_storage_writes_the_number()
    {
        await using var query = await seededAsync(EnumStorage.AsInteger);

        var json = await query.Query<DayDoc>().OrderBy(x => x.Name)
            .Select(x => new { Day = x.Created.DayOfWeek }).ToJsonArrayAsync(Ct);

        rows(json).Select(x => x.GetProperty("day").GetInt32()).ShouldBe([1, 0]);
    }
}
