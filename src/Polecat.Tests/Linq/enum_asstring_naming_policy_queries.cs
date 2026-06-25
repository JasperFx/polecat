using Polecat.Linq;
using Polecat.Tests.Harness;
using Weasel.Core;

namespace Polecat.Tests.Linq;

/// <summary>
/// #222: With EnumStorage.AsString, enum *values* are serialized through the configured
/// JsonNamingPolicy (Minute → "minute" under CamelCase), but a LINQ equality predicate used to
/// compare against the raw PascalCase Enum.ToString() — so the predicate emitted
/// JSON_VALUE(...) = 'Minute', never matched the stored 'minute', and the query silently
/// returned nothing. These tests pin the corrected behavior across every casing.
/// </summary>
public class enum_asstring_naming_policy_queries : OneOffConfigurationsContext
{
    public enum Granularity
    {
        Minute,
        Hour,
        FifteenMinute // multi-word: exercises full-name policy conversion, not just first-letter
    }

    public class Sample
    {
        public Guid Id { get; set; }
        public Granularity Granularity { get; set; }
    }

    private async Task SeedAsync(Casing casing)
    {
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsString, casing));

        await using var session = theStore.LightweightSession();
        session.Store(new Sample { Id = Guid.NewGuid(), Granularity = Granularity.Minute });
        session.Store(new Sample { Id = Guid.NewGuid(), Granularity = Granularity.Minute });
        session.Store(new Sample { Id = Guid.NewGuid(), Granularity = Granularity.Hour });
        session.Store(new Sample { Id = Guid.NewGuid(), Granularity = Granularity.FifteenMinute });
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task equality_matches_under_camel_case()
    {
        await SeedAsync(Casing.CamelCase);

        await using var query = theStore.QuerySession();

        // The exact repro from the issue: stored as "minute", must match.
        var minutes = await query.Query<Sample>()
            .Where(x => x.Granularity == Granularity.Minute)
            .ToListAsync();
        minutes.Count.ShouldBe(2);

        var hours = await query.Query<Sample>()
            .CountAsync(x => x.Granularity == Granularity.Hour);
        hours.ShouldBe(1);
    }

    [Fact]
    public async Task equality_matches_multiword_member_under_camel_case()
    {
        await SeedAsync(Casing.CamelCase);

        await using var query = theStore.QuerySession();

        // FifteenMinute → "fifteenMinute" (not "FifteenMinute", not "fifteenminute").
        var count = await query.Query<Sample>()
            .CountAsync(x => x.Granularity == Granularity.FifteenMinute);
        count.ShouldBe(1);
    }

    [Fact]
    public async Task equality_matches_multiword_member_under_snake_case()
    {
        await SeedAsync(Casing.SnakeCase);

        await using var query = theStore.QuerySession();

        // FifteenMinute → "fifteen_minute".
        var count = await query.Query<Sample>()
            .CountAsync(x => x.Granularity == Granularity.FifteenMinute);
        count.ShouldBe(1);
    }

    [Fact]
    public async Task equality_still_matches_under_default_casing()
    {
        // Regression guard for the identity policy (null naming policy) path: PascalCase
        // round-trips, so ToString() is correct and must keep working.
        await SeedAsync(Casing.Default);

        await using var query = theStore.QuerySession();

        var count = await query.Query<Sample>()
            .CountAsync(x => x.Granularity == Granularity.Minute);
        count.ShouldBe(2);
    }

    [Fact]
    public async Task not_equal_predicate_matches_under_camel_case()
    {
        await SeedAsync(Casing.CamelCase);

        await using var query = theStore.QuerySession();

        // != Minute → the Hour and FifteenMinute rows (2).
        var results = await query.Query<Sample>()
            .Where(x => x.Granularity != Granularity.Minute)
            .ToListAsync();
        results.Count.ShouldBe(2);
        results.ShouldAllBe(x => x.Granularity != Granularity.Minute);
    }

    [Fact]
    public async Task contains_membership_matches_under_camel_case()
    {
        await SeedAsync(Casing.CamelCase);

        var wanted = new[] { Granularity.Hour, Granularity.FifteenMinute };

        await using var query = theStore.QuerySession();
        var results = await query.Query<Sample>()
            .Where(x => wanted.Contains(x.Granularity))
            .ToListAsync();
        results.Count.ShouldBe(2);
    }

    [Fact]
    public async Task as_integer_storage_is_unaffected()
    {
        // Sanity: the AsInteger path is untouched by the fix.
        ConfigureStore(opts => opts.ConfigureSerialization(EnumStorage.AsInteger, Casing.CamelCase));

        await using (var session = theStore.LightweightSession())
        {
            session.Store(new Sample { Id = Guid.NewGuid(), Granularity = Granularity.Hour });
            await session.SaveChangesAsync();
        }

        await using var query = theStore.QuerySession();
        var count = await query.Query<Sample>()
            .CountAsync(x => x.Granularity == Granularity.Hour);
        count.ShouldBe(1);
    }
}
