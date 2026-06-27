using JasperFx;
using JasperFx.Events;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Metadata;

/// <summary>
/// #256: exact-match metadata filters on the event read store's <see cref="IReadOnlyEventStore.QueryEventsAsync"/>
/// (parity with the JasperFx EventQuery surface). CorrelationId / CausationId / UserName are honored only
/// when the option is set AND the event store captures that column (EnableCorrelationId / EnableCausationId /
/// EnableUserName, #237). UserName is sourced from the session's LastModifiedBy (#237/#239).
///
/// Seeding: 8 single-event streams covering every combination of correlation ∈ {c0,c1} ×
/// causation ∈ {u0,u1} × user ∈ {b0,b1}, indexed 0..7 by the (corr,caus,user) binary tuple.
/// </summary>
public class event_metadata_filter_tests
{
    public record MetricRecorded(double Value);

    private static async Task<DocumentStore> CreateStoreAsync(
        string schema, bool enableCorr = true, bool enableCaus = true, bool enableUser = true)
    {
        await using (var conn = new Microsoft.Data.SqlClient.SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            // Drop the event tables so each schema starts clean across reruns.
            cmd.CommandText = $"""
                IF OBJECT_ID('[{schema}].[pc_events]','U') IS NOT NULL DROP TABLE [{schema}].[pc_events];
                IF OBJECT_ID('[{schema}].[pc_streams]','U') IS NOT NULL DROP TABLE [{schema}].[pc_streams];
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.EnableCorrelationId = enableCorr;
            opts.Events.EnableCausationId = enableCaus;
            opts.Events.EnableUserName = enableUser;
        });
    }

    private static async Task SeedMatrixAsync(DocumentStore store)
    {
        for (var i = 0; i < 8; i++)
        {
            await using var session = store.LightweightSession();
            session.CorrelationId = (i & 0b100) == 0 ? "c0" : "c1";
            session.CausationId = (i & 0b010) == 0 ? "u0" : "u1";
            session.LastModifiedBy = (i & 0b001) == 0 ? "b0" : "b1"; // → event.UserName
            session.Events.StartStream(Guid.NewGuid(), new MetricRecorded(i));
            await session.SaveChangesAsync();
        }
    }

    private static Task<PagedEvents> QueryAsync(DocumentStore store, EventQuery query)
        => ((IEventStore)store).OpenReadOnlyEventStore().QueryEventsAsync(query, default);

    [Theory]
    // (corr, caus, user, expectedCount)
    [InlineData(null, null, null, 8)]
    [InlineData("c0", null, null, 4)]
    [InlineData(null, "u0", null, 4)]
    [InlineData(null, null, "b0", 4)]
    [InlineData("c0", "u0", null, 2)]
    [InlineData("c0", null, "b0", 2)]
    [InlineData(null, "u0", "b0", 2)]
    [InlineData("c0", "u0", "b0", 1)]
    public async Task every_filter_combo_returns_the_expected_subset(
        string? corr, string? caus, string? user, int expectedCount)
    {
        await using var store = await CreateStoreAsync("evt256_combo");
        await SeedMatrixAsync(store);

        var result = await QueryAsync(store, new EventQuery
        {
            PageSize = 50,
            CorrelationId = corr,
            CausationId = caus,
            UserName = user
        });

        result.TotalCount.ShouldBe(expectedCount);
        result.Events.Count.ShouldBe(expectedCount);

        // Every returned event must actually match each set filter.
        foreach (var e in result.Events)
        {
            if (corr != null) e.CorrelationId.ShouldBe(corr);
            if (caus != null) e.CausationId.ShouldBe(caus);
            if (user != null) e.UserName.ShouldBe(user);
        }
    }

    [Fact]
    public async Task filter_on_unmatched_value_returns_zero()
    {
        await using var store = await CreateStoreAsync("evt256_unmatched");
        await SeedMatrixAsync(store);

        var result = await QueryAsync(store, new EventQuery { PageSize = 50, CorrelationId = "nope" });

        result.TotalCount.ShouldBe(0);
        result.Events.Count.ShouldBe(0);
    }

    [Theory]
    [InlineData(false, true, true, "correlation")]
    [InlineData(true, false, true, "causation")]
    [InlineData(true, true, false, "username")]
    public async Task filter_is_silently_ignored_when_its_column_is_disabled(
        bool enableCorr, bool enableCaus, bool enableUser, string which)
    {
        await using var store = await CreateStoreAsync($"evt256_off_{which}", enableCorr, enableCaus, enableUser);
        await SeedMatrixAsync(store);

        var query = which switch
        {
            "correlation" => new EventQuery { PageSize = 50, CorrelationId = "c0" },
            "causation" => new EventQuery { PageSize = 50, CausationId = "u0" },
            _ => new EventQuery { PageSize = 50, UserName = "b0" }
        };

        var result = await QueryAsync(store, query);

        result.TotalCount.ShouldBe(8); // disabled column → filter dropped → all events
    }

    [Fact]
    public async Task disabled_filter_does_not_suppress_an_enabled_one()
    {
        // causation disabled, correlation + user enabled.
        await using var store = await CreateStoreAsync("evt256_mixed", enableCorr: true, enableCaus: false, enableUser: true);
        await SeedMatrixAsync(store);

        var result = await QueryAsync(store, new EventQuery
        {
            PageSize = 50,
            CorrelationId = "c0", // honored
            CausationId = "u0",    // ignored (disabled)
            UserName = "b0"        // honored
        });

        result.TotalCount.ShouldBe(2); // c0 ∩ b0
    }

    [Fact]
    public async Task event_type_name_and_stream_id_filters_apply()
    {
        await using var store = await CreateStoreAsync("evt256_type_stream");
        var streamA = Guid.NewGuid();
        var streamB = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamA, new MetricRecorded(1), new MetricRecorded(2));
            session.Events.StartStream(streamB, new MetricRecorded(3));
            await session.SaveChangesAsync();
        }

        // EventTypeName matches the only type → all 3 events.
        var all = await QueryAsync(store, new EventQuery { PageSize = 50, EventTypeName = "metric_recorded" });
        all.TotalCount.ShouldBe(3);

        // Bogus EventTypeName → none.
        var none = await QueryAsync(store, new EventQuery { PageSize = 50, EventTypeName = "no_such_type" });
        none.TotalCount.ShouldBe(0);

        // StreamId scopes to a single stream.
        var streamScoped = await QueryAsync(store, new EventQuery { PageSize = 50, StreamId = streamA.ToString() });
        streamScoped.TotalCount.ShouldBe(2);
        streamScoped.Events.ShouldAllBe(e => e.StreamId == streamA);
    }

    [Fact]
    public async Task paging_limits_and_reports_total()
    {
        await using var store = await CreateStoreAsync("evt256_paging");
        await SeedMatrixAsync(store); // 8 events

        var page1 = await QueryAsync(store, new EventQuery { PageNumber = 1, PageSize = 3 });
        page1.TotalCount.ShouldBe(8); // full count regardless of page size
        page1.Events.Count.ShouldBe(3);
        page1.PageNumber.ShouldBe(1);
        page1.PageSize.ShouldBe(3);

        var page3 = await QueryAsync(store, new EventQuery { PageNumber = 3, PageSize = 3 });
        page3.Events.Count.ShouldBe(2); // 8 = 3 + 3 + 2
    }
}
