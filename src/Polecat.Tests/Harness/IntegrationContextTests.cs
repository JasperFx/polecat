namespace Polecat.Tests.Harness;

/// <summary>
///     Smoke tests verifying the test harness infrastructure works correctly.
/// </summary>
[Collection("integration")]
public class IntegrationContextTests : IntegrationContext
{
    public IntegrationContextTests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public void fixture_provides_database()
    {
        theDatabase.ShouldNotBeNull();
    }

    [Fact]
    public async Task shared_fixture_has_tables_created()
    {
        var tables = await SchemaInspector.GetTableNamesAsync();
        tables.ShouldContain("pc_streams");
        tables.ShouldContain("pc_events");
        tables.ShouldContain("pc_event_progression");
    }

    [Fact]
    public async Task can_open_connection_to_test_database()
    {
        var conn = await OpenConnectionAsync();
        conn.State.ShouldBe(System.Data.ConnectionState.Open);
    }

    [Fact]
    public async Task store_options_creates_custom_database()
    {
        await StoreOptions(opts =>
        {
            opts.Events.EnableCorrelationId = true;
        });

        // The custom database should have been applied
        theDatabase.ShouldNotBeNull();
    }
}
