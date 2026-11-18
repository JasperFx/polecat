using JasperFx.Events;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Daemon;

[Collection("integration")]
public class all_databases_tests : IntegrationContext
{
    public all_databases_tests(DefaultStoreFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task returns_one_event_database_for_single_database_store()
    {
        var databases = await ((IEventStore)theStore).AllDatabases();

        databases.Count.ShouldBe(1);
        databases.ShouldAllBe(db => db is PolecatDatabase);
    }

    [Fact]
    public async Task each_database_can_serve_projection_progress()
    {
        // The whole point of the abstraction (jasperfx#387) is that store-agnostic tooling can
        // reach an IEventDatabase and call the read members without referencing concrete types.
        var databases = await ((IEventStore)theStore).AllDatabases();

        foreach (var database in databases)
        {
            var progress = await database.AllProjectionProgress(CancellationToken.None);
            progress.ShouldNotBeNull();
        }
    }
}
