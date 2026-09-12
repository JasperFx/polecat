using JasperFx;
using JasperFx.Descriptors;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using Polecat.Storage;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;

namespace Polecat.Tests.ExplorerApi;

/// <summary>
///     #584 / jasperfx#810 — the database dimension of the explorer reads, on a store that actually
///     has more than one database.
/// </summary>
/// <remarks>
///     <para>
///         <c>EventStoreExplorerCompliance</c> covers the single-database half: there, the database
///         overload and the store-global read are the same read, so all it can pin is that they agree.
///         The failure this issue exists to stop only appears once there are two databases — a
///         store-global read answering from whichever one the default session resolved, with nothing
///         in the result saying so. That needs a database-per-tenant fixture, which is what this is.
///     </para>
///     <para>
///         Each tenant database gets a stream that exists ONLY there, so a read that silently answered
///         from one database would come back missing the other's stream rather than throwing — the
///         assertions below are written to catch that shape, not just an exception type.
///     </para>
/// </remarks>
public class database_scoped_explorer_reads_tests : IAsyncLifetime
{
    private const string TenantA = "explorer_tenant_a";
    private const string TenantB = "explorer_tenant_b";

    // Scoped per CLAUDE.md: a database a test creates is a SIBLING of the worker's own catalog, not a
    // child of it, so a hardcoded name would be one database every parallel worker fights over.
    private static readonly string DbA = ConnectionSource.Scoped("explorer_a");
    private static readonly string DbB = ConnectionSource.Scoped("explorer_b");

    private static string TenantConnectionString(string dbName) =>
        ConnectionSource.ConnectionStringFor(dbName);

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = TenantConnectionString(DbA);
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DatabaseSchemaName = "explorer_by_db";

            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant(TenantA, TenantConnectionString(DbA));
                tenancy.AddTenant(TenantB, TenantConnectionString(DbB));
            });
        });
    }

    /// <summary>
    ///     Appends one stream per tenant database and returns (streamInA, streamInB). Each stream id
    ///     exists in exactly one of the two databases.
    /// </summary>
    private static async Task<(Guid inA, Guid inB)> SeedAsync(DocumentStore store, CancellationToken ct)
    {
        var inA = Guid.NewGuid();
        var inB = Guid.NewGuid();

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantA }))
        {
            session.Events.StartStream<QuestParty>(inA, new QuestStarted("a"), new MembersJoined(1, "Rivendell", ["Frodo"]));
            await session.SaveChangesAsync(ct);
        }

        await using (var session = store.LightweightSession(new SessionOptions { TenantId = TenantB }))
        {
            session.Events.StartStream<QuestParty>(inB, new QuestStarted("b"));
            await session.SaveChangesAsync(ct);
        }

        return (inA, inB);
    }

    [Fact]
    public async Task recent_streams_scoped_to_a_database_returns_that_database_only()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, inB) = await SeedAsync(store, ct);

        var eventStore = (IEventStore)store;
        var databases = await eventStore.AllDatabases();
        databases.Count.ShouldBe(2);

        // Attribute each answer to the database it came from — the whole point of the overload.
        var perDatabase = new Dictionary<string, List<string>>();
        foreach (var database in databases)
        {
            var streams = await eventStore.GetRecentStreamsAsync(database, 10, null, ct);
            perDatabase[database.Identifier] = streams.Select(x => x.StreamId).ToList();
        }

        // Exactly one database holds each stream, and it is not the same one.
        perDatabase.Values.Count(x => x.Contains(inA.ToString())).ShouldBe(1);
        perDatabase.Values.Count(x => x.Contains(inB.ToString())).ShouldBe(1);
        perDatabase.Single(x => x.Value.Contains(inA.ToString())).Key
            .ShouldNotBe(perDatabase.Single(x => x.Value.Contains(inB.ToString())).Key);
    }

    [Fact]
    public async Task store_global_recent_streams_fans_out_rather_than_answering_from_one_database()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, inB) = await SeedAsync(store, ct);

        var streams = await ((IEventStore)store).GetRecentStreamsAsync(10, ct);
        var ids = streams.Select(x => x.StreamId).ToList();

        // The pre-#584 behaviour returned whichever database the default session resolved, so this
        // would have carried inA and silently dropped inB.
        ids.ShouldContain(inA.ToString());
        ids.ShouldContain(inB.ToString());
    }

    [Fact]
    public async Task the_fan_out_respects_the_overall_count_cap()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        await SeedAsync(store, ct);

        // A cap of 1 over two databases must yield one stream, not one PER database.
        var streams = await ((IEventStore)store).GetRecentStreamsAsync(1, ct);

        streams.Count.ShouldBe(1);
    }

    [Fact]
    public async Task stream_metadata_scoped_to_a_database_is_null_in_the_other_one()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, _) = await SeedAsync(store, ct);

        var eventStore = (IEventStore)store;
        var databases = await eventStore.AllDatabases();

        var answers = new List<StreamMetadata?>();
        foreach (var database in databases)
        {
            answers.Add(await eventStore.GetStreamMetadataAsync(database, inA.ToString(), null, ct));
        }

        // "No such stream in THIS database" rather than "no such stream" — the distinction a
        // store-global read cannot make once there is more than one database.
        answers.Count(x => x != null).ShouldBe(1);
        answers.Single(x => x != null)!.Version.ShouldBe(2);
    }

    [Fact]
    public async Task stream_events_scoped_to_a_database_read_only_that_database()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, _) = await SeedAsync(store, ct);

        var eventStore = (IEventStore)store;
        var databases = await eventStore.AllDatabases();

        var counts = new List<int>();
        foreach (var database in databases)
        {
            var versions = new List<long>();
            await foreach (var e in eventStore.ReadStreamAsync(database, inA.ToString(), null, ct))
            {
                versions.Add(e.StreamVersion);
            }

            counts.Add(versions.Count);
        }

        counts.OrderDescending().ShouldBe(new[] { 2, 0 });
    }

    [Fact]
    public async Task projection_statuses_scoped_to_a_database_read_that_databases_progression()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        await SeedAsync(store, ct);

        var eventStore = (IEventStore)store;
        var databases = await eventStore.AllDatabases();

        // Progression rows live in the database whose events they track, so this has to answer per
        // database rather than throw the way its store-global sibling now does.
        foreach (var database in databases)
        {
            var statuses = await eventStore.GetProjectionStatusesAsync(database, null, ct);
            statuses.ShouldNotBeNull();
        }
    }

    // ---- the store-global reads that refuse rather than answering from one database ----
    //
    // Each of these returns ONE database's worth of answer by nature, so there is no merge that is
    // not a fabrication: a stream's events out of two databases would interleave two independent
    // version sequences, a stream's metadata is a single row, and progression rows named
    // "MyProjection:All" exist once per database. Refusing and naming the database overload is the
    // honest answer; answering from one database is the option #584 rules out.

    [Fact]
    public async Task store_global_stream_metadata_refuses_on_a_multi_database_store()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, _) = await SeedAsync(store, ct);

        var ex = await Should.ThrowAsync<NotSupportedException>(
            () => ((IEventStore)store).GetStreamMetadataAsync(inA.ToString(), ct));

        ex.Message.ShouldContain("AllDatabases");
    }

    [Fact]
    public async Task store_global_stream_read_refuses_on_a_multi_database_store()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        var (inA, _) = await SeedAsync(store, ct);

        await Should.ThrowAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in ((IEventStore)store).ReadStreamAsync(inA.ToString(), ct))
            {
            }
        });
    }

    [Fact]
    public async Task store_global_projection_statuses_refuse_on_a_multi_database_store()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();
        await SeedAsync(store, ct);

        await Should.ThrowAsync<NotSupportedException>(
            () => ((IEventStore)store).GetProjectionStatusesAsync(ct));
    }

    [Fact]
    public async Task a_null_database_is_rejected_rather_than_quietly_meaning_store_global()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = CreateStore();

        await Should.ThrowAsync<ArgumentNullException>(
            () => ((IEventStore)store).GetRecentStreamsAsync(null!, 10, null, ct));
    }
}
