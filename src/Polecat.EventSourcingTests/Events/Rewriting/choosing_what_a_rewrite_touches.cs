using System.Buffers.Binary;
using System.Text.Json;
using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Events.Rewriting;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     Writes a fresh prefix in front of the JSON, the way an encrypting serializer with a random IV never writes
///     the same bytes twice for the same event.
/// </summary>
public class NeverTheSameBytesBinarySerializer : JasperFx.Events.IEventBinarySerializer
{
    // A counter rather than a random byte: one random byte repeats one time in 256, which makes a rewrite of the
    // same event write the same bytes and count as unchanged now and then (Marten found this in #5645)
    private static int _writes;

    public byte[] Serialize(Type type, object data)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, type);
        var bytes = new byte[json.Length + sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, Interlocked.Increment(ref _writes));
        json.CopyTo(bytes, sizeof(int));
        return bytes;
    }

    public object Deserialize(Type type, byte[] data) => JsonSerializer.Deserialize(data.AsSpan(sizeof(int)), type)!;
}

/// <summary>
///     #1034 (marten#5643): which rows a rewrite reads and writes.
/// </summary>
public class choosing_what_a_rewrite_touches : OneOffConfigurationsContext
{
    public record PersonRegisteredV1(string Name);

    public record PersonRegistered(string FullName);

    public record PersonMoved(string City);

    private readonly List<DocumentStore> _others = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public override async ValueTask DisposeAsync()
    {
        foreach (var store in _others) store.Dispose();
        await base.DisposeAsync();
    }

    private void withUpcaster(Action<StoreOptions>? configure = null) => ConfigureStore(opts =>
    {
        opts.Events.Upcast<PersonRegisteredV1, PersonRegistered>(x => new PersonRegistered(x.Name));
        opts.Events.AddEventType<PersonMoved>();
        configure?.Invoke(opts);
    });

    // A second store over this class's schema
    private DocumentStore separateStore(Action<StoreOptions> configure)
    {
        var options = new StoreOptions
        {
            ConnectionString = theStore.Options.ConnectionString,
            DatabaseSchemaName = theStore.Options.DatabaseSchemaName,
            AutoCreateSchemaObjects = AutoCreate.All,
            UseNativeJsonType = theStore.Options.UseNativeJsonType
        };
        configure(options);

        var store = new DocumentStore(options);
        _others.Add(store);
        return store;
    }

    private string aliasOf<T>() => theStore.Options.EventGraph.EventMappingFor(typeof(T)).EventTypeName;

    private async Task<Guid> appendAsync(params object[] events)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, events);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    private async Task<List<(string Type, string Tenant, bool Binary)>> storedRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT type, tenant_id, CASE WHEN bdata IS NULL THEN 0 ELSE 1 END FROM {theStore.Options.EventGraph.EventsTableName} ORDER BY seq_id";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        var rows = new List<(string, string, bool)>();
        while (await reader.ReadAsync(Ct)) rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2) == 1));
        return rows;
    }

    [Fact]
    public async Task only_events_still_stored_as_json_are_read_when_the_serializer_never_writes_the_same_bytes_twice()
    {
        ConfigureStore(opts => opts.Events.AddEventType<PersonMoved>());
        var streamId = await appendAsync(new PersonMoved("Utrecht"), new PersonMoved("Leiden"));

        var binary = separateStore(opts =>
            opts.Events.UseBinarySerializer<PersonMoved>(new NeverTheSameBytesBinarySerializer()));

        var first = await binary.Advanced.RewriteStoredEventsAsync(x =>
            x.IncludeType<PersonMoved>().OnlyEventsStoredAsJson(), Ct);
        first.Databases.Single().Rewritten.ShouldBe(2);
        (await storedRowsAsync()).ShouldAllBe(x => x.Binary);

        // Already in bdata, so not even read
        var second = await binary.Advanced.RewriteStoredEventsAsync(x =>
            x.IncludeType<PersonMoved>().OnlyEventsStoredAsJson(), Ct);
        second.Databases.Single().Visited.ShouldBe(0);
        second.Databases.Single().Rewritten.ShouldBe(0);

        // What the filter saves: without it, every row comes out different and is written again
        var withoutFilter = await binary.Advanced.RewriteStoredEventsAsync(x => x.IncludeType<PersonMoved>(), Ct);
        withoutFilter.Databases.Single().Rewritten.ShouldBe(2);

        await using var query = binary.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Select(x => x.Data).ShouldBe([new PersonMoved("Utrecht"), new PersonMoved("Leiden")]);
    }

    [Fact]
    public async Task one_tenant_of_a_conjoined_store_is_rewritten_alone()
    {
        withUpcaster(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        foreach (var tenant in new[] { "red", "blue" })
        {
            await using var session = theStore.LightweightSession(tenant);
            session.Events.StartStream(Guid.NewGuid(), new PersonRegisteredV1($"{tenant} 1"),
                new PersonRegisteredV1($"{tenant} 2"));
            await session.SaveChangesAsync(Ct);
        }

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<PersonRegistered>();
            x.ForTenant("blue");
            x.BatchSize = 1;
        }, Ct);

        result.Databases.Single().Visited.ShouldBe(2);
        result.Databases.Single().Rewritten.ShouldBe(2);

        var rows = await storedRowsAsync();
        rows.Where(x => x.Tenant == "blue").ShouldAllBe(x => x.Type == aliasOf<PersonRegistered>());
        rows.Where(x => x.Tenant == "red").ShouldAllBe(x => x.Type == aliasOf<PersonRegisteredV1>());
    }

    [Fact]
    public async Task one_database_is_chosen_by_its_identifier()
    {
        withUpcaster();
        await appendAsync(new PersonRegisteredV1("Alice"));

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x => x
            .IncludeType<PersonRegistered>()
            .ForDatabase(theStore.Database.Identifier), Ct);

        result.Databases.Single().Cursor.Database.ShouldBe(theStore.Database.Identifier);
        result.Databases.Single().Rewritten.ShouldBe(1);

        await Should.ThrowAsync<ArgumentOutOfRangeException>(() =>
            theStore.Advanced.RewriteStoredEventsAsync(x => x.IncludeType<PersonRegistered>().ForDatabase("nowhere"),
                Ct));
    }

    [Fact]
    public async Task events_of_a_type_the_application_no_longer_knows_are_left_alone()
    {
        withUpcaster();
        await appendAsync(new PersonRegisteredV1("Alice"), new PersonMoved("Utrecht"), new PersonRegisteredV1("Bob"));

        // The second event's type has since been deleted from the code
        await using (var conn = new SqlConnection(ConnectionSource.ConnectionString))
        {
            await conn.OpenAsync(Ct);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"UPDATE {theStore.Options.EventGraph.EventsTableName} SET type = 'retired_event', dotnet_type = 'Nowhere.Retired, Nowhere' WHERE type = @moved";
            cmd.Parameters.AddWithValue("@moved", aliasOf<PersonMoved>());
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        // Two per page, so the unknown event is the last row of the first page
        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeAllTypes();
            x.BatchSize = 2;
        }, Ct);

        var database = result.Databases.Single();
        database.Visited.ShouldBe(3);
        database.UnknownTypes.ShouldBe(1);
        database.Rewritten.ShouldBe(2);
        database.Completed.ShouldBeTrue();

        (await storedRowsAsync()).Select(x => x.Type)
            .ShouldBe([aliasOf<PersonRegistered>(), "retired_event", aliasOf<PersonRegistered>()]);
    }

    [Fact]
    public async Task cancelling_stops_between_pages_and_the_last_reported_cursor_resumes()
    {
        withUpcaster();
        for (var i = 0; i < 5; i++) await appendAsync(new PersonRegisteredV1($"Person {i}"));

        using var cancellation = new CancellationTokenSource();
        var progress = new List<StoredEventRewriteProgress>();
        var cancelled = false;

        try
        {
            await theStore.Advanced.RewriteStoredEventsAsync(x =>
            {
                x.IncludeType<PersonRegistered>();
                x.BatchSize = 2;
                x.OnProgress = (p, _) =>
                {
                    progress.Add(p);
                    cancellation.Cancel();
                    return Task.CompletedTask;
                };
            }, cancellation.Token);
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
        }

        cancelled.ShouldBeTrue();
        progress.Single().Visited.ShouldBe(2);
        progress.Single().Completed.ShouldBeFalse();
        (await storedRowsAsync()).Count(x => x.Type == aliasOf<PersonRegistered>()).ShouldBe(2);

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x => x
            .IncludeType<PersonRegistered>()
            .ResumeFrom(progress.Single().Cursor), Ct);

        result.Databases.Single().Visited.ShouldBe(3);
        (await storedRowsAsync()).ShouldAllBe(x => x.Type == aliasOf<PersonRegistered>());
    }

    [Fact]
    public async Task a_transform_that_returns_null_is_refused()
    {
        withUpcaster();
        await appendAsync(new PersonMoved("Utrecht"));

        await Should.ThrowAsync<InvalidOperationException>(() =>
            theStore.Advanced.RewriteStoredEventsAsync(x => x.Transform<PersonMoved>(_ => null!), Ct));

        (await storedRowsAsync()).Single().Type.ShouldBe(aliasOf<PersonMoved>());
    }
}
