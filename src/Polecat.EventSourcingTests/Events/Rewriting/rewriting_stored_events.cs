using JasperFx;
using Microsoft.Data.SqlClient;
using Polecat.Events.Rewriting;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events.Rewriting;

/// <summary>
///     #1034, the port of marten#5643: rewrite stored events the way an append would store them today.
/// </summary>
public class rewriting_stored_events : OneOffConfigurationsContext
{
    public record PersonRegisteredV1(string Name);

    public record PersonRegistered(string FullName);

    public record PersonMoved(string City);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void withUpcaster(Action<StoreOptions>? configure = null) => ConfigureStore(opts =>
    {
        opts.Events.Upcast<PersonRegisteredV1, PersonRegistered>(x => new PersonRegistered(x.Name));
        opts.Events.AddEventType<PersonMoved>();
        configure?.Invoke(opts);
    });

    private string aliasOf<T>() => theStore.Options.EventGraph.EventMappingFor(typeof(T)).EventTypeName;

    private async Task<Guid> registerAsync(object first, params object[] rest)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var streamId = Guid.NewGuid();
        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamId, [first, .. rest]);
        await session.SaveChangesAsync(Ct);
        return streamId;
    }

    // Everything a rewrite could change, so an untouched row compares equal
    private async Task<List<(string Type, string Stored)>> storedRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT type, CONCAT(dotnet_type, '|', CAST(data AS nvarchar(max)), '|', CONVERT(nvarchar(max), bdata, 2)) FROM {theStore.Options.EventGraph.EventsTableName} ORDER BY seq_id";
        await using var reader = await cmd.ExecuteReaderAsync(Ct);

        var rows = new List<(string, string)>();
        while (await reader.ReadAsync(Ct)) rows.Add((reader.GetString(0), reader.GetString(1)));
        return rows;
    }

    [Fact]
    public async Task once_rewritten_the_upcaster_can_be_retired()
    {
        withUpcaster();
        var streamId = await registerAsync(new PersonRegisteredV1("Alice"));

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x => x.IncludeType<PersonRegistered>(), Ct);

        result.Databases.Single().Rewritten.ShouldBe(1);
        (await storedRowsAsync()).Single().Type.ShouldBe(aliasOf<PersonRegistered>());

        var options = new StoreOptions
        {
            ConnectionString = theStore.Options.ConnectionString,
            DatabaseSchemaName = theStore.Options.DatabaseSchemaName,
            AutoCreateSchemaObjects = AutoCreate.None,
            UseNativeJsonType = theStore.Options.UseNativeJsonType
        };
        options.Events.AddEventType<PersonRegistered>();
        using var withoutUpcaster = new DocumentStore(options);

        await using var query = withoutUpcaster.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Single().Data.ShouldBe(new PersonRegistered("Alice"));
    }

    [Fact]
    public async Task a_dry_run_reports_what_would_change_and_writes_nothing()
    {
        withUpcaster();
        await registerAsync(new PersonRegisteredV1("Alice"), new PersonMoved("Utrecht"));
        var before = await storedRowsAsync();

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<PersonRegistered>();
            x.DryRun = true;
        }, Ct);

        var database = result.Databases.Single();
        database.Visited.ShouldBe(1);
        database.Rewritten.ShouldBe(1);
        database.BytesBefore.ShouldBeGreaterThan(0);
        database.BytesAfter.ShouldBeGreaterThan(0);
        (await storedRowsAsync()).ShouldBe(before);
    }

    [Fact]
    public async Task events_already_stored_in_their_current_form_are_not_written()
    {
        withUpcaster();
        // Characters System.Text.Json escapes and SQL Server's json type stores unescaped, so a comparison that
        // did not go through the column's own type would see every row as changed
        await registerAsync(new PersonRegistered("Alice <O'Neill> & co"), new PersonMoved("Utrecht"));
        var before = await storedRowsAsync();

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x => x.IncludeAllTypes(), Ct);

        result.Databases.Single().Visited.ShouldBe(2);
        result.Databases.Single().Rewritten.ShouldBe(0);
        (await storedRowsAsync()).ShouldBe(before);
    }

    [Fact]
    public async Task a_transform_corrects_the_stored_events()
    {
        withUpcaster();
        var streamId = await registerAsync(new PersonRegistered("Alice"), new PersonMoved("utrecht"));

        // Only the case changes, which a case-insensitive comparison would call unchanged
        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
            x.Transform<PersonMoved>(e => e with { City = e.City.ToUpperInvariant() }), Ct);

        result.Databases.Single().Rewritten.ShouldBe(1);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Select(e => e.Data).ShouldBe([new PersonRegistered("Alice"), new PersonMoved("UTRECHT")]);
    }

    [Fact]
    public async Task other_event_types_are_left_alone()
    {
        withUpcaster();
        await registerAsync(new PersonRegisteredV1("Alice"), new PersonMoved("Utrecht"));
        var moved = (await storedRowsAsync())[1];

        await theStore.Advanced.RewriteStoredEventsAsync(x => x.IncludeType<PersonRegistered>(), Ct);

        (await storedRowsAsync())[1].ShouldBe(moved);
    }

    [Fact]
    public async Task it_runs_in_pages_and_resumes_from_a_reported_cursor()
    {
        withUpcaster();
        for (var i = 0; i < 5; i++) await registerAsync(new PersonRegisteredV1($"Person {i}"));

        var progress = new List<StoredEventRewriteProgress>();
        await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<PersonRegistered>();
            x.BatchSize = 2;
            x.OnProgress = (p, _) =>
            {
                progress.Add(p);
                // Stop after the first page, as a crash would
                throw new InvalidOperationException("stopped");
            };
        }, Ct).ShouldThrowAsync<InvalidOperationException>();

        progress.Single().Visited.ShouldBe(2);
        (await storedRowsAsync()).Count(r => r.Type == aliasOf<PersonRegistered>()).ShouldBe(2);

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x =>
        {
            x.IncludeType<PersonRegistered>();
            x.BatchSize = 2;
            x.ResumeFrom(progress.Single().Cursor);
        }, Ct);

        result.Databases.Single().Visited.ShouldBe(3);
        result.Databases.Single().Completed.ShouldBeTrue();
        (await storedRowsAsync()).ShouldAllBe(r => r.Type == aliasOf<PersonRegistered>());
    }

    [Fact]
    public async Task an_event_changed_between_the_read_and_the_write_is_skipped_not_overwritten()
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<PersonMoved>();
            opts.Events.AddMaskingRuleForProtectedInformation<PersonMoved>(x => x with { City = "****" });
        });
        var streamId = await registerAsync(new PersonMoved("utrecht"));

        var result = await theStore.Advanced.RewriteStoredEventsAsync(x => x.Transform<PersonMoved>(e =>
        {
            // Runs after the page was read: masking it now is a concurrent change
            theStore.Advanced.ApplyEventDataMasking(m => m.IncludeStream(streamId)).GetAwaiter().GetResult();
            return e with { City = e.City.ToUpperInvariant() };
        }), Ct);

        result.Databases.Single().Rewritten.ShouldBe(0);
        result.Databases.Single().Skipped.ShouldBe(1);

        await using var query = theStore.QuerySession();
        var events = await query.Events.FetchStreamAsync(streamId, token: Ct);
        events.Single().Data.ShouldBe(new PersonMoved("****"));
    }

    [Fact]
    public async Task it_refuses_to_run_without_naming_the_events()
    {
        withUpcaster();

        await Should.ThrowAsync<InvalidOperationException>(() =>
            theStore.Advanced.RewriteStoredEventsAsync(_ => { }, Ct));
    }
}
