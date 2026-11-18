using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Protected;
using Microsoft.Data.SqlClient;
using Polecat.Events.Protected;
using Polecat.Tests.Harness;
using Polecat.Tests.Projections;
using Polecat.TestUtils;
using Weasel.SqlServer;

namespace Polecat.Tests.Events;

/// <summary>
///     #834. <c>DeleteEventsOperation</c> bound one parameter per folded event, so compaction failed
///     with "too many parameters" on any stream past about 2100 events, and those are the streams
///     compaction exists for. The class of #710, and the same answer.
/// </summary>
public class compacting_past_the_parameter_limit_tests : OneOffConfigurationsContext
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static object[] Quest(int count)
    {
        var events = new object[count];
        events[0] = new QuestStarted("Long Quest");
        for (var i = 1; i < count; i++)
        {
            events[i] = new MonsterSlain($"monster-{i}", i);
        }

        return events;
    }

    private async Task<int> EventRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"select count(*) from [{theStore.Options.DatabaseSchemaName}].[pc_events]";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    /// <summary>The issue's reproduction: a stream that grew through ordinary small saves.</summary>
    [Fact]
    public async Task a_stream_grown_in_small_saves_compacts()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, new QuestStarted("Long Quest"));
            await session.SaveChangesAsync(Ct);
        }

        for (var chunk = 0; chunk < 25; chunk++)
        {
            await using var session = theStore.LightweightSession();
            session.Events.Append(id,
                Enumerable.Range(0, 100).Select(i => (object)new MonsterSlain($"monster-{chunk}-{i}", i)).ToArray());
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<QuestParty>(id);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var compacted = (await query.Events.FetchStreamAsync(id, token: Ct)).Single();
        compacted.Version.ShouldBe(2501);
        compacted.Data.ShouldBeOfType<Compacted<QuestParty>>().Snapshot.MonstersSlain.Count.ShouldBe(2500);
    }

    [Theory]
    [InlineData(99)]
    [InlineData(2100)]
    [InlineData(5500)]
    public async Task a_stream_of_any_length_compacts_to_one_event(int count)
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, Quest(count));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<QuestParty>(id);
            await session.SaveChangesAsync(Ct);
        }

        (await EventRowsAsync()).ShouldBe(1);

        await using var query = theStore.QuerySession();
        var compacted = (await query.Events.FetchStreamAsync(id, token: Ct)).Single();
        compacted.Version.ShouldBe(count);
        compacted.Data.ShouldBeOfType<Compacted<QuestParty>>().Snapshot.MonstersSlain.Count.ShouldBe(count - 1);

        var state = await query.Events.FetchStreamStateAsync(id, Ct);
        state!.Version.ShouldBe(count);
    }

    [Fact]
    public async Task a_string_keyed_stream_compacts()
    {
        ConfigureStore(opts => opts.Events.StreamIdentity = StreamIdentity.AsString);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var key = Guid.NewGuid().ToString();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(key, Quest(2200));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<QuestParty>(key);
            await session.SaveChangesAsync(Ct);
        }

        (await EventRowsAsync()).ShouldBe(1);
    }

    [Fact]
    public async Task a_partial_compaction_keeps_what_comes_after_its_cutoff()
    {
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(id, Quest(3000));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<QuestParty>(id, x => x.Version = 2500);
            await session.SaveChangesAsync(Ct);
        }

        await using var query = theStore.QuerySession();
        var remaining = await query.Events.FetchStreamAsync(id, token: Ct);

        remaining.Count.ShouldBe(501);
        remaining[0].Version.ShouldBe(2500);
        remaining[0].Data.ShouldBeOfType<Compacted<QuestParty>>();
        remaining.Skip(1).Select(x => x.Version).ShouldBe(Enumerable.Range(2501, 500).Select(x => (long)x));
    }

    [Fact]
    public async Task conjoined_tenancy_keeps_its_filter_on_a_long_delete()
    {
        ConfigureStore(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        var id = Guid.NewGuid();
        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            session.Events.StartStream(id, Quest(2200));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "beta" }))
        {
            session.Events.StartStream(id, Quest(10));
            await session.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "alpha" }))
        {
            await session.Events.CompactStreamAsync<QuestParty>(id);
            await session.SaveChangesAsync(Ct);
        }

        (await EventRowsAsync()).ShouldBe(11);
    }

    // ---- the rendered shape ---------------------------------------------------------------------

    private (string Sql, int Parameters) Render(long[] sequences, string tenantId = "alpha")
    {
        var operation = new DeleteEventsOperation(theStore.Options.EventGraph, sequences, tenantId);

        var builder = new BatchBuilder();
        operation.ConfigureCommand(builder);

        var command = builder.Compile().BatchCommands[0];
        return (command.CommandText, command.Parameters.Count);
    }

    [Fact]
    public void a_short_list_keeps_its_literal_in_list()
    {
        // Unchanged on purpose: a short IN gives the optimizer real values and a real row count.
        var (sql, parameters) = Render([1, 2, 3]);

        sql.ShouldContain("seq_id IN (@");
        sql.ShouldNotContain("OPENJSON");
        parameters.ShouldBe(3);
    }

    [Fact]
    public void a_long_list_is_one_parameter_typed_to_the_column()
    {
        var (sql, parameters) = Render(Enumerable.Range(1, 5000).Select(x => (long)x).ToArray());

        sql.ShouldContain("seq_id IN (SELECT [value] FROM OPENJSON(@");
        sql.ShouldContain("WITH ([value] bigint '$')");
        parameters.ShouldBe(1);
    }

    [Fact]
    public void a_long_list_still_carries_the_conjoined_tenant_filter()
    {
        ConfigureStore(opts => opts.Events.TenancyStyle = TenancyStyle.Conjoined);

        var (sql, parameters) = Render(Enumerable.Range(1, 5000).Select(x => (long)x).ToArray());

        sql.ShouldContain("AND tenant_id = @");
        parameters.ShouldBe(2);
    }
}
