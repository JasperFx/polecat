using System.Text;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// The event as it was originally written, and as rows in pc_events still record it.
public record StayBooked(Guid RoomId, string Name, string? Notes);

// The event this deployment reads. Every member but RoomId has been renamed, which is what makes
// the bug visible: an upcast that renames nothing happens to survive being run twice.
public record StayReserved(Guid RoomId, string Guest, string? Remark);

// Never upcast, and masked: the control for "only the rows that needed it changed identity".
public record StayNoted(string Guest);

// The "same name, older JSON schema" case: the legacy shape and the current one share a stored name.
public static class LegacyStayShape
{
    public record StayReviewed(Guid RoomId, string Name);
}

public record StayReviewed(Guid RoomId, string Reviewer);

public class StayLedger
{
    public Guid Id { get; set; }
    public string? Guest { get; set; }
    public string? Remark { get; set; }
}

public partial class StayLedgerProjection : SingleStreamProjection<StayLedger, Guid>
{
    public void Apply(StayLedger ledger, StayReserved e)
    {
        ledger.Guest = e.Guest;
        ledger.Remark = e.Remark;
    }
}

public class StayFolio
{
    public Guid Id { get; set; }
    public string? Guest { get; set; }
    public string? Remark { get; set; }

    public void Apply(StayReserved e)
    {
        Guest = e.Guest;
        Remark = e.Remark;
    }
}

/// <summary>
///     polecat#843, the twin of marten#5642's upcast half. Masking reads an upcast event as the
///     upcaster's target type and writes that shape back, so the row has to say it IS that type: left
///     under its old name, the next read upcasts the new shape a second time and every renamed member
///     comes back empty.
/// </summary>
/// <remarks>
///     <para>
///         Rows are written the way the pre-migration application really wrote them -- through a store
///         with no upcasters -- and only then handed to a store that has them. See
///         <see cref="ALegacyStayAsync" />.
///     </para>
///     <para>
///         <b>A row keeps its identity only until it is rewritten.</b> The alternative to rewriting
///         <c>type</c> and <c>dotnet_type</c> was serializing the mask back to the stored shape, which
///         cannot work: an upcast is one-way, and with a raw-JSON registration the old CLR type may no
///         longer exist. Both columns move together because they answer different questions --
///         <c>type</c> decides whether a read upcasts at all, <c>dotnet_type</c> decides the CLR type
///         everywhere else and is the daemon's allow list.
///         <see cref="the_daemon_admits_the_masked_row_through_its_allow_list" /> is the one that
///         fails if only <c>type</c> moves.
///     </para>
/// </remarks>
public class masking_upcast_events_tests : OneOffConfigurationsContext
{
    private const string TheOldClrTypeIsGone = "Polecat.Tests.Gone.StayBooked, Polecat.Tests.Gone";

    private static readonly string _sourceName = EventTypeExtensions.GetEventTypeName<StayBooked>();
    private static readonly string _targetName = EventTypeExtensions.GetEventTypeName<StayReserved>();

    private static readonly Guid _roomId = Guid.NewGuid();

    private readonly TestBinaryEventSerializer _serializer = new();

    private async Task<Guid> ALegacyStayAsync(params object[] more)
    {
        ConfigureStore(opts =>
        {
            opts.Events.AddEventType<StayBooked>();
            opts.Events.AddEventType<StayNoted>();
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();

        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId,
                [new StayBooked(_roomId, "Hilda", "Late arrival"), .. more]);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        return streamId;
    }

    private async Task AStoreWithTheUpcasterAsync(Action<StoreOptions>? extra = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.Upcast<StayBooked, StayReserved>(old => new StayReserved(old.RoomId, old.Name, old.Notes));
            opts.Events.AddMaskingRuleForProtectedInformation<StayReserved>(e => e with { Guest = "****" });
            opts.Events.AddMaskingRuleForProtectedInformation<StayNoted>(e => e with { Guest = "****" });
            extra?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
    }

    // Reads the new shape strictly, exactly as the documented raw-JSON example does: GetProperty
    // throws on a member that is not there.
    private async Task AStoreWithARawJsonUpcasterAsync(Action<StoreOptions>? extra = null)
    {
        ConfigureStore(opts =>
        {
            opts.Events.Upcast<StayReserved>(_sourceName, document =>
            {
                var root = document.RootElement;
                return new StayReserved(
                    root.GetProperty("roomId").GetGuid(),
                    root.GetProperty("name").GetString()!,
                    root.GetProperty("notes").GetString());
            });
            opts.Events.AddMaskingRuleForProtectedInformation<StayReserved>(e => e with { Guest = "****" });
            extra?.Invoke(opts);
        });

        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);
    }

    private Task MaskAsync(Guid streamId) => theStore.Advanced.ApplyEventDataMasking(
        x => x.IncludeStream(streamId), TestContext.Current.CancellationToken);

    private async Task<IReadOnlyList<IEvent>> ReadAsync(Guid streamId)
    {
        await using var query = theStore.QuerySession();
        return await query.Events.FetchStreamAsync(streamId, token: TestContext.Current.CancellationToken);
    }

    private async Task<(string Type, string? DotNetType, string Data, byte[]? Bdata)> ReadRowAsync(long sequence)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT type, dotnet_type, CONVERT(nvarchar(max), data), bdata FROM {theStore.Options.EventGraph.EventsTableName} WHERE seq_id = @seq";
        cmd.Parameters.AddWithValue("@seq", sequence);

        await using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();

        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2),
            reader.IsDBNull(3) ? null : (byte[])reader.GetValue(3));
    }

    private async Task ExecuteAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters) cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task TheOldClrTypeIsDeletedAsync(Guid streamId) => ExecuteAsync(
        $"UPDATE {theStore.Options.EventGraph.EventsTableName} SET dotnet_type = @gone WHERE stream_id = @stream",
        ("@gone", TheOldClrTypeIsGone), ("@stream", streamId));

    /// <summary>
    ///     What a masking pass left behind before the fix: the new shape, masked, under the old name.
    ///     Only <c>data</c> changed, because only <c>data</c> was ever written.
    /// </summary>
    private async Task<long> TheRowWasDamagedByAnEarlierMaskingPassAsync(Guid streamId)
    {
        var sequence = (await ReadAsync(streamId))[0].Sequence;

        await ExecuteAsync(
            $"UPDATE {theStore.Options.EventGraph.EventsTableName} SET data = @data WHERE seq_id = @seq",
            ("@data", theStore.Options.Serializer.ToJson(new StayReserved(_roomId, "****", "Late arrival"))),
            ("@seq", sequence));

        return sequence;
    }

    [Fact]
    public async Task a_masked_renamed_member_stays_masked_and_the_other_renamed_members_survive()
    {
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();

        await MaskAsync(streamId);

        var stay = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBe("Late arrival");
        stay.RoomId.ShouldBe(_roomId);
    }

    [Fact]
    public async Task the_masked_row_is_stored_the_way_an_append_of_that_type_would_be()
    {
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();

        await MaskAsync(streamId);

        var @event = (await ReadAsync(streamId)).Single();
        var mapping = theStore.Options.EventGraph.EventMappingFor(typeof(StayReserved));

        var row = await ReadRowAsync(@event.Sequence);
        row.Type.ShouldBe(_targetName);
        row.DotNetType.ShouldBe(mapping.DotNetTypeName);
        row.Data.ShouldNotContain("Hilda");

        // The name a read reports is a fact about the row, and the row has changed.
        @event.EventTypeName.ShouldBe(_targetName);
    }

    [Fact]
    public async Task only_the_rows_that_were_masked_change_identity()
    {
        var streamId = await ALegacyStayAsync(
            new StayBooked(_roomId, "Bertha", "Early check-in"),
            new StayNoted("Hilda"));
        await AStoreWithTheUpcasterAsync();

        var sourceDotNetType = (await ReadRowAsync((await ReadAsync(streamId))[1].Sequence)).DotNetType;

        await theStore.Advanced.ApplyEventDataMasking(x => x.IncludeStream(streamId, e => e.Version != 2),
            TestContext.Current.CancellationToken);

        var events = await ReadAsync(streamId);

        (await ReadRowAsync(events[0].Sequence)).Type.ShouldBe(_targetName);
        events[0].Data.ShouldBeOfType<StayReserved>().Guest.ShouldBe("****");

        // Filtered out, so still a legacy row, still read through the upcaster.
        var untouched = await ReadRowAsync(events[1].Sequence);
        untouched.Type.ShouldBe(_sourceName);
        untouched.DotNetType.ShouldBe(sourceDotNetType);
        events[1].Data.ShouldBeOfType<StayReserved>().Guest.ShouldBe("Bertha");

        // Masked, but never upcast: it already was what it says it is.
        var native = await ReadRowAsync(events[2].Sequence);
        native.Type.ShouldBe(EventTypeExtensions.GetEventTypeName<StayNoted>());
        native.DotNetType.ShouldBe(theStore.Options.EventGraph.EventMappingFor(typeof(StayNoted)).DotNetTypeName);
        events[2].Data.ShouldBeOfType<StayNoted>().Guest.ShouldBe("****");
    }

    [Fact]
    public async Task masking_twice_is_the_same_as_masking_once()
    {
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();

        await MaskAsync(streamId);
        await MaskAsync(streamId);

        var stay = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBe("Late arrival");
    }

    [Fact]
    public async Task a_strict_raw_json_upcaster_can_still_read_the_stream()
    {
        // The worst form of the bug, and the one the documented raw-JSON example walks straight into:
        // GetProperty throws on the new shape, so the masked stream could not be read at all.
        var streamId = await ALegacyStayAsync();
        await AStoreWithARawJsonUpcasterAsync();

        await MaskAsync(streamId);

        var stay = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBe("Late arrival");
    }

    [Fact]
    public async Task a_row_masked_while_the_old_clr_type_is_gone_reads_back_masked()
    {
        // Why the mask cannot go back to the stored shape: there is nothing left to serialize it as.
        var streamId = await ALegacyStayAsync();
        await AStoreWithARawJsonUpcasterAsync();
        await TheOldClrTypeIsDeletedAsync(streamId);

        await MaskAsync(streamId);

        var @event = (await ReadAsync(streamId)).Single();
        @event.Data.ShouldBeOfType<StayReserved>().Guest.ShouldBe("****");
        (await ReadRowAsync(@event.Sequence)).DotNetType.ShouldNotBe(TheOldClrTypeIsGone);
    }

    [Fact]
    public async Task the_daemon_admits_the_masked_row_through_its_allow_list()
    {
        // The projection declares only the NEW type, so the loader's allow list is StayReserved's
        // dotnet_type alone. A masked row that moved `type` but not `dotnet_type` is no longer an
        // upcast source and is not on the list, so it would be dropped in SQL and this document would
        // never exist.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync(
            opts => opts.Projections.Add<StayLedgerProjection>(ProjectionLifecycle.Async));

        await MaskAsync(streamId);

        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.StartAllAsync();
        await daemon.WaitForNonStaleData(TimeSpan.FromSeconds(30));

        await using var query = theStore.QuerySession();
        var ledger = await query.LoadAsync<StayLedger>(streamId, TestContext.Current.CancellationToken);
        ledger.ShouldNotBeNull();
        ledger!.Guest.ShouldBe("****");
        ledger.Remark.ShouldBe("Late arrival");
    }

    [Fact]
    public async Task compacting_after_masking_folds_the_masked_value_into_the_snapshot()
    {
        // Compaction deletes the rows it folds, so a wrong read here is permanent.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();

        await MaskAsync(streamId);

        await using (var session = theStore.LightweightSession())
        {
            await session.Events.CompactStreamAsync<StayFolio>(streamId);
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var compacted = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<Compacted<StayFolio>>();
        compacted.Snapshot.Guest.ShouldBe("****");
        compacted.Snapshot.Remark.ShouldBe("Late arrival");
    }

    [Fact]
    public async Task readers_keyed_on_the_stored_name_see_the_row_as_its_new_type()
    {
        // QueryRawEventDataOnly<T> bypasses upcasting and filters on the alias, so it is the reader
        // that shows what the row really is.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();

        await MaskAsync(streamId);

        await using var query = theStore.QuerySession();

        var raw = await query.Events.QueryAllRawEvents()
            .Where(e => e.StreamId == streamId)
            .ToListAsync(TestContext.Current.CancellationToken);
        raw.Single().Data.ShouldBeOfType<StayReserved>().Guest.ShouldBe("****");

        var asNewType = await query.Events.QueryRawEventDataOnly<StayReserved>()
            .ToListAsync(TestContext.Current.CancellationToken);
        asNewType.Single().Guest.ShouldBe("****");

        var asOldType = await query.Events.QueryRawEventDataOnly<StayBooked>()
            .ToListAsync(TestContext.Current.CancellationToken);
        asOldType.ShouldBeEmpty();
    }

    [Fact]
    public async Task masking_into_a_binary_target_reads_back_masked()
    {
        // The same root with a different symptom. The target is binary, so masking writes bdata under
        // the source type -- and a non-null bdata switches upcasting off, so the row's source
        // dotnet_type decides the read and the target's bytes have no serializer to go through.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync(opts => opts.Events.UseBinarySerializer<StayReserved>(_serializer));

        await MaskAsync(streamId);

        var @event = (await ReadAsync(streamId)).Single();
        var stay = @event.Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBe("Late arrival");

        var row = await ReadRowAsync(@event.Sequence);
        row.Type.ShouldBe(_targetName);
        row.Data.ShouldBe("{}");
        row.Bdata.ShouldNotBeNull();
        Encoding.UTF8.GetString(row.Bdata!).ShouldNotContain("Hilda");
    }

    [Fact]
    public async Task a_binary_target_does_not_lose_the_event_when_the_old_clr_type_is_gone()
    {
        // Under the old name this row hydrated to nothing: upcasting is off for a binary row, the
        // dotnet_type resolves to no type, and the reader's "unknown type" branch skips the event --
        // so masking made an event silently vanish from its stream.
        var streamId = await ALegacyStayAsync();
        await AStoreWithARawJsonUpcasterAsync(opts => opts.Events.UseBinarySerializer<StayReserved>(_serializer));
        await TheOldClrTypeIsDeletedAsync(streamId);

        await MaskAsync(streamId);

        var @event = (await ReadAsync(streamId)).ShouldHaveSingleItem();
        @event.Data.ShouldBeOfType<StayReserved>().Guest.ShouldBe("****");
    }

    [Fact]
    public async Task the_same_name_form_stores_what_an_append_would_and_relies_on_the_upcaster_reading_it()
    {
        // The limit of this fix, pinned. When the source name IS the target's own alias there is no
        // other name to move the row to, so it stays a source row and the next read upcasts it. That
        // is not a masking problem: every native append of the new shape is in the same position, so a
        // same-name upcaster already has to accept both shapes, and this one does. What masking still
        // owes is a row indistinguishable from such an append, which is the dotnet_type asserted here.
        ConfigureStore(opts => opts.Events.AddEventType<LegacyStayShape.StayReviewed>());
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = theStore.LightweightSession())
        {
            session.Events.StartStream(streamId, new LegacyStayShape.StayReviewed(_roomId, "Hilda"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        ConfigureStore(opts =>
        {
            opts.Events.Upcast<StayReviewed>(document =>
            {
                var root = document.RootElement;
                var name = root.TryGetProperty("reviewer", out var reviewer) ? reviewer : root.GetProperty("name");
                return new StayReviewed(root.GetProperty("roomId").GetGuid(), name.GetString()!);
            });
            opts.Events.AddMaskingRuleForProtectedInformation<StayReviewed>(e => e with { Reviewer = "****" });
        });
        await theDatabase.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await MaskAsync(streamId);

        var @event = (await ReadAsync(streamId)).Single();
        @event.Data.ShouldBeOfType<StayReviewed>().Reviewer.ShouldBe("****");

        var row = await ReadRowAsync(@event.Sequence);
        row.Type.ShouldBe(EventTypeExtensions.GetEventTypeName<StayReviewed>());
        row.DotNetType.ShouldBe(theStore.Options.EventGraph.EventMappingFor(typeof(StayReviewed)).DotNetTypeName);
    }

    [Fact]
    public async Task remasking_a_damaged_row_restores_the_mask_but_destroys_a_renamed_unmasked_member()
    {
        // Pinned because docs/events/upcasting.md says so, and because it is the reason the docs steer
        // an operator to the SQL below instead. The pass reads through the upcaster, which sees the new
        // shape and produces nulls; the rule puts the mask back, and nothing puts Remark back -- so a
        // value that was still sitting in the row is now gone for good.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();
        var sequence = await TheRowWasDamagedByAnEarlierMaskingPassAsync(streamId);

        (await ReadRowAsync(sequence)).Data.ShouldContain("Late arrival");

        await MaskAsync(streamId);

        var stay = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBeNull();

        var row = await ReadRowAsync(sequence);
        row.Type.ShouldBe(_targetName);
        row.Data.ShouldNotContain("Late arrival");
    }

    [Fact]
    public async Task the_documented_sql_repair_recovers_a_damaged_row_without_losing_anything()
    {
        // This one exercises the statement in docs/events/upcasting.md rather than product code, so it
        // passes with or without the fix: the damaged row always held the right payload under the wrong
        // name, which is exactly why moving the name is lossless.
        var streamId = await ALegacyStayAsync();
        await AStoreWithTheUpcasterAsync();
        var sequence = await TheRowWasDamagedByAnEarlierMaskingPassAsync(streamId);

        (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>().Guest.ShouldBeNull();

        await ExecuteAsync(
            $"""
             UPDATE {theStore.Options.EventGraph.EventsTableName}
             SET type = @target, dotnet_type = @dotnet_type
             WHERE type = @source AND JSON_PATH_EXISTS(data, '$.guest') = 1
             """,
            ("@target", _targetName),
            ("@dotnet_type", theStore.Options.EventGraph.EventMappingFor(typeof(StayReserved)).DotNetTypeName),
            ("@source", _sourceName));

        var stay = (await ReadAsync(streamId)).Single().Data.ShouldBeOfType<StayReserved>();
        stay.Guest.ShouldBe("****");
        stay.Remark.ShouldBe("Late arrival");

        (await ReadRowAsync(sequence)).Type.ShouldBe(_targetName);
    }
}
