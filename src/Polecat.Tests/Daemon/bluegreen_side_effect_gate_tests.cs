using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Events.Aggregation;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #326 (jasperfx#480) — the headline blue/green scenario (CritterWatch#77). When a new projection
///     version opts into <c>GateSideEffectsBehindPriorVersion</c> and starts behind the prior version's
///     persisted progression mark <c>N</c>, the daemon replays to <c>N</c> in Rebuild mode with side
///     effects SUPPRESSED, then hands off to Continuous from <c>N</c> — so message side effects fire
///     only for events the previous version never processed.
///
///     This drives the shared JasperFxAsyncDaemon warm-up against real SQL Server through Polecat's
///     PolecatProjectionBatch → IMessageOutbox message-publication path.
/// </summary>
public partial class bluegreen_side_effect_gate_tests : IAsyncLifetime
{
    private const string Schema = "bluegreen_gate";

    public async Task InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public Task DisposeAsync() => Task.CompletedTask;

    private static DocumentStore CreateStore(uint version, GateOutbox outbox, bool gate)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.MessageOutbox = outbox;

            var projection = new GateSnapProjection { Version = version };
            projection.Options.GateSideEffectsBehindPriorVersion = gate;
            opts.Projections.Add(projection, ProjectionLifecycle.Async);
        });
    }

    // Append K single-event streams, returning their labels in order.
    private static async Task<List<string>> AppendStreamsAsync(DocumentStore store, string prefix, int count)
    {
        var labels = new List<string>();
        for (var i = 0; i < count; i++)
        {
            var label = $"{prefix}-{i}";
            labels.Add(label);
            await using var session = store.LightweightSession();
            session.Events.StartStream(Guid.NewGuid(), new GateStarted(label));
            await session.SaveChangesAsync();
        }

        return labels;
    }

    [Fact]
    public async Task new_version_suppresses_side_effects_for_events_the_prior_version_already_processed()
    {
        // --- V2: run to N with side effects firing normally ---
        var v2Outbox = new GateOutbox();
        using (var v2 = CreateStore(version: 2, v2Outbox, gate: false))
        {
            await v2.Database.ApplyAllConfiguredChangesToDatabaseAsync();
            var priorLabels = await AppendStreamsAsync(v2, "prior", 3);

            await v2.WaitForProjectionAsync();

            // V2 processed all 3 prior streams and fired their side effects.
            v2Outbox.Labels.OrderBy(x => x).ShouldBe(priorLabels.OrderBy(x => x));
        }

        // --- V3: gate on. Append M new events, then start. The warm-up replays [0..N] suppressed,
        //     then continuous fires only for the new events. ---
        var v3Outbox = new GateOutbox();
        using (var v3 = CreateStore(version: 3, v3Outbox, gate: true))
        {
            var newLabels = await AppendStreamsAsync(v3, "new", 2);

            await v3.WaitForProjectionAsync();

            // Headline assertion: V3 fired side effects ONLY for the 2 new events — the 3 prior events
            // were replayed with side effects suppressed during the blue/green warm-up.
            v3Outbox.Labels.OrderBy(x => x).ShouldBe(newLabels.OrderBy(x => x));
            foreach (var prior in new[] { "prior-0", "prior-1", "prior-2" })
            {
                v3Outbox.Labels.ShouldNotContain(prior);
            }

            // And V3's projected state is correct over the FULL history (all 5 aggregates exist).
            await using var query = v3.QuerySession();
            var all = await query.Query<GateSnap>().ToListAsync();
            all.Count.ShouldBe(5);
        }
    }

    private static async Task DropSchemaTablesAsync(string schema)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            DECLARE @sql nvarchar(max) = N'';
            SELECT @sql = @sql + 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
            FROM sys.foreign_keys fk
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;

            SELECT @sql = @sql + 'DROP TABLE [' + s.name + '].[' + t.name + '];'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            EXEC sp_executesql @sql;
            """;
        cmd.Parameters.AddWithValue("@schema", schema);
        await cmd.ExecuteNonQueryAsync();
    }

    public record GateStarted(string Label);

    public record GateNotice(string Label);

    public partial class GateSnap
    {
        public Guid Id { get; set; }
        public string Label { get; set; } = "";

        public void Apply(GateStarted e) => Label = e.Label;
    }

    public partial class GateSnapProjection : SingleStreamProjection<GateSnap, Guid>
    {
        public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<GateSnap> slice)
        {
            if (slice.Snapshot is not null)
            {
                slice.PublishMessage(new GateNotice(slice.Snapshot.Label));
            }

            return ValueTask.CompletedTask;
        }
    }

    // Captures every message published through the async daemon's IMessageOutbox path.
    private sealed class GateOutbox : IMessageOutbox
    {
        private readonly GateBatch _batch = new();
        public List<string> Labels => _batch.Labels;

        public ValueTask<IMessageBatch> CreateBatch(IDocumentSession session) =>
            new(_batch);

        private sealed class GateBatch : IMessageBatch
        {
            public List<string> Labels { get; } = new();

            public ValueTask PublishAsync<T>(T message, string tenantId)
            {
                if (message is GateNotice notice)
                {
                    lock (Labels) Labels.Add(notice.Label);
                }

                return ValueTask.CompletedTask;
            }

            public Task BeforeCommitAsync(CancellationToken token) => Task.CompletedTask;
            public Task AfterCommitAsync(CancellationToken token) => Task.CompletedTask;
        }
    }
}
