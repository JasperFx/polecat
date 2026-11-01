using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Projections;
using Polecat.TestUtils;

namespace Polecat.Tests.Daemon;

/// <summary>
///     #834 on the daemon's side. A projection's raised events reach
///     <c>PolecatProjectionBatch</c> through the same quick append a session uses, so one slice raising
///     past the limit failed its whole shard. The split operations arrive one to one as commands in the
///     batch that also carries the projection's documents and its progression row.
/// </summary>
public partial class raised_events_past_the_parameter_limit_tests : IAsyncLifetime
{
    private const string Schema = "gh834_raised";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public record BulkRaiseRequested(string Label, int Count);

    public record BulkRaised(string Label, int Position);

    public class BulkSnap
    {
        public Guid Id { get; set; }
        public int Requests { get; set; }

        public void Apply(BulkRaiseRequested _) => Requests++;
    }

    public partial class BulkRaisingProjection : SingleStreamProjection<BulkSnap, Guid>
    {
        public override ValueTask RaiseSideEffects(IDocumentSession session, IEventSlice<BulkSnap> slice)
        {
            foreach (var e in slice.Events().Where(x => x.Data is BulkRaiseRequested))
            {
                var requested = (BulkRaiseRequested)e.Data;
                for (var i = 0; i < requested.Count; i++)
                {
                    slice.AppendEvent(AuditStream, new BulkRaised(requested.Label, i));
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    private static readonly Guid AuditStream = Guid.NewGuid();

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            IF SCHEMA_ID('{Schema}') IS NOT NULL
            BEGIN
                DECLARE @fks nvarchar(max) = N'';
                SELECT @fks = @fks + 'ALTER TABLE [{Schema}].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
                FROM sys.foreign_keys fk
                JOIN sys.tables t ON fk.parent_object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{Schema}';
                IF @fks <> N'' EXEC sp_executesql @fks;

                DECLARE @tables nvarchar(max) = N'';
                SELECT @tables = @tables + 'DROP TABLE [{Schema}].[' + t.name + '];'
                FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{Schema}';
                IF @tables <> N'' EXEC sp_executesql @tables;
            END
            """;
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.Projections.Add(new BulkRaisingProjection(), ProjectionLifecycle.Async);
        });
    }

    [Fact]
    public async Task a_projection_raises_past_the_limit_in_one_batch()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new BulkRaiseRequested("first", 3000));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var audit = await query.Events.FetchStreamAsync(AuditStream, token: Ct);

        audit.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 3000).Select(x => (long)x));
        audit.Select(x => ((BulkRaised)x.Data).Position).ShouldBe(Enumerable.Range(0, 3000));
        audit.Select(x => x.Sequence).ShouldBeInOrder();

        var state = await query.Events.FetchStreamStateAsync(AuditStream, Ct);
        state!.Version.ShouldBe(3000);
    }

    /// <summary>The second batch updates the stream row the first one inserted.</summary>
    [Fact]
    public async Task a_later_batch_continues_the_stream_the_first_one_started()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: Ct);

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new BulkRaiseRequested("first", 1000));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(Guid.NewGuid(), new BulkRaiseRequested("second", 1000));
            await session.SaveChangesAsync(Ct);
        }

        await store.WaitForProjectionAsync();

        await using var query = store.QuerySession();
        var audit = await query.Events.FetchStreamAsync(AuditStream, token: Ct);

        audit.Select(x => x.Version).ShouldBe(Enumerable.Range(1, 2000).Select(x => (long)x));
        audit.Skip(1000).ShouldAllBe(x => ((BulkRaised)x.Data).Label == "second");
    }
}
