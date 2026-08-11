using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Projections;
using Microsoft.Data.SqlClient;
using Polecat.Linq;
using Polecat.Projections;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Projections;

/// <summary>
///     polecat#439 (parity with marten#5175): a composite's rebuild tears its members down by reading
///     each member's own <c>PublishedTypes()</c> and <c>Options.CleanUps</c>. A member registered as a
///     raw <see cref="IProjection" /> is wrapped in <c>CompositeIProjectionSource</c>, which carried a
///     fresh, EMPTY <c>AsyncOptions</c> and published nothing — so that member contributed no teardown
///     at all. Its progression row was deleted anyway, so the rebuild restarted from sequence zero and
///     replayed onto a table still holding the previous run's rows: a silent double-count.
///     <para>
///         The composite's own <c>Options</c> were never consulted either, so
///         <c>composite.Options.DeleteViewTypeOnTeardown&lt;T&gt;()</c> was a no-op on the parent.
///     </para>
/// </summary>
public class Bug_439_composite_member_teardown : IAsyncLifetime
{
    private const string Schema = "composite_teardown_439";
    private const string CompositeName = "Teardown439";

    public async ValueTask InitializeAsync() => await DropSchemaTablesAsync(Schema);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.Projections.CompositeProjectionFor(CompositeName, composite =>
            {
                // A regular projection source: its own PublishedTypes() already reached teardown
                // through the composite parent's fan-out, so this is the control.
                composite.Add(new Teardown439ProductProjection());

                // The regression: a raw IProjection wrapped in CompositeIProjectionSource. It declares
                // nothing about its own storage, so the teardown rule is declared at registration —
                // which is what the new Add(IProjection, Action<AsyncOptions>) overload is for.
                composite.Add(new Teardown439MetricProjection(),
                    options => options.DeleteViewTypeOnTeardown<Teardown439Metric>());
            });
        });
    }

    private static PolecatCompositeProjection CompositeFor(DocumentStore store) =>
        store.Options.Projections.All.OfType<PolecatCompositeProjection>().Single(x => x.Name == CompositeName);

    [Fact]
    public async Task every_member_reports_its_own_teardown_rules()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var members = CompositeFor(store).AllProjections();

        // The source-registered member publishes its view type.
        members.SelectMany(x => x.PublishedTypes()).ShouldContain(typeof(Teardown439Product));

        // Before #439 this was absent: the wrapper carried an empty AsyncOptions and there was no
        // overload through which the rule could be declared.
        members.SelectMany(x => x.Options.CleanUps)
            .OfType<DeleteDocuments>()
            .Select(x => x.DocumentType)
            .ShouldContain(typeof(Teardown439Metric));
    }

    [Fact]
    public async Task rebuilding_deletes_every_members_documents_first()
    {
        using var store = CreateStore();
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new Teardown439Registered("Ankle Socks", "Socks"));
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(CompositeName, CancellationToken.None);
        }

        // Orphans: rows of each member's view type that NO event can reproduce. If the rebuild's
        // teardown reaches every member they are gone afterwards; if a member contributes no teardown,
        // its orphan survives — which is the same table state a replay would double-count into.
        var orphanId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Store(new Teardown439Product { Id = orphanId, Name = "Ghost", Category = "Ghost" });
            session.Store(new Teardown439Metric { Id = orphanId, Price = 999 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(CompositeName, CancellationToken.None);
        }

        await using var query = store.QuerySession();

        (await query.LoadAsync<Teardown439Product>(orphanId, TestContext.Current.CancellationToken))
            .ShouldBeNull("the source-registered member's documents must be torn down on rebuild");
        (await query.LoadAsync<Teardown439Metric>(orphanId, TestContext.Current.CancellationToken))
            .ShouldBeNull("the raw IProjection member's documents must be torn down on rebuild");

        // ...and the rebuild genuinely reproduced the read models it deleted.
        (await query.Query<Teardown439Product>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(1);
        (await query.Query<Teardown439Metric>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(1);
    }

    [Fact]
    public async Task the_composites_own_teardown_rules_are_applied()
    {
        // Second half of #5175: the parent goes through the same teardown collection as any other
        // source, so a view type declared on the COMPOSITE (not on any member) is wiped too.
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;

            opts.Projections.CompositeProjectionFor(CompositeName, composite =>
            {
                composite.Add(new Teardown439ProductProjection());
                composite.Options.DeleteViewTypeOnTeardown<Teardown439Metric>();
            });
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var streamId = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(streamId, new Teardown439Registered("Ankle Socks", "Socks"));
            session.Store(new Teardown439Metric { Id = Guid.NewGuid(), Price = 999 });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        using (var daemon = (IProjectionDaemon)await store.BuildProjectionDaemonAsync())
        {
            await daemon.RebuildProjectionAsync(CompositeName, CancellationToken.None);
        }

        await using var query = store.QuerySession();
        (await query.Query<Teardown439Metric>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);
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
}

public record Teardown439Registered(string Name, string Category);

public class Teardown439Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
}

public class Teardown439Metric
{
    public Guid Id { get; set; }
    public double Price { get; set; }
}

public partial class Teardown439ProductProjection : SingleStreamProjection<Teardown439Product, Guid>
{
    public Teardown439ProductProjection()
    {
        Name = "Teardown439Product";
    }

    public void Apply(Teardown439Registered e, Teardown439Product view)
    {
        view.Name = e.Name;
        view.Category = e.Category;
    }
}

/// <summary>
///     A raw <see cref="IProjection" />: it declares neither its storage nor its teardown, which is
///     exactly the shape #439 is about.
/// </summary>
public class Teardown439MetricProjection : IProjection
{
    public Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        foreach (var e in events)
        {
            if (e.Data is Teardown439Registered)
            {
                operations.Store(new Teardown439Metric { Id = e.StreamId, Price = 1 });
            }
        }

        return Task.CompletedTask;
    }
}
