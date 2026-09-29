using Polecat.Linq;
using JasperFx;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Internal;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     polecat#680 — <c>AdvancedOperations.DeleteAllTenantDataAsync</c>, the tenant wipe that works on
///     a conjoined store WITHOUT partitioning. Before it, the only tenant-offboarding path was
///     <c>RemovePolecatManagedTenantsAsync(..., DeleteData)</c>, which requires a partitioned store and
///     drops a partition rather than deleting rows.
///     <para>
///         Every fact here asserts the OTHER tenant is intact as well as the target being gone. For a
///         destructive API those are two different claims, and the dangerous failure is the one where
///         the target is correctly erased and a neighbour quietly goes with it.
///     </para>
/// </summary>
public class delete_all_tenant_data_tests
{
    private const string Schema = "delete_tenant_data";

    private static DocumentStore CreateStore(bool conjoined = true)
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = Schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            if (conjoined)
            {
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;
                opts.Policies.AllDocumentsAreMultiTenanted();
            }
        });
    }

    [Fact]
    public async Task erases_one_tenants_documents_events_and_streams_and_leaves_the_others()
    {
        using var store = CreateStore();
        await store.Advanced.Clean.CompletelyRemoveAllAsync(TestContext.Current.CancellationToken);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        var redStream = Guid.NewGuid();
        var blueStream = Guid.NewGuid();

        await using (var red = store.LightweightSession("red"))
        {
            red.Store(new TenantWidget { Id = Guid.NewGuid(), Name = "red-widget" });
            red.Events.StartStream(redStream, new WidgetMade("red"), new WidgetMade("red-2"));
            await red.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var blue = store.LightweightSession("blue"))
        {
            blue.Store(new TenantWidget { Id = Guid.NewGuid(), Name = "blue-widget" });
            blue.Events.StartStream(blueStream, new WidgetMade("blue"));
            await blue.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var report = await store.Advanced.DeleteAllTenantDataAsync("red", TestContext.Current.CancellationToken);

        // The report is the caller's evidence, so it has to be non-empty and name real tables.
        report.ShouldNotBeEmpty();
        report.Values.Sum().ShouldBeGreaterThan(0);

        await using (var red = store.QuerySession("red"))
        {
            (await red.Query<TenantWidget>().CountAsync(TestContext.Current.CancellationToken)).ShouldBe(0);
            (await red.Events.FetchStreamAsync(redStream, token: TestContext.Current.CancellationToken))
                .ShouldBeEmpty();
        }

        await using (var blue = store.QuerySession("blue"))
        {
            (await blue.Query<TenantWidget>().CountAsync(TestContext.Current.CancellationToken))
                .ShouldBe(1, "the other tenant's documents are untouched");
            (await blue.Events.FetchStreamAsync(blueStream, token: TestContext.Current.CancellationToken))
                .Count.ShouldBe(1, "the other tenant's events are untouched");
        }
    }

    /// <summary>
    ///     The stream row carries the inline snapshot, so erasing a tenant's streams erases its
    ///     snapshots with them. Asserted at the table rather than through the API, because a snapshot
    ///     that survives its stream is invisible to every read path and would only surface later as a
    ///     stale aggregate for a re-onboarded tenant of the same id.
    /// </summary>
    [Fact]
    public async Task leaves_no_stream_row_behind_for_the_erased_tenant()
    {
        using var store = CreateStore();
        await store.Advanced.Clean.CompletelyRemoveAllAsync(TestContext.Current.CancellationToken);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using (var red = store.LightweightSession("red"))
        {
            red.Events.StartStream(Guid.NewGuid(), new WidgetMade("red"));
            await red.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await using (var blue = store.LightweightSession("blue"))
        {
            blue.Events.StartStream(Guid.NewGuid(), new WidgetMade("blue"));
            await blue.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        await store.Advanced.DeleteAllTenantDataAsync("red", TestContext.Current.CancellationToken);

        (await CountRowsAsync("pc_streams", "red")).ShouldBe(0);
        (await CountRowsAsync("pc_events", "red")).ShouldBe(0);
        (await CountRowsAsync("pc_streams", "blue")).ShouldBe(1);
        (await CountRowsAsync("pc_events", "blue")).ShouldBe(1);
    }

    /// <summary>
    ///     <c>pc_event_progression</c> has no <c>tenant_id</c> — a tenant's rows are identified by the
    ///     <c>":{tenant}"</c> suffix on the shard name, so they need their own deletion and their own
    ///     fact. A left-behind high-water row makes a re-onboarded tenant of the same id start its
    ///     projections at the OLD tenant's mark and skip its entire history.
    /// </summary>
    [Fact]
    public async Task removes_the_tenants_progression_rows_and_only_those()
    {
        using var store = CreateStore();
        await store.Advanced.Clean.CompletelyRemoveAllAsync(TestContext.Current.CancellationToken);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await InsertProgressionAsync("HighWaterMark:red", 40);
        await InsertProgressionAsync("Tally:All:red", 41);
        await InsertProgressionAsync("HighWaterMark:blue", 50);
        await InsertProgressionAsync("HighWaterMark", 60);

        await store.Advanced.DeleteAllTenantDataAsync("red", TestContext.Current.CancellationToken);

        (await ProgressionNamesAsync()).OrderBy(x => x)
            .ShouldBe(["HighWaterMark", "HighWaterMark:blue"]);
    }

    /// <summary>
    ///     A store with no multi-tenancy has no tenant rows to single out, so the call reports an empty
    ///     wipe rather than throwing OR deleting everything. The second half is the one that matters:
    ///     with no tenant predicate to apply, "delete this tenant" and "delete the store" are the same
    ///     statement, and this must not be the latter.
    /// </summary>
    [Fact]
    public async Task a_single_tenanted_store_deletes_nothing_and_says_so()
    {
        using var store = CreateStore(conjoined: false);
        await store.Advanced.Clean.CompletelyRemoveAllAsync(TestContext.Current.CancellationToken);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await using (var session = store.LightweightSession())
        {
            session.Store(new TenantWidget { Id = Guid.NewGuid(), Name = "only" });
            await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        }

        var report = await store.Advanced.DeleteAllTenantDataAsync("red", TestContext.Current.CancellationToken);

        report.ShouldBeEmpty();

        await using var check = store.QuerySession();
        (await check.Query<TenantWidget>().CountAsync(TestContext.Current.CancellationToken))
            .ShouldBe(1, "a single-tenanted store's data is not 'some tenant's data'");
    }

    /// <summary>
    ///     An empty tenant id is refused rather than silently meaning the default tenant, because
    ///     "erase the tenant I failed to specify" is never the intent behind this call.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task an_unspecified_tenant_is_refused(string? tenantId)
    {
        using var store = CreateStore();
        await Should.ThrowAsync<ArgumentException>(
            () => store.Advanced.DeleteAllTenantDataAsync(tenantId!, TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     A tenant id containing a LIKE metacharacter must match only itself. The progression delete
    ///     is a suffix match, so an unescaped <c>%</c> tenant would take every other tenant's
    ///     progression rows with it — the exact shape of a cross-tenant data loss that no ordinary
    ///     tenant id would ever reveal.
    /// </summary>
    [Fact]
    public async Task a_tenant_id_containing_a_like_wildcard_matches_only_itself()
    {
        using var store = CreateStore();
        await store.Advanced.Clean.CompletelyRemoveAllAsync(TestContext.Current.CancellationToken);
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: TestContext.Current.CancellationToken);

        await InsertProgressionAsync("HighWaterMark:%", 10);
        await InsertProgressionAsync("HighWaterMark:blue", 20);
        await InsertProgressionAsync("HighWaterMark:green", 30);

        await store.Advanced.DeleteAllTenantDataAsync("%", TestContext.Current.CancellationToken);

        (await ProgressionNamesAsync()).OrderBy(x => x)
            .ShouldBe(["HighWaterMark:blue", "HighWaterMark:green"]);
    }

    /// <summary>
    ///     The FK ordering, as a unit fact — a parent must not be emitted before the children that
    ///     reference it. Exercised directly because the database only reveals a wrong order as an FK
    ///     violation on the particular schema under test, and the ordering has to hold for user-defined
    ///     document foreign keys too.
    /// </summary>
    [Fact]
    public void the_delete_order_puts_every_child_before_its_parent()
    {
        var tables = new[] { "pc_streams", "pc_events", "pc_event_tag_x", "pc_natural_key_y" };
        var edges = new[]
        {
            ("pc_events", "pc_streams"),
            ("pc_event_tag_x", "pc_events"),
            ("pc_natural_key_y", "pc_streams")
        };

        var ordered = TenantDataCleaner.OrderChildrenFirst(tables, edges);

        ordered.Count.ShouldBe(4);
        foreach (var (child, parent) in edges)
        {
            ordered.IndexOf(child).ShouldBeLessThan(ordered.IndexOf(parent),
                $"{child} references {parent}, so its rows have to go first");
        }
    }

    /// <summary>
    ///     A foreign-key CYCLE cannot be satisfied by any ordering, and Weasel can create one
    ///     deliberately (weasel#540). Every table still has to appear exactly once: dropping one would
    ///     leave a tenant partially erased while reporting success.
    /// </summary>
    [Fact]
    public void a_foreign_key_cycle_still_yields_every_table_exactly_once()
    {
        var tables = new[] { "a", "b", "c" };
        var edges = new[] { ("a", "b"), ("b", "a"), ("c", "a") };

        var ordered = TenantDataCleaner.OrderChildrenFirst(tables, edges);

        ordered.OrderBy(x => x).ShouldBe(["a", "b", "c"]);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static async Task<int> CountRowsAsync(string table, string tenant)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[{table}] WHERE tenant_id = @t;";
        cmd.Parameters.AddWithValue("@t", tenant);
        return (int)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task InsertProgressionAsync(string name, long seq)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            DELETE FROM [{Schema}].[pc_event_progression] WHERE name = @n;
            INSERT INTO [{Schema}].[pc_event_progression] (name, last_seq_id, last_updated)
            VALUES (@n, @s, SYSDATETIMEOFFSET());
            """;
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@s", seq);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> ProgressionNamesAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT name FROM [{Schema}].[pc_event_progression];";
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        return names;
    }
}

public class TenantWidget
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public record WidgetMade(string Name);
