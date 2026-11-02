using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Tags;
using Microsoft.Data.SqlClient;
using Polecat.Tests.Events;
using Polecat.Tests.Harness;
using Polecat.TestUtils;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     #891 item 1: on database-per-tenant under <c>UseArchivedStreamPartitioning</c>, a tag table has
///     neither a <c>tenant_id</c> nor a foreign key, so neither the tenant delete nor a cascade reached it
///     and a tag value outlived the tenant. A probe ended with <c>events=0 tagrows=1 versions=0</c>.
/// </summary>
public class delete_tenant_data_tag_rows_tests : IAsyncLifetime
{
    private const string Schema = "tag_wipe";
    private const string TagTable = "pc_event_tag_student";

    private static readonly string DbA = ConnectionSource.Scoped("tag_wipe_a");
    private static readonly string DbB = ConnectionSource.Scoped("tag_wipe_b");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await DropDatabasesAsync();

        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync(Ct);

        foreach (var db in new[] { DbA, DbB })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE [{db}];";
            await cmd.ExecuteNonQueryAsync(Ct);
        }
    }

    public ValueTask DisposeAsync() => new(DropDatabasesAsync());

    private static async Task DropDatabasesAsync()
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

    private static DocumentStore CreateStore(bool archivedPartitioning) => DocumentStore.For(opts =>
    {
        opts.ConnectionString = ConnectionSource.ConnectionStringFor(DbA);
        opts.DatabaseSchemaName = Schema;
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.EventGraph.UseArchivedStreamPartitioning = archivedPartitioning;
        opts.Events.AddEventType<StudentEnrolled>();
        opts.Events.RegisterTagType<StudentId>("student").ForAggregate<StudentCourseEnrollment>();

        opts.MultiTenantedDatabases(tenancy =>
        {
            tenancy.AddTenant("alpha", ConnectionSource.ConnectionStringFor(DbA));
            tenancy.AddTenant("beta", ConnectionSource.ConnectionStringFor(DbB));

            // A database can hold more than one tenant, and only one of them is being deleted
            tenancy.AddTenant("alpha-neighbour", ConnectionSource.ConnectionStringFor(DbA));
        });
    });

    private static async Task AppendAsync(DocumentStore store, string tenant)
    {
        await using var session = store.LightweightSession(new SessionOptions { TenantId = tenant });
        var tagged = session.Events.BuildEvent(new StudentEnrolled("Ann", "Math"));
        tagged.WithTag(new StudentId(Guid.NewGuid()));
        session.Events.Append(Guid.NewGuid(), tagged);
        await session.SaveChangesAsync(Ct);
    }

    private static async Task<int> CountAsync(string database, string table, string? where = null)
    {
        await using var conn = new SqlConnection(ConnectionSource.ConnectionStringFor(database));
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].[{table}] {where}";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_tenants_tag_rows_go_with_it(bool archivedPartitioning)
    {
        using var store = CreateStore(archivedPartitioning);
        await AppendAsync(store, "alpha");
        await AppendAsync(store, "beta");

        (await CountAsync(DbA, TagTable)).ShouldBe(1);

        await store.Advanced.DeleteAllTenantDataAsync("alpha", Ct);

        (await CountAsync(DbA, "pc_events")).ShouldBe(0);
        (await CountAsync(DbA, TagTable)).ShouldBe(0);
        (await CountAsync(DbA, "pc_dcb_tag_version", "WHERE tenant_id = 'alpha'")).ShouldBe(0);

        (await CountAsync(DbB, "pc_events")).ShouldBe(1);
        (await CountAsync(DbB, TagTable)).ShouldBe(1);
    }

    [Fact]
    public async Task only_the_deleted_tenants_tag_rows_go_from_a_shared_database()
    {
        using var store = CreateStore(archivedPartitioning: true);
        await AppendAsync(store, "alpha");
        await AppendAsync(store, "alpha-neighbour");

        (await CountAsync(DbA, TagTable)).ShouldBe(2);

        await store.Advanced.DeleteAllTenantDataAsync("alpha", Ct);

        (await CountAsync(DbA, TagTable)).ShouldBe(1);
        (await CountAsync(DbA, "pc_events")).ShouldBe(1);
    }

    [Fact]
    public async Task the_report_counts_the_tag_rows()
    {
        using var store = CreateStore(archivedPartitioning: true);
        await AppendAsync(store, "alpha");

        var deleted = await store.Advanced.DeleteAllTenantDataAsync("alpha", Ct);

        deleted[TagTable].ShouldBe(1);
    }
}
