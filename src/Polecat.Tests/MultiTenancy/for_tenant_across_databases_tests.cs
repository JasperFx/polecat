using JasperFx;
using JasperFx.Events;
using JasperFx.MultiTenancy;
using Microsoft.Data.SqlClient;
using Polecat.Exceptions;
using Polecat.Linq;
using Polecat.Storage;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

public record CrossDatabaseHappened(string Note);

public class CrossDatabaseDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
///     The three databases one of these test classes works in, for the life of the class. Named for
///     the class, because the two run at the same time.
/// </summary>
public abstract class ForTenantDatabases : IAsyncLifetime
{
    protected ForTenantDatabases(string name)
    {
        DbA = ConnectionSource.Scoped($"{name}_a");
        DbB = ConnectionSource.Scoped($"{name}_b");
        Control = ConnectionSource.Scoped($"{name}_control");
    }

    public string DbA { get; }
    public string DbB { get; }
    public string Control { get; }

    private string[] All => [DbA, DbB, Control];

    public async ValueTask InitializeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in All)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                IF DB_ID('{db}') IS NOT NULL
                BEGIN
                    ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [{db}];
                END
                CREATE DATABASE [{db}];
                """;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await using var conn = new SqlConnection(ConnectionSource.MasterConnectionString);
        await conn.OpenAsync();

        foreach (var db in All)
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
}

public class StaticForTenantDatabases() : ForTenantDatabases("ftd_static");

public class MasterTableForTenantDatabases() : ForTenantDatabases("ftd_master");

/// <summary>
///     A session holds one connection, to one tenant's database, and <c>ForTenant</c> shares it. Under
///     database-per-tenant tenancy that made every view of a tenant in another database read and write
///     the session's own database, with the other tenant's id on the rows and no error of any kind.
///     Writing to the right one would take a second transaction, which is not what a save on one session
///     promises, so the view is refused where it is asked for. Marten refuses it the same way, with
///     <c>InvalidTenantForDatabaseException</c> from <c>DocumentSessionBase.ForTenant</c>.
/// </summary>
public abstract class for_tenant_across_databases_context : IAsyncLifetime
{
    protected const string TenantA = "tenant_a";
    protected const string TenantB = "tenant_b";

    // On the same physical database as tenant_a, under a connection string spelled differently
    protected const string TenantShared = "tenant_shared";

    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ForTenantDatabases _databases;

    protected DocumentStore Store = null!;

    protected for_tenant_across_databases_context(ForTenantDatabases databases)
    {
        _databases = databases;
    }

    protected string A => ConnectionSource.ConnectionStringFor(_databases.DbA);
    protected string B => ConnectionSource.ConnectionStringFor(_databases.DbB);
    protected string Control => ConnectionSource.ConnectionStringFor(_databases.Control);

    protected string SharedWithA => new SqlConnectionStringBuilder(A) { ApplicationName = "shared" }.ConnectionString;

    protected static void ConfigureStore(StoreOptions opts)
    {
        opts.AutoCreateSchemaObjects = AutoCreate.All;
        opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        opts.Events.AddEventType<CrossDatabaseHappened>();
    }

    protected abstract Task<DocumentStore> BuildStoreAsync();

    public async ValueTask InitializeAsync() => Store = await BuildStoreAsync();

    public ValueTask DisposeAsync()
    {
        Store.Dispose();
        return ValueTask.CompletedTask;
    }

    // What a test asks of the tenant in the other database, from a session on tenant_a
    private static readonly Dictionary<string, Func<IDocumentSession, Guid, Task>> Operations = new()
    {
        ["Store"] = (session, id) =>
        {
            session.ForTenant(TenantB).Store(new CrossDatabaseDoc { Id = id, Name = "b" });
            return session.SaveChangesAsync(Ct);
        },
        ["Insert"] = (session, id) =>
        {
            session.ForTenant(TenantB).Insert(new CrossDatabaseDoc { Id = id, Name = "b" });
            return session.SaveChangesAsync(Ct);
        },
        ["StartStream"] = (session, id) =>
        {
            session.ForTenant(TenantB).Events.StartStream(id, new CrossDatabaseHappened("b"));
            return session.SaveChangesAsync(Ct);
        },
        ["Append"] = (session, id) =>
        {
            session.ForTenant(TenantB).Events.Append(id, new CrossDatabaseHappened("b"));
            return session.SaveChangesAsync(Ct);
        },
        ["TwoTenantsInOneSave"] = (session, id) =>
        {
            session.Store(new CrossDatabaseDoc { Id = id, Name = "a" });
            session.ForTenant(TenantB).Store(new CrossDatabaseDoc { Id = id, Name = "b" });
            return session.SaveChangesAsync(Ct);
        },
        ["LoadAsync"] = (session, id) => session.ForTenant(TenantB).LoadAsync<CrossDatabaseDoc>(id, Ct),
        ["Query"] = (session, _) => session.ForTenant(TenantB).Query<CrossDatabaseDoc>().ToListAsync(Ct),
        ["FetchStreamAsync"] = (session, id) => session.ForTenant(TenantB).Events.FetchStreamAsync(id, token: Ct),
        ["Nested"] = (session, id) =>
        {
            session.ForTenant(TenantA).ForTenant(TenantB).Store(new CrossDatabaseDoc { Id = id, Name = "b" });
            return session.SaveChangesAsync(Ct);
        }
    };

    private async Task ShouldHaveNothingForAsync(Guid id)
    {
        foreach (var tenant in new[] { TenantA, TenantB })
        {
            await using var session = Store.LightweightSession(tenant);

            (await session.LoadAsync<CrossDatabaseDoc>(id, Ct)).ShouldBeNull();
            (await session.Events.FetchStreamAsync(id, token: Ct)).ShouldBeEmpty();
        }
    }

    [Theory]
    [InlineData("Store")]
    [InlineData("Insert")]
    [InlineData("StartStream")]
    [InlineData("Append")]
    [InlineData("TwoTenantsInOneSave")]
    [InlineData("LoadAsync")]
    [InlineData("Query")]
    [InlineData("FetchStreamAsync")]
    [InlineData("Nested")]
    public async Task a_tenant_in_another_database_is_refused(string operation)
    {
        var id = Guid.NewGuid();

        await using (var session = Store.LightweightSession(TenantA))
        {
            var refusal = await Should.ThrowAsync<InvalidTenantForDatabaseException>(
                () => Operations[operation](session, id));

            refusal.TenantId.ShouldBe(TenantB);
            refusal.Message.ShouldContain(TenantB);
            refusal.Message.ShouldContain(_databases.DbA);
        }

        await ShouldHaveNothingForAsync(id);
    }

    [Fact]
    public async Task the_refusal_comes_from_for_tenant_itself()
    {
        await using var session = Store.LightweightSession(TenantA);

        Should.Throw<InvalidTenantForDatabaseException>(() => session.ForTenant(TenantB));
    }

    [Fact]
    public async Task the_refusal_is_not_remembered()
    {
        await using var session = Store.LightweightSession(TenantA);

        Should.Throw<InvalidTenantForDatabaseException>(() => session.ForTenant(TenantB));
        Should.Throw<InvalidTenantForDatabaseException>(() => session.ForTenant(TenantB));
    }

    [Fact]
    public async Task a_session_goes_on_working_after_a_refusal()
    {
        var id = Guid.NewGuid();

        await using (var session = Store.LightweightSession(TenantA))
        {
            session.Store(new CrossDatabaseDoc { Id = id, Name = "a" });
            Should.Throw<InvalidTenantForDatabaseException>(() => session.ForTenant(TenantB));

            await session.SaveChangesAsync(Ct);
        }

        await using var a = Store.LightweightSession(TenantA);
        await using var b = Store.LightweightSession(TenantB);

        (await a.LoadAsync<CrossDatabaseDoc>(id, Ct)).ShouldNotBeNull();
        (await b.LoadAsync<CrossDatabaseDoc>(id, Ct)).ShouldBeNull();
    }

    [Fact]
    public async Task a_view_of_the_sessions_own_tenant_is_allowed()
    {
        var doc = Guid.NewGuid();
        var stream = Guid.NewGuid();

        await using (var session = Store.LightweightSession(TenantA))
        {
            var view = session.ForTenant(TenantA);

            view.Store(new CrossDatabaseDoc { Id = doc, Name = "a" });
            view.Events.StartStream(stream, new CrossDatabaseHappened("a"));

            await session.SaveChangesAsync(Ct);
        }

        await using var query = Store.LightweightSession(TenantA);

        (await query.LoadAsync<CrossDatabaseDoc>(doc, Ct)).ShouldNotBeNull();
        (await query.Events.FetchStreamAsync(stream, token: Ct)).Count.ShouldBe(1);
    }

    [Fact]
    public async Task two_tenants_in_one_database_share_a_save()
    {
        var doc = Guid.NewGuid();
        var stream = Guid.NewGuid();

        await using (var session = Store.LightweightSession(TenantA))
        {
            var view = session.ForTenant(TenantShared);

            session.Events.StartStream(Guid.NewGuid(), new CrossDatabaseHappened("a"));
            view.Store(new CrossDatabaseDoc { Id = doc, Name = "shared" });
            view.Events.StartStream(stream, new CrossDatabaseHappened("shared"));

            await session.SaveChangesAsync(Ct);
        }

        await using var query = Store.LightweightSession(TenantShared);

        (await query.LoadAsync<CrossDatabaseDoc>(doc, Ct)).ShouldNotBeNull();

        var events = await query.Events.FetchStreamAsync(stream, token: Ct);
        events.Single().TenantId.ShouldBe(TenantShared);
    }

    [Fact]
    public async Task a_session_bound_to_a_database_is_held_to_that_database()
    {
        var database = Store.Options.Tenancy!.GetDatabase(TenantA);
        var doc = Guid.NewGuid();

        await using (var session = Store.LightweightSession(SessionOptions.ForDatabase(database)))
        {
            Should.Throw<InvalidTenantForDatabaseException>(() => session.ForTenant(TenantB));

            session.ForTenant(TenantA).Store(new CrossDatabaseDoc { Id = doc, Name = "a" });
            await session.SaveChangesAsync(Ct);
        }

        await using var query = Store.LightweightSession(TenantA);
        (await query.LoadAsync<CrossDatabaseDoc>(doc, Ct)).ShouldNotBeNull();
    }

    [Fact]
    public async Task a_tenant_nobody_registered_is_refused_as_unknown()
    {
        await using var session = Store.LightweightSession(TenantA);

        Should.Throw<UnknownTenantIdException>(() => session.ForTenant("nobody"));
    }
}

public class for_tenant_across_static_databases_tests(StaticForTenantDatabases databases)
    : for_tenant_across_databases_context(databases), IClassFixture<StaticForTenantDatabases>
{
    protected override Task<DocumentStore> BuildStoreAsync() => Task.FromResult(DocumentStore.For(opts =>
    {
        ConfigureStore(opts);
        opts.MultiTenantedDatabases(tenancy =>
        {
            tenancy.AddTenant(TenantA, A);
            tenancy.AddTenant(TenantB, B);
            tenancy.AddTenant(TenantShared, SharedWithA);
        });
    }));
}

public class for_tenant_across_master_table_databases_tests(MasterTableForTenantDatabases databases)
    : for_tenant_across_databases_context(databases), IClassFixture<MasterTableForTenantDatabases>
{
    private MasterTableTenancy _tenancy = null!;

    protected override async Task<DocumentStore> BuildStoreAsync()
    {
        var store = DocumentStore.For(opts =>
        {
            ConfigureStore(opts);
            _tenancy = opts.MultiTenantedMasterTable(Control);
        });

        await _tenancy.AddDatabaseRecordAsync(TenantA, A, Ct);
        await _tenancy.AddDatabaseRecordAsync(TenantB, B, Ct);
        await _tenancy.AddDatabaseRecordAsync(TenantShared, SharedWithA, Ct);

        return store;
    }

    [Fact]
    public async Task a_disabled_tenant_is_refused_as_disabled()
    {
        await _tenancy.AddDatabaseRecordAsync("tenant_off", B, Ct);
        await _tenancy.DisableTenantAsync("tenant_off", Ct);

        await using var session = Store.LightweightSession(TenantA);

        Should.Throw<DisabledTenantException>(() => session.ForTenant("tenant_off"));
    }
}
