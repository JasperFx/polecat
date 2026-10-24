using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Documents;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

/// <summary>
///     jasperfx#1079: the store-wide, every-tenant stream-state read. The only store-wide opener,
///     <c>OpenReadOnlyEventStore()</c>, opens a default-tenant session, which a conjoined store with
///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> off refuses — so a stream predicate could not
///     reach such a store at all.
/// </summary>
public class stream_states_for_all_tenants_tests
{
    private static async Task<DocumentStore> ConjoinedStoreAsync(string schema, bool defaultTenantEnabled,
        CancellationToken ct)
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = schema;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.DefaultTenantUsageEnabled = defaultTenantEnabled;
        });

        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);
        return store;
    }

    private static async Task StartAsync(DocumentStore store, string tenant, Guid id, params object[] events)
    {
        await using var session = store.LightweightSession(tenant);
        session.Events.StartStream(id, events);
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The jasperfx#1079 repro, verbatim in shape.</summary>
    [Fact]
    public async Task the_store_wide_opener_is_refused_with_the_default_tenant_disabled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await ConjoinedStoreAsync("ss_all_refused", false, ct);

        // Unchanged, and the reason the new member exists: this opener IS a default-tenant session.
        Should.Throw<Polecat.Exceptions.DefaultTenantUsageDisabledException>(() =>
            ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates());
    }

    [Fact]
    public async Task every_tenants_streams_with_the_default_tenant_disabled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await ConjoinedStoreAsync("ss_all_no_default", false, ct);

        var shared = Guid.NewGuid();
        await StartAsync(store, "t1", shared, new QuestStarted("a"), new QuestStarted("b"));
        await StartAsync(store, "t2", shared, new QuestStarted("c"));

        var all = await ((IEventStore)store).QueryStreamStatesForAllTenants()
            .Where(x => x.Id == shared && x.Version >= 1)
            .OrderBy(x => x.TenantId)
            .ToListAsync(ct);

        all.Select(x => x.TenantId).ShouldBe(["t1", "t2"]);
        all[0].Version.ShouldBe(2);
        all[1].Version.ShouldBe(1);

        // TenantId translates in Where like every other member.
        var onlyT2 = await ((IEventStore)store).QueryStreamStatesForAllTenants()
            .Where(x => x.TenantId == "t2" && x.Id == shared)
            .ToListAsync(ct);
        onlyT2.ShouldHaveSingleItem().Version.ShouldBe(1);

        // Count goes through the same provider.
        (await ((IEventStore)store).QueryStreamStatesForAllTenants().Where(x => x.Id == shared).CountAsync(ct))
            .ShouldBe(2);
    }

    [Fact]
    public async Task tenant_scoped_stream_states_carry_their_tenant()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await ConjoinedStoreAsync("ss_all_scoped", true, ct);

        var id = Guid.NewGuid();
        await StartAsync(store, "t1", id, new QuestStarted("a"));

        var readOnly = ((IEventStore)store).OpenReadOnlyEventStore("t1");
        var state = (await readOnly.QueryStreamStates("t1").Where(x => x.Id == id).ToListAsync(ct))
            .ShouldHaveSingleItem();
        state.TenantId.ShouldBe("t1");

        // ...and FetchStreamStateAsync reads through the same row reader.
        (await readOnly.FetchStreamStateAsync(id, ct))!.TenantId.ShouldBe("t1");
    }

    [Fact]
    public async Task single_tenanted_events_read_the_same_streams_as_the_tenant_less_query()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "ss_all_single";
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
        });
        await store.Database.ApplyAllConfiguredChangesToDatabaseAsync(ct: ct);

        var id = Guid.NewGuid();
        await using (var session = store.LightweightSession())
        {
            session.Events.StartStream(id, new QuestStarted("a"));
            await session.SaveChangesAsync(ct);
        }

        var all = await ((IEventStore)store).QueryStreamStatesForAllTenants().Where(x => x.Id == id).ToListAsync(ct);
        var tenantLess = await ((IEventStore)store).OpenReadOnlyEventStore().QueryStreamStates()
            .Where(x => x.Id == id).ToListAsync(ct);

        all.Select(x => (x.Id, x.Version, x.TenantId)).ShouldBe(tenantLess.Select(x => (x.Id, x.Version, x.TenantId)));
        all.ShouldHaveSingleItem().TenantId.ShouldBe(StorageConstants.DefaultTenantId);
    }

    [Fact]
    public void a_database_per_tenant_store_refuses_rather_than_answering_from_one_database()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "ss_all_multi_db";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.MultiTenantedDatabases(tenancy =>
            {
                tenancy.AddTenant("a", ConnectionSource.ConnectionStringFor(ConnectionSource.Scoped("ss_all_a")));
                tenancy.AddTenant("b", ConnectionSource.ConnectionStringFor(ConnectionSource.Scoped("ss_all_b")));
            });
        });

        var ex = Should.Throw<NotSupportedException>(() => ((IEventStore)store).QueryStreamStatesForAllTenants());
        ex.Message.ShouldContain(nameof(IEventStore.QueryStreamStatesForAllTenants));
    }
}
