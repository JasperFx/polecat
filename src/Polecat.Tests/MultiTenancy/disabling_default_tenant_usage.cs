using JasperFx;
using Polecat.Exceptions;
using Polecat.Tests.Harness;

namespace Polecat.Tests.MultiTenancy;

/// <summary>
///     Port of Marten's <c>DocumentDbTests/MultiTenancy/disabling_default_tenant_usage.cs</c>.
///     Every way of opening a session against the default tenant has to fail once
///     <see cref="StoreOptions.DefaultTenantUsageEnabled" /> is off, otherwise the setting is a
///     suggestion rather than a guard. polecat#514.
/// </summary>
public class disabling_default_tenant_usage
{
    private static DocumentStore CreateStore()
    {
        return DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.DatabaseSchemaName = "no_default_tenant";
            opts.UseNativeJsonType = ConnectionSource.SupportsNativeJson;
            opts.DefaultTenantUsageEnabled = false;
        });
    }

    [Fact]
    public void default_tenant_usage_is_enabled_by_default()
    {
        new StoreOptions().DefaultTenantUsageEnabled.ShouldBeTrue();
    }

    [Fact]
    public void get_exception_when_creating_session_with_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() => store.LightweightSession());
    }

    [Fact]
    public void get_exception_when_creating_identity_session_with_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() => store.IdentitySession());
    }

    [Fact]
    public void get_exception_when_creating_query_session_with_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() => store.QuerySession());
    }

    [Fact]
    public void get_exception_when_creating_session_with_default_tenant_session_options_and_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() =>
            store.LightweightSession(new SessionOptions { TenantId = StorageConstants.DefaultTenantId }));
    }

    [Fact]
    public void get_exception_when_creating_query_session_with_default_tenant_session_options_and_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() =>
            store.QuerySession(new SessionOptions { TenantId = StorageConstants.DefaultTenantId }));
    }

    [Fact]
    public void get_exception_when_opening_a_session_with_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        Should.Throw<DefaultTenantUsageDisabledException>(() =>
            store.OpenSession(new SessionOptions { TenantId = StorageConstants.DefaultTenantId }));
    }

    [Fact]
    public async Task get_exception_when_opening_a_session_async_with_default_tenant_usage_disabled()
    {
        using var store = CreateStore();

        await Should.ThrowAsync<DefaultTenantUsageDisabledException>(async () =>
            await store.OpenSessionAsync(
                new SessionOptions { TenantId = StorageConstants.DefaultTenantId },
                TestContext.Current.CancellationToken));
    }

    /// <summary>
    ///     Marten's <c>SessionOptions.AllowAnyTenant</c> escape hatch: the daemon and any other
    ///     infrastructure that has already resolved a database must still be able to open a session
    ///     for the default tenant.
    /// </summary>
    [Fact]
    public async Task allow_any_tenant_bypasses_the_guard()
    {
        using var store = CreateStore();

        await using var session = store.LightweightSession(new SessionOptions
        {
            TenantId = StorageConstants.DefaultTenantId,
            AllowAnyTenant = true
        });

        session.ShouldNotBeNull();
    }

    /// <summary>
    ///     <c>SessionOptions.ForDatabase</c> sets <c>AllowAnyTenant</c>, exactly as Marten's does —
    ///     that is what lets the async daemon work a tenant database whose events all carry the
    ///     default tenant id.
    /// </summary>
    [Fact]
    public async Task for_database_allows_any_tenant()
    {
        using var store = CreateStore();

        var options = SessionOptions.ForDatabase(store.Database);

        options.AllowAnyTenant.ShouldBeTrue();
        options.Database.ShouldBeSameAs(store.Database);
        options.TenantId.ShouldBe(StorageConstants.DefaultTenantId);

        await using var session = store.LightweightSession(options);
        session.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_non_default_tenant_is_unaffected()
    {
        using var store = CreateStore();

        await using var session = store.LightweightSession(new SessionOptions { TenantId = "tenant_a" });

        session.TenantId.ShouldBe("tenant_a");
    }
}
