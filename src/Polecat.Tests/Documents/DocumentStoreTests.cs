using Polecat.Tests.Harness;

namespace Polecat.Tests.Documents;

public class DocumentStoreTests
{
    [Fact]
    public void can_create_with_for_action()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
        });

        store.Options.ConnectionString.ShouldBe(ConnectionSource.ConnectionString);
    }

    [Fact]
    public void can_create_with_for_connection_string()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        store.Options.ConnectionString.ShouldBe(ConnectionSource.ConnectionString);
    }

    [Fact]
    public void throws_without_connection_string()
    {
        Should.Throw<InvalidOperationException>(() =>
            DocumentStore.For(opts => { /* no connection string */ }));
    }

    [Fact]
    public void lightweight_session_returns_session()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.LightweightSession();
        session.ShouldNotBeNull();
        session.ShouldBeAssignableTo<IDocumentSession>();
    }

    [Fact]
    public void identity_session_returns_session()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.IdentitySession();
        session.ShouldNotBeNull();
        session.ShouldBeAssignableTo<IDocumentSession>();
    }

    [Fact]
    public void query_session_returns_session()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.QuerySession();
        session.ShouldNotBeNull();
        session.ShouldBeAssignableTo<IQuerySession>();
    }

    [Fact]
    public void open_session_uses_tracking_option()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);

        var lightweight = store.OpenSession(new SessionOptions { Tracking = DocumentTracking.None });
        lightweight.ShouldBeAssignableTo<IDocumentSession>();

        var identity = store.OpenSession(new SessionOptions { Tracking = DocumentTracking.IdentityOnly });
        identity.ShouldBeAssignableTo<IDocumentSession>();
    }

    [Fact]
    public void session_has_default_tenant_id()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.LightweightSession();
        session.TenantId.ShouldBe(Tenancy.DefaultTenantId);
    }

    [Fact]
    public void session_respects_custom_tenant_id()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" });
        session.TenantId.ShouldBe("tenant-a");
    }
}
