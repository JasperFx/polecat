using JasperFx.Events;
using Microsoft.Extensions.DependencyInjection;
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
        session.TenantId.ShouldBe(JasperFx.StorageConstants.DefaultTenantId);
    }

    [Fact]
    public void session_respects_custom_tenant_id()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var session = store.LightweightSession(new SessionOptions { TenantId = "tenant-a" });
        session.TenantId.ShouldBe("tenant-a");
    }

    // polecat#207: IEventStore.Identity must vary by StoreName so multiple stores are distinguishable.
    [Fact]
    public void event_store_identity_defaults_to_store_name_main()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        var identity = ((IEventStore)store).Identity;
        identity.Name.ShouldBe("main"); // "Main".ToLowerInvariant()
        identity.Type.ShouldBe("SqlServer");
    }

    [Fact]
    public void event_store_identity_varies_by_store_name()
    {
        using var primary = DocumentStore.For(ConnectionSource.ConnectionString);
        using var named = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.StoreName = "Invoicing";
        });

        var a = ((IEventStore)primary).Identity;
        var b = ((IEventStore)named).Identity;

        b.Name.ShouldBe("invoicing");
        a.ToString().ShouldNotBe(b.ToString());
    }

    [Fact]
    public void ancillary_store_takes_store_name_from_marker_type()
    {
        var services = new ServiceCollection();
        services.AddPolecatStore<IInvoiceStore>(opts => opts.ConnectionString = ConnectionSource.ConnectionString);

        using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IInvoiceStore>();

        store.Options.StoreName.ShouldBe(nameof(IInvoiceStore));
        ((IEventStore)store).Identity.Name.ShouldBe("iinvoicestore");
    }

    // polecat#320: IEventStore.Subject identifies the store, not the database backing it.
    [Fact]
    public void event_store_subject_defaults_to_polecat_main()
    {
        using var store = DocumentStore.For(ConnectionSource.ConnectionString);
        ((IEventStore)store).Subject.ShouldBe(new Uri("polecat://main"));
    }

    [Fact]
    public void event_store_subject_varies_by_store_name()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.StoreName = "Invoicing";
        });

        ((IEventStore)store).Subject.ShouldBe(new Uri("polecat://invoicing"));
    }

    // The actual regression: a primary store and an ancillary store sharing ONE database (schemas
    // apart) must still report distinct Subjects, or monitoring tools bucket their shard states and
    // high-water marks together.
    [Fact]
    public void stores_sharing_a_database_have_distinct_subjects()
    {
        using var primary = DocumentStore.For(ConnectionSource.ConnectionString);

        var services = new ServiceCollection();
        services.AddPolecatStore<IInvoiceStore>(opts => opts.ConnectionString = ConnectionSource.ConnectionString);
        using var provider = services.BuildServiceProvider();
        var ancillary = provider.GetRequiredService<IInvoiceStore>();

        ((IEventStore)primary).Subject.ShouldBe(new Uri("polecat://main"));
        ((IEventStore)ancillary).Subject.ShouldBe(new Uri("polecat://iinvoicestore"));
        ((IEventStore)primary).Subject.ShouldNotBe(((IEventStore)ancillary).Subject);
    }
}

public interface IInvoiceStore : IDocumentStore;
