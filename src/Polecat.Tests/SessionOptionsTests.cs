using System.Data;

namespace Polecat.Tests;

public class SessionOptionsTests
{
    [Fact]
    public void default_tracking_is_none()
    {
        var options = new SessionOptions();
        options.Tracking.ShouldBe(DocumentTracking.None);
    }

    [Fact]
    public void default_tenant_id()
    {
        var options = new SessionOptions();
        options.TenantId.ShouldBe(Tenancy.DefaultTenantId);
    }

    [Fact]
    public void default_isolation_level_is_read_committed()
    {
        var options = new SessionOptions();
        options.IsolationLevel.ShouldBe(IsolationLevel.ReadCommitted);
    }

    [Fact]
    public void default_timeout_is_null()
    {
        var options = new SessionOptions();
        options.Timeout.ShouldBeNull();
    }

    [Fact]
    public void can_set_identity_tracking()
    {
        var options = new SessionOptions { Tracking = DocumentTracking.IdentityOnly };
        options.Tracking.ShouldBe(DocumentTracking.IdentityOnly);
    }

    [Fact]
    public void can_set_tenant_id()
    {
        var options = new SessionOptions { TenantId = "tenant-a" };
        options.TenantId.ShouldBe("tenant-a");
    }
}
