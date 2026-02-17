using JasperFx;
using JasperFx.Events;
using Polecat.Serialization;

namespace Polecat.Tests;

public class StoreOptionsTests
{
    [Fact]
    public void default_schema_is_dbo()
    {
        var options = new StoreOptions();
        options.DatabaseSchemaName.ShouldBe("dbo");
    }

    [Fact]
    public void default_auto_create_is_create_or_update()
    {
        var options = new StoreOptions();
        options.AutoCreateSchemaObjects.ShouldBe(AutoCreate.CreateOrUpdate);
    }

    [Fact]
    public void default_command_timeout()
    {
        var options = new StoreOptions();
        options.CommandTimeout.ShouldBe(30);
    }

    [Fact]
    public void default_serializer_is_polecat_serializer()
    {
        var options = new StoreOptions();
        options.Serializer.ShouldBeOfType<PolecatSerializer>();
    }

    [Fact]
    public void default_stream_identity_is_guid()
    {
        var options = new StoreOptions();
        options.Events.StreamIdentity.ShouldBe(StreamIdentity.AsGuid);
    }

    [Fact]
    public void default_tenancy_style_is_single()
    {
        var options = new StoreOptions();
        options.Events.TenancyStyle.ShouldBe(TenancyStyle.Single);
    }

    [Fact]
    public void can_set_connection_string()
    {
        var options = new StoreOptions
        {
            ConnectionString = "Server=localhost;Database=test;"
        };
        options.ConnectionString.ShouldBe("Server=localhost;Database=test;");
    }

    [Fact]
    public void create_connection_factory_throws_without_connection_string()
    {
        var options = new StoreOptions();
        Should.Throw<InvalidOperationException>(() => options.CreateConnectionFactory());
    }

    [Fact]
    public void create_connection_factory_succeeds_with_connection_string()
    {
        var options = new StoreOptions
        {
            ConnectionString = "Server=localhost;Database=test;"
        };
        var factory = options.CreateConnectionFactory();
        factory.ShouldNotBeNull();
        factory.ConnectionString.ShouldBe("Server=localhost;Database=test;");
    }

    [Fact]
    public void can_override_schema_name()
    {
        var options = new StoreOptions { DatabaseSchemaName = "myschema" };
        options.DatabaseSchemaName.ShouldBe("myschema");
    }

    [Fact]
    public void can_configure_event_store_options()
    {
        var options = new StoreOptions();
        options.Events.StreamIdentity = StreamIdentity.AsString;
        options.Events.TenancyStyle = TenancyStyle.Conjoined;
        options.Events.EnableCorrelationId = true;
        options.Events.EnableCausationId = true;
        options.Events.EnableHeaders = true;

        options.Events.StreamIdentity.ShouldBe(StreamIdentity.AsString);
        options.Events.TenancyStyle.ShouldBe(TenancyStyle.Conjoined);
        options.Events.EnableCorrelationId.ShouldBeTrue();
        options.Events.EnableCausationId.ShouldBeTrue();
        options.Events.EnableHeaders.ShouldBeTrue();
    }
}
