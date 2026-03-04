using JasperFx.Events.Projections;

using JasperFx;

namespace Polecat.EntityFrameworkCore.Tests;

public class ef_core_multi_tenancy_validation_tests
{
    [Fact]
    public void single_stream_should_fail_when_conjoined_but_no_ITenanted()
    {
        var ex = Should.Throw<Exception>(() =>
        {
            DocumentStore.For(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
                opts.AutoCreateSchemaObjects = AutoCreate.All;
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;

                opts.Projections.Add<NonTenantedOrderAggregate, NonTenantedOrder, TestDbContext>(
                    opts, new NonTenantedOrderAggregate(), ProjectionLifecycle.Inline);
            });
        });

        ex.Message.ShouldContain("ITenanted");
    }

    [Fact]
    public void multi_stream_should_fail_when_conjoined_but_no_ITenanted()
    {
        var ex = Should.Throw<Exception>(() =>
        {
            DocumentStore.For(opts =>
            {
                opts.ConnectionString = ConnectionSource.ConnectionString;
                opts.AutoCreateSchemaObjects = AutoCreate.All;
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;

                opts.Projections.Add<NonTenantedMultiStreamProjection, NonTenantedOrder, Guid, TestDbContext>(
                    opts, new NonTenantedMultiStreamProjection(), ProjectionLifecycle.Inline);
            });
        });

        ex.Message.ShouldContain("ITenanted");
    }

    [Fact]
    public void single_stream_should_pass_when_conjoined_with_ITenanted()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;

            opts.Projections.Add<TenantedOrderAggregate, TenantedOrder, TenantedTestDbContext>(
                opts, new TenantedOrderAggregate(), ProjectionLifecycle.Inline);
        });

        store.ShouldNotBeNull();
        store.Dispose();
    }

    [Fact]
    public void single_stream_should_pass_without_conjoined_tenancy()
    {
        var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.AutoCreateSchemaObjects = AutoCreate.All;

            opts.Projections.Add<NonTenantedOrderAggregate, NonTenantedOrder, TestDbContext>(
                opts, new NonTenantedOrderAggregate(), ProjectionLifecycle.Inline);
        });

        store.ShouldNotBeNull();
        store.Dispose();
    }
}
