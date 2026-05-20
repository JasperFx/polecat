using JasperFx.Events;
using Polecat;
using Shouldly;

namespace Polecat.Tests.Schema;

/// <summary>
///     Coverage for Polecat's implementation of the lifted
///     <see cref="IDocumentSchemaResolver"/> (jasperfx#333), exposed as
///     <c>StoreOptions.SchemaResolver</c>.
/// </summary>
public class document_schema_resolver_tests
{
    private sealed record Customer(Guid Id, string Name);

    private static IDocumentSchemaResolver ResolverFor(string schema)
    {
        var options = new StoreOptions { DatabaseSchemaName = schema };
        return options.SchemaResolver;
    }

    [Fact]
    public void resolves_schema_names()
    {
        var resolver = ResolverFor("events");
        resolver.DatabaseSchemaName.ShouldBe("events");
        // Events share the document schema in Polecat.
        resolver.EventsSchemaName.ShouldBe("events");
    }

    [Fact]
    public void resolves_event_store_tables_qualified_and_bare()
    {
        var resolver = ResolverFor("dbo");

        resolver.ForEvents().ShouldBe("[dbo].[pc_events]");
        resolver.ForStreams().ShouldBe("[dbo].[pc_streams]");
        resolver.ForEventProgression().ShouldBe("[dbo].[pc_event_progression]");

        resolver.ForEvents(qualified: false).ShouldBe("pc_events");
        resolver.ForStreams(qualified: false).ShouldBe("pc_streams");
        resolver.ForEventProgression(qualified: false).ShouldBe("pc_event_progression");
    }

    [Fact]
    public void resolves_document_table_by_type()
    {
        var resolver = ResolverFor("app");

        resolver.For<Customer>().ShouldBe("[app].[pc_doc_customer]");
        resolver.For<Customer>(qualified: false).ShouldBe("pc_doc_customer");
        resolver.For(typeof(Customer)).ShouldBe("[app].[pc_doc_customer]");
        resolver.For(typeof(Customer), qualified: false).ShouldBe("pc_doc_customer");
    }

    [Fact]
    public void honors_custom_schema()
    {
        ResolverFor("tenant_a").ForEvents().ShouldBe("[tenant_a].[pc_events]");
    }
}
