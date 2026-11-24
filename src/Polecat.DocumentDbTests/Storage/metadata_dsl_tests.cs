using Polecat.Attributes;
using Polecat.Storage;
using Polecat.TestUtils;
using Shouldly;

namespace Polecat.Tests.Storage;

/// <summary>
/// #243: the document metadata DSL — Schema.For&lt;T&gt;().Metadata(m => ...) with Enabled + MapTo,
/// plus metadata attributes. This issue adds the configuration surface (the columns issue #241 and
/// the read API #242 consume it); these tests assert the config is captured correctly.
/// </summary>
public class metadata_dsl_tests
{
    public class PlainDoc
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class MappedDoc
    {
        public Guid Id { get; set; }
        public DateTimeOffset CreatedDate { get; set; }
        public string? ModifiedBy { get; set; }
    }

    public class AttributedDoc
    {
        public Guid Id { get; set; }

        [LastModifiedByMetadata]
        public string? ModifiedByUser { get; set; }

        [CreatedAtMetadata]
        public DateTimeOffset WhenCreated { get; set; }

        [CorrelationIdMetadata]
        public string? Correlation { get; set; }
    }

    [Fact]
    public void dsl_enables_optin_columns()
    {
        var expr = new DocumentMappingExpression<PlainDoc>();
        expr.Metadata(m =>
        {
            m.CorrelationId.Enabled = true;
            m.CausationId.Enabled = true;
            m.LastModifiedBy.Enabled = true;
            m.Headers.Enabled = true;
        });

        expr.MetadataConfig.CorrelationId.Enabled.ShouldBeTrue();
        expr.MetadataConfig.CausationId.Enabled.ShouldBeTrue();
        expr.MetadataConfig.LastModifiedBy.Enabled.ShouldBeTrue();
        expr.MetadataConfig.Headers.Enabled.ShouldBeTrue();
    }

    [Fact]
    public void dsl_mapto_resolves_member_and_enables_column()
    {
        var expr = new DocumentMappingExpression<MappedDoc>();
        expr.Metadata(m =>
        {
            m.CreatedAt.MapTo(x => x.CreatedDate);
            m.LastModifiedBy.MapTo(x => x.ModifiedBy);
        });

        expr.MetadataConfig.CreatedAt.Member!.Name.ShouldBe(nameof(MappedDoc.CreatedDate));
        expr.MetadataConfig.LastModifiedBy.Member!.Name.ShouldBe(nameof(MappedDoc.ModifiedBy));
        expr.MetadataConfig.LastModifiedBy.Enabled.ShouldBeTrue(); // MapTo implies Enabled
    }

    [Fact]
    public void attributes_enable_and_map_columns()
    {
        var mapping = new DocumentMapping(typeof(AttributedDoc), new StoreOptions());

        mapping.Metadata.LastModifiedBy.Enabled.ShouldBeTrue();
        mapping.Metadata.LastModifiedBy.Member!.Name.ShouldBe(nameof(AttributedDoc.ModifiedByUser));

        mapping.Metadata.CreatedAt.Member!.Name.ShouldBe(nameof(AttributedDoc.WhenCreated));

        mapping.Metadata.CorrelationId.Enabled.ShouldBeTrue();
        mapping.Metadata.CorrelationId.Member!.Name.ShouldBe(nameof(AttributedDoc.Correlation));
    }

    [Fact]
    public void optional_columns_are_enabled_when_the_store_says_so()
    {
        var options = new StoreOptions();
        options.Policies.EnableDocumentMetadataByDefault = true;
        var mapping = new DocumentMapping(typeof(PlainDoc), options);

        mapping.Metadata.CorrelationId.Enabled.ShouldBeTrue();
        mapping.Metadata.CausationId.Enabled.ShouldBeTrue();
        mapping.Metadata.LastModifiedBy.Enabled.ShouldBeTrue();
        mapping.Metadata.Headers.Enabled.ShouldBeTrue();
    }

    // Off by default, as in Polecat 5 (#1134, marten#6144); the 6.0 alphas turned them on (#1032)
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void optional_columns_disabled_by_default(bool restoreV5Defaults)
    {
        var options = new StoreOptions();
        if (restoreV5Defaults) options.RestoreV5Defaults();

        var mapping = new DocumentMapping(typeof(PlainDoc), options);

        mapping.Metadata.CorrelationId.Enabled.ShouldBeFalse();
        mapping.Metadata.CausationId.Enabled.ShouldBeFalse();
        mapping.Metadata.LastModifiedBy.Enabled.ShouldBeFalse();
        mapping.Metadata.Headers.Enabled.ShouldBeFalse();
    }

    [Fact]
    public void dsl_config_merges_onto_mapping_through_the_store()
    {
        using var store = DocumentStore.For(opts =>
        {
            opts.ConnectionString = ConnectionSource.ConnectionString;
            opts.Schema.For<MappedDoc>().Metadata(m =>
            {
                m.CorrelationId.Enabled = true;
                m.LastModifiedBy.MapTo(x => x.ModifiedBy);
            });
        });

        var mapping = store.GetProvider(typeof(MappedDoc)).Mapping;

        mapping.Metadata.CorrelationId.Enabled.ShouldBeTrue();
        mapping.Metadata.LastModifiedBy.Enabled.ShouldBeTrue();
        mapping.Metadata.LastModifiedBy.Member!.Name.ShouldBe(nameof(MappedDoc.ModifiedBy));
    }
}
