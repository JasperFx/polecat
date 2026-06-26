using System.Reflection;
using Polecat.Storage;
using Polecat.Storage.Metadata;

namespace Polecat.Attributes;

/// <summary>
///     #243: base class for attributes that map a stored document-metadata value onto a document
///     member (and enable the corresponding column). Mirrors Marten's metadata attributes
///     (<c>[CorrelationIdMetadata]</c>, <c>[LastModifiedMetadata]</c>, …).
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public abstract class MetadataAttribute : Attribute
{
    /// <summary>
    ///     Select the metadata column this member maps to, then assign the member and enable it.
    /// </summary>
    internal abstract void Apply(DocumentMetadataConfig config, MemberInfo member);

    protected static void Map(MetadataColumn column, MemberInfo member)
    {
        column.Member = member;
        column.Enabled = true;
    }
}

/// <summary>Maps the stored correlation id onto this member (and enables the <c>correlation_id</c> column).</summary>
public sealed class CorrelationIdMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.CorrelationId, member);
}

/// <summary>Maps the stored causation id onto this member (and enables the <c>causation_id</c> column).</summary>
public sealed class CausationIdMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.CausationId, member);
}

/// <summary>Maps the stored user / last-modified-by onto this member (and enables the <c>last_modified_by</c> column).</summary>
public sealed class LastModifiedByMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.LastModifiedBy, member);
}

/// <summary>Maps the stored headers onto this member (and enables the <c>headers</c> column).</summary>
public sealed class HeadersMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.Headers, member);
}

/// <summary>Maps the <c>created_at</c> timestamp onto this member.</summary>
public sealed class CreatedAtMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.CreatedAt, member);
}

/// <summary>Maps the <c>last_modified</c> timestamp onto this member.</summary>
public sealed class LastModifiedMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.LastModified, member);
}

/// <summary>Maps the <c>version</c> onto this member.</summary>
public sealed class VersionMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.Version, member);
}

/// <summary>Maps the <c>tenant_id</c> onto this member.</summary>
public sealed class TenantIdMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.TenantId, member);
}

/// <summary>Maps the <c>is_deleted</c> soft-delete flag onto this member.</summary>
public sealed class IsSoftDeletedMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.IsSoftDeleted, member);
}

/// <summary>Maps the <c>dotnet_type</c> discriminator onto this member.</summary>
public sealed class DotNetTypeMetadataAttribute : MetadataAttribute
{
    internal override void Apply(DocumentMetadataConfig config, MemberInfo member) => Map(config.DotNetType, member);
}
