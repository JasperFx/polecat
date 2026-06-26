namespace Polecat.Storage.Metadata;

/// <summary>
///     #243: per-document metadata configuration. Mirrors Marten's <c>DocumentMetadataCollection</c>
///     — the set of metadata columns a document type can opt into and optionally map onto its own
///     members. Lives on <see cref="DocumentMapping" />; populated from metadata attributes and the
///     fluent <c>Schema.For&lt;T&gt;().Metadata(...)</c> DSL.
/// </summary>
public class DocumentMetadataConfig
{
    /// <summary>Stored correlation id (opt-in column, #241).</summary>
    public MetadataColumn CorrelationId { get; } = new("correlation_id");

    /// <summary>Stored causation id (opt-in column, #241).</summary>
    public MetadataColumn CausationId { get; } = new("causation_id");

    /// <summary>Stored user / last-modified-by (opt-in column, #241).</summary>
    public MetadataColumn LastModifiedBy { get; } = new("last_modified_by");

    /// <summary>Stored custom headers (opt-in column, #241).</summary>
    public MetadataColumn Headers { get; } = new("headers");

    /// <summary>The always-present <c>created_at</c> column; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn CreatedAt { get; } = new("created_at") { Enabled = true };

    /// <summary>The always-present <c>last_modified</c> column; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn LastModified { get; } = new("last_modified") { Enabled = true };

    /// <summary>The always-present <c>version</c> column; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn Version { get; } = new("version") { Enabled = true };

    /// <summary>The always-present <c>tenant_id</c> column; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn TenantId { get; } = new("tenant_id") { Enabled = true };

    /// <summary>The <c>is_deleted</c> soft-delete flag; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn IsSoftDeleted { get; } = new("is_deleted");

    /// <summary>The always-present <c>dotnet_type</c> column; <see cref="MetadataColumn.Member" /> maps it onto a document member.</summary>
    public MetadataColumn DotNetType { get; } = new("dotnet_type") { Enabled = true };

    /// <summary>
    ///     Every metadata column in a stable order.
    /// </summary>
    public IEnumerable<MetadataColumn> AllColumns()
    {
        yield return CorrelationId;
        yield return CausationId;
        yield return LastModifiedBy;
        yield return Headers;
        yield return CreatedAt;
        yield return LastModified;
        yield return Version;
        yield return TenantId;
        yield return IsSoftDeleted;
        yield return DotNetType;
    }

    /// <summary>
    ///     The opt-in columns Polecat actually adds to document tables (#241). The remaining columns
    ///     are either always present (<c>created_at</c>/<c>last_modified</c>/<c>version</c>/
    ///     <c>tenant_id</c>/<c>dotnet_type</c>) or governed by their own feature (<c>is_deleted</c>).
    /// </summary>
    public IEnumerable<MetadataColumn> OptInColumns()
    {
        yield return CorrelationId;
        yield return CausationId;
        yield return LastModifiedBy;
        yield return Headers;
    }

    /// <summary>
    ///     Merge another config into this one (the DSL config layered over attribute-derived config):
    ///     enables any column the other enabled and copies any member mapping it set.
    /// </summary>
    internal void MergeFrom(DocumentMetadataConfig other)
    {
        using var mine = AllColumns().GetEnumerator();
        using var theirs = other.AllColumns().GetEnumerator();
        while (mine.MoveNext() && theirs.MoveNext())
        {
            if (theirs.Current.Enabled) mine.Current.Enabled = true;
            if (theirs.Current.Member != null) mine.Current.Member = theirs.Current.Member;
        }
    }
}
