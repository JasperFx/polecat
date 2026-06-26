namespace Polecat.Metadata;

/// <summary>
///     #242: a read-only snapshot of a document's stored metadata, returned by
///     <see cref="IQuerySession.MetadataForAsync{T}(T, System.Threading.CancellationToken)" />.
///     Mirrors Marten's <c>DocumentMetadata</c> — lets callers inspect "who last touched this
///     document, and under what correlation/causation context" (plus version, timestamps, tenant,
///     and soft-delete state) without loading and reflecting over the document body.
/// </summary>
public class DocumentMetadata
{
    public DocumentMetadata(object id, long version, DateTimeOffset lastModified, DateTimeOffset createdAt,
        string tenantId)
    {
        Id = id;
        Version = version;
        LastModified = lastModified;
        CreatedAt = createdAt;
        TenantId = tenantId;
    }

    /// <summary>The document id.</summary>
    public object Id { get; }

    /// <summary>The numeric version/revision (1-based, incremented per write).</summary>
    public long Version { get; }

    /// <summary>When the row was last modified.</summary>
    public DateTimeOffset LastModified { get; }

    /// <summary>When the row was first created.</summary>
    public DateTimeOffset CreatedAt { get; }

    /// <summary>The owning tenant id.</summary>
    public string TenantId { get; }

    /// <summary>The stored .NET type name (<c>dotnet_type</c>).</summary>
    public string? DotNetType { get; init; }

    /// <summary>The hierarchy discriminator (<c>doc_type</c>), when the type is a hierarchy.</summary>
    public string? DocumentType { get; init; }

    /// <summary>The Guid optimistic-concurrency version (<c>guid_version</c>), when enabled.</summary>
    public Guid? GuidVersion { get; init; }

    /// <summary>Whether the document is soft-deleted, when the type uses soft deletes.</summary>
    public bool Deleted { get; init; }

    /// <summary>When the document was soft-deleted, if applicable.</summary>
    public DateTimeOffset? DeletedAt { get; init; }

    /// <summary>The stored correlation id, when the opt-in column is enabled (#241).</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The stored causation id, when the opt-in column is enabled (#241).</summary>
    public string? CausationId { get; init; }

    /// <summary>The stored user / last-modified-by, when the opt-in column is enabled (#241).</summary>
    public string? LastModifiedBy { get; init; }

    /// <summary>The stored headers, when the opt-in column is enabled (#241).</summary>
    public Dictionary<string, object>? Headers { get; init; }
}
