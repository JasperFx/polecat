using Polecat.Metadata;

namespace Polecat;

/// <summary>
///     Configurable policies applied to document types.
/// </summary>
public class StorePolicies
{
    private bool _allDocumentsSoftDeleted;
    private readonly HashSet<Type> _softDeletedTypes = new();

    /// <summary>
    ///     Enable soft deletes for all document types.
    /// </summary>
    public void AllDocumentsSoftDeleted()
    {
        _allDocumentsSoftDeleted = true;
    }

    /// <summary>
    ///     Enable soft deletes for a specific document type.
    /// </summary>
    public void ForDocument<T>(Action<DocumentPolicy> configure)
    {
        var policy = new DocumentPolicy();
        configure(policy);
        if (policy.SoftDeleted)
        {
            _softDeletedTypes.Add(typeof(T));
        }
    }

    internal bool IsSoftDeleted(Type documentType)
    {
        return _allDocumentsSoftDeleted || _softDeletedTypes.Contains(documentType);
    }
}

/// <summary>
///     Per-document-type policy configuration.
/// </summary>
public class DocumentPolicy
{
    /// <summary>
    ///     Enable soft deletes for this document type.
    /// </summary>
    public bool SoftDeleted { get; set; }
}
