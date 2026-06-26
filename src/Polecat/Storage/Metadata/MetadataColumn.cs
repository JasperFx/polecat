using System.Reflection;

namespace Polecat.Storage.Metadata;

/// <summary>
///     #243: configuration for a single document metadata column. Mirrors Marten's
///     <c>MetadataColumn</c>. Carries whether the column is enabled (persisted) and an optional
///     document member the stored value is projected onto when a document is loaded.
/// </summary>
public class MetadataColumn
{
    public MetadataColumn(string name)
    {
        Name = name;
    }

    /// <summary>
    ///     The SQL column name (e.g. <c>correlation_id</c>).
    /// </summary>
    public string Name { get; }

    /// <summary>
    ///     Whether this metadata column is persisted for the document type.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    ///     Optional document member the stored value is mapped to/from. When set, loading a document
    ///     projects the column value onto this member.
    /// </summary>
    public MemberInfo? Member { get; set; }
}
