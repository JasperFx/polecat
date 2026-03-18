using System.Data.Common;
using JasperFx;
using Polecat.Metadata;
using Polecat.Serialization;
using Polecat.Storage;

namespace Polecat.Linq.Selectors;

/// <summary>
///     Reads the JSON 'data' column and deserializes it to T.
///     Optionally syncs version/revision properties from additional columns.
///     Supports polymorphic deserialization for document hierarchies via doc_type column.
/// </summary>
internal class DeserializingSelector<T> where T : class
{
    private readonly ISerializer _serializer;
    private readonly bool _syncRevision;
    private readonly bool _syncGuidVersion;
    private readonly DocumentMapping? _mapping;

    public DeserializingSelector(ISerializer serializer)
        : this(serializer, false, false, null)
    {
    }

    public DeserializingSelector(ISerializer serializer, bool syncRevision, bool syncGuidVersion)
        : this(serializer, syncRevision, syncGuidVersion, null)
    {
    }

    public DeserializingSelector(ISerializer serializer, bool syncRevision, bool syncGuidVersion,
        DocumentMapping? mapping)
    {
        _serializer = serializer;
        _syncRevision = syncRevision;
        _syncGuidVersion = syncGuidVersion;
        _mapping = mapping;
    }

    public T Resolve(DbDataReader reader)
    {
        var json = reader.GetString(0);
        T doc;

        if (_mapping != null && _mapping.IsHierarchy())
        {
            // doc_type is the last column in the select list
            var docTypeIndex = 1; // after data
            if (_syncRevision) docTypeIndex++;
            if (_syncGuidVersion) docTypeIndex++;
            var docType = reader.GetString(docTypeIndex);
            var resolvedType = _mapping.TypeFor(docType);
            doc = (T)_serializer.FromJson(resolvedType, json);
        }
        else
        {
            doc = _serializer.FromJson<T>(json);
        }

        if (_syncRevision && doc is IRevisioned revisioned)
        {
            revisioned.Version = reader.GetInt32(1); // version column at index 1
        }

        if (_syncGuidVersion && doc is IVersioned versioned)
        {
            // guid_version is at index 2 when syncGuidVersion, since select is "data, version, guid_version"
            versioned.Version = reader.GetGuid(2);
        }

        return doc;
    }
}
