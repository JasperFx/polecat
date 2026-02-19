using System.Data.Common;
using JasperFx;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Linq.Selectors;

/// <summary>
///     Reads the JSON 'data' column and deserializes it to T.
///     Optionally syncs version/revision properties from additional columns.
/// </summary>
internal class DeserializingSelector<T> where T : class
{
    private readonly ISerializer _serializer;
    private readonly bool _syncRevision;
    private readonly bool _syncGuidVersion;

    public DeserializingSelector(ISerializer serializer)
        : this(serializer, false, false)
    {
    }

    public DeserializingSelector(ISerializer serializer, bool syncRevision, bool syncGuidVersion)
    {
        _serializer = serializer;
        _syncRevision = syncRevision;
        _syncGuidVersion = syncGuidVersion;
    }

    public T Resolve(DbDataReader reader)
    {
        var json = reader.GetString(0);
        var doc = _serializer.FromJson<T>(json);

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
