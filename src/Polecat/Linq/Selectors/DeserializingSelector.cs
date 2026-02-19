using System.Data.Common;
using Polecat.Serialization;

namespace Polecat.Linq.Selectors;

/// <summary>
///     Reads the JSON 'data' column and deserializes it to T.
/// </summary>
internal class DeserializingSelector<T> where T : class
{
    private readonly ISerializer _serializer;

    public DeserializingSelector(ISerializer serializer)
    {
        _serializer = serializer;
    }

    public T Resolve(DbDataReader reader)
    {
        var json = reader.GetString(0);
        return _serializer.FromJson<T>(json);
    }
}
