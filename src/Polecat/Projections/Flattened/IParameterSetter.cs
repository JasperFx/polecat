using JasperFx.Events;
using Microsoft.Data.SqlClient;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Extracts a value from an event and sets it on a SqlParameter.
/// </summary>
internal interface IParameterSetter
{
    void SetValue(SqlParameter parameter, IEvent source);
}

/// <summary>
///     Extracts a value from the event data object using a compiled lambda.
/// </summary>
internal class EventDataParameterSetter<TEvent, TValue> : IParameterSetter
{
    private readonly Func<TEvent, TValue> _accessor;

    public EventDataParameterSetter(Func<TEvent, TValue> accessor)
    {
        _accessor = accessor;
    }

    public void SetValue(SqlParameter parameter, IEvent source)
    {
        var value = _accessor((TEvent)source.Data);
        parameter.Value = value is not null ? (object)value : DBNull.Value;
    }
}

/// <summary>
///     Extracts the stream ID (Guid) from an IEvent for use as the primary key parameter.
/// </summary>
internal class StreamIdParameterSetter : IParameterSetter
{
    public void SetValue(SqlParameter parameter, IEvent source)
    {
        parameter.Value = source.StreamId;
    }
}

/// <summary>
///     Extracts the stream key (string) from an IEvent for use as the primary key parameter.
/// </summary>
internal class StreamKeyParameterSetter : IParameterSetter
{
    public void SetValue(SqlParameter parameter, IEvent source)
    {
        parameter.Value = (object?)source.StreamKey ?? DBNull.Value;
    }
}
