using JasperFx.Core.Reflection;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
using Polecat.Storage;

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
    // A strong-typed-id wrapper has no SqlClient mapping, so the inner value is what goes on the
    // parameter — matching the inner-typed column StatementMap builds for it. Resolved once per
    // closed generic; null for every ordinary value. See marten#4290.
    private static readonly ValueTypeInfo? Wrapper =
        ValueTypes.TryResolve(typeof(TValue), allowReferenceTypes: true);

    private readonly Func<TEvent, TValue> _accessor;

    public EventDataParameterSetter(Func<TEvent, TValue> accessor)
    {
        _accessor = accessor;
    }

    public void SetValue(SqlParameter parameter, IEvent source)
    {
        var value = _accessor((TEvent)source.Data);
        if (value is null)
        {
            parameter.Value = DBNull.Value;
            return;
        }

        parameter.Value = Wrapper != null
            ? Wrapper.ValueProperty.GetValue(value) ?? (object)DBNull.Value
            : value;
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
