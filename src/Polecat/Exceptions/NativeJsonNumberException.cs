namespace Polecat.Exceptions;

/// <summary>
///     Thrown instead of saving a number that SQL Server's native <c>json</c> type would change. The
///     type keeps 38 digits, and given more it rounds to ten decimal places with no error, so the
///     alternative to this exception is a stored value that is quietly wrong.
/// </summary>
public class NativeJsonNumberException : Exception
{
    public NativeJsonNumberException(string number, string? member)
        : base(
            $"SQL Server's native json type cannot hold the number {number}{(member == null ? "" : $" in '{member}'")} " +
            "exactly, and would store a different value without an error. It keeps at most 38 digits, counting the " +
            "zeros after the decimal point of a small number, so nothing from 1E+38 upwards fits and neither does a " +
            "number below 1E-38. To keep this value, either write the member as a JSON string with " +
            "[JsonNumberHandling(JsonNumberHandling.WriteAsString | JsonNumberHandling.AllowReadingFromString)], or " +
            $"keep JSON in nvarchar(max) with StoreOptions.{nameof(StoreOptions.UseNativeJsonType)} = false.")
    {
        Number = number;
        Member = member;
    }

    /// <summary>
    ///     The number, as the serializer wrote it.
    /// </summary>
    public string Number { get; }

    /// <summary>
    ///     The JSON key the number was found under, or the nearest one before it for a number in an
    ///     array. Null if there isn't one.
    /// </summary>
    public string? Member { get; }
}
