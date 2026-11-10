using JasperFx;
using System.Data;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;

namespace Polecat.Linq.CursorPaging;

/// <summary>
///     How one ordering key is stored, which decides how its cursor slot is read and how the value is
///     bound on the next request.
/// </summary>
/// <remarks>
///     <para>
///         <b>Keyed on the type the locator yields, not the member's CLR type.</b> A cursor slot was
///         read from the database and is compared back against the same locator, so the CLR type is
///         the wrong question: an enum is an <c>int</c> or a name, a <see cref="bool" /> is the text
///         <c>'true'</c>, a strong-typed id is its inner scalar, and an <c>Index(...)</c> can override
///         the type outright. Asking the CLR type is what made every one of those pass page 1 and
///         throw on page 2 (#838). <see cref="IQueryableMember.LocatorSqlType" /> is the same answer
///         #710 relies on when it types an <c>OPENJSON</c> column.
///     </para>
///     <para>
///         ⚠️ <b>The parameter has to carry the locator's exact type.</b> Left to infer, SqlClient binds
///         a <see cref="DateTime" /> as <c>datetime</c>, rounding it to 3ms, so a <c>datetime2</c> key
///         with sub-millisecond ticks seeks from a different instant than the row it came from. Rounding
///         down repeats a row forever, and rounding up skips some. Taking the type from the locator
///         rather than special-casing that one means an index that overrides a key's type is covered
///         too, and a string identity binds <c>varchar</c> against its <c>varchar</c> column (#363).
///     </para>
/// </remarks>
internal sealed class CursorKeyType
{
    private readonly SqlDbType _dbType;
    private readonly Type _readType;
    private readonly int? _size;
    private readonly byte? _precision;
    private readonly byte? _scale;

    private CursorKeyType(SqlDbType dbType, Type readType, int? size = null, byte? precision = null,
        byte? scale = null)
    {
        _dbType = dbType;
        _readType = readType;
        _size = size;
        _precision = precision;
        _scale = scale;
    }

    /// <summary>
    ///     The stored type of an ordering key, or a refusal of one that cannot seek. Called on every
    ///     request, the first included, so an unsupported key is refused before a page is handed out
    ///     rather than on the request after it.
    /// </summary>
    public static CursorKeyType For(IQueryableMember member)
    {
        if (member is not (IdMember or EnumMember or ValueTypeMember)
            && !SelectProjectionAnalyzer.IsScalar(member.MemberType))
        {
            throw Unsupported(member, "it is not a scalar, so it has no single value to seek from");
        }

        // A null type is a locator left uncast, so it yields the nvarchar JSON_VALUE returns: a string,
        // a bool, an enum stored by name, and anything else SqlTypeMap has no column type for.
        if (member.LocatorSqlType is not { } sqlType) return Text(SqlDbType.NVarChar);

        var open = sqlType.IndexOf('(');
        var name = (open < 0 ? sqlType : sqlType[..open]).Trim().ToLowerInvariant();
        var arguments = open < 0
            ? []
            : sqlType[(open + 1)..].TrimEnd(')', ' ').Split(',', StringSplitOptions.TrimEntries);

        return name switch
        {
            "int" => new CursorKeyType(SqlDbType.Int, typeof(int)),
            "bigint" => new CursorKeyType(SqlDbType.BigInt, typeof(long)),
            "smallint" => new CursorKeyType(SqlDbType.SmallInt, typeof(short)),
            "tinyint" => new CursorKeyType(SqlDbType.TinyInt, typeof(byte)),
            "bit" => new CursorKeyType(SqlDbType.Bit, typeof(bool)),
            "float" => new CursorKeyType(SqlDbType.Float, typeof(double)),
            "real" => new CursorKeyType(SqlDbType.Real, typeof(float)),
            "decimal" or "numeric" => new CursorKeyType(SqlDbType.Decimal, typeof(decimal),
                precision: Argument(arguments, 0, 18), scale: Argument(arguments, 1, 0)),
            "uniqueidentifier" => new CursorKeyType(SqlDbType.UniqueIdentifier, typeof(Guid)),
            "datetime2" => new CursorKeyType(SqlDbType.DateTime2, typeof(DateTime),
                scale: Argument(arguments, 0, 7)),
            "datetime" => new CursorKeyType(SqlDbType.DateTime, typeof(DateTime)),
            "smalldatetime" => new CursorKeyType(SqlDbType.SmallDateTime, typeof(DateTime)),
            "date" => new CursorKeyType(SqlDbType.Date, typeof(DateTime)),
            "datetimeoffset" => new CursorKeyType(SqlDbType.DateTimeOffset, typeof(DateTimeOffset),
                scale: Argument(arguments, 0, 7)),
            "time" => new CursorKeyType(SqlDbType.Time,
                typeof(TimeSpan),
                scale: Argument(arguments, 0, 7)),
            "nvarchar" or "nchar" => Text(SqlDbType.NVarChar, Length(arguments)),
            "varchar" or "char" => Text(SqlDbType.VarChar, Length(arguments)),
            _ => throw Unsupported(member, $"it is stored as {sqlType}")
        };
    }

    private static CursorKeyType Text(SqlDbType dbType, int? size = null)
        => new(dbType, typeof(string), size);

    private static byte Argument(string[] arguments, int index, byte fallback)
        => arguments.Length > index ? byte.Parse(arguments[index], CultureInfo.InvariantCulture) : fallback;

    private static int? Length(string[] arguments)
    {
        if (arguments.Length == 0) return null;

        return arguments[0].Equals("max", StringComparison.OrdinalIgnoreCase)
            ? -1
            : int.Parse(arguments[0], CultureInfo.InvariantCulture);
    }

    private static BadLinqExpressionException Unsupported(IQueryableMember member, string why)
        => new($"Keyset (cursor) pagination cannot order by a key of type '{member.MemberType.Name}': {why}. "
               + "Order by scalar members with the document identity as the terminal key.");

    /// <summary>
    ///     The value in a non-null slot. A slot of the wrong shape throws
    ///     <see cref="FormatException" />, <see cref="InvalidOperationException" /> or
    ///     <see cref="OverflowException" />, which the caller turns into a malformed cursor.
    /// </summary>
    /// <summary>
    ///     The CLR type a cursor slot for this key is read as, which is the type it is bound as.
    /// </summary>
    public Type ReadType => _readType;

    /// <summary>Gives a bound parameter the locator's exact type.</summary>
    public void ApplyTo(SqlParameter parameter)
    {
        parameter.SqlDbType = _dbType;
        if (_size.HasValue) parameter.Size = _size.Value;
        if (_precision.HasValue) parameter.Precision = _precision.Value;
        if (_scale.HasValue) parameter.Scale = _scale.Value;
    }
}
