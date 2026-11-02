using System.Buffers;
using System.Text;
using System.Text.Json;
using Polecat.Exceptions;

namespace Polecat.Serialization;

/// <summary>
///     #879: makes a JSON document safe to hand to SQL Server's native <c>json</c> type, which changes
///     numbers rather than storing what it was given.
/// </summary>
/// <remarks>
///     <para>
///         ⚠️ <b>What the type does, measured on SQL Server 2025 RTM-CU5.</b> A number with an exponent
///         is parsed as a double and then stored as a <c>numeric(38,10)</c>, in the stored value and
///         not just in its text. So <c>1.2345678901234567E-05</c> is stored as <c>0.0000123457</c>,
///         <c>1.5E-30</c> as <c>0.0000000000</c>, <c>1E+28</c> as
///         <c>9999999999999999583119736832.0000000000</c>, and <c>1E+29</c> and up are refused with
///         Msg 1007. System.Text.Json writes an exponent for any double under 1E-4 or from about 1E+16,
///         so this is nowhere near as exotic as those examples make it look.
///     </para>
///     <para>
///         A number with no exponent is stored as it is written, trailing zeros included, as long as it
///         has at most 38 digits, counting the zeros after the point of a number below 1. Past that a
///         fraction is rounded to ten places with no error, and an integer is refused.
///     </para>
///     <para>
///         So the same value written out in full is stored exactly, provided it fits in those 38
///         digits, and what doesn't fit is refused here rather than left for the server to change. That
///         rule predicted the result for all of 6,000 values drawn from every exponent, each put
///         through a real column. <c>nvarchar(max)</c> stores text and has none of this, which is why
///         nothing here runs for that storage.
///     </para>
///     <para>
///         This works on the JSON rather than through a <c>JsonConverter</c> for <see cref="double" />
///         because only the JSON sees all of it. A converter doesn't apply to a number in a
///         <see cref="JsonElement" />, a member with a converter of its own or a serializer that isn't
///         ours, each of which would still lose the value in silence, and it would override
///         <see cref="System.Text.Json.Serialization.JsonNumberHandlingAttribute" /> on a member, which
///         is the way to keep a number that doesn't fit.
///     </para>
/// </remarks>
internal static class NativeJsonNumbers
{
    /// <summary>
    ///     What <c>numeric</c> holds, which is what the <c>json</c> type stores a number in.
    /// </summary>
    internal const int MaxDigits = 38;

    private static readonly SearchValues<char> StartOfStringOrNumber = SearchValues.Create("\"-0123456789");
    private static readonly SearchValues<char> InNumber = SearchValues.Create("+-.0123456789Ee");

    /// <summary>
    ///     <paramref name="json" /> with every number written so the <c>json</c> type keeps it exactly.
    ///     The same string comes back when there's nothing to change, and that is nearly always.
    /// </summary>
    /// <exception cref="NativeJsonNumberException">
    ///     For a number the <c>json</c> type has no way of holding.
    /// </exception>
    public static string ForStorage(string json)
    {
        var text = json.AsSpan();
        var position = 0;
        var copiedTo = 0;
        StringBuilder? rewritten = null;

        while (position < text.Length)
        {
            var next = text[position..].IndexOfAny(StartOfStringOrNumber);
            if (next < 0)
            {
                break;
            }

            position += next;
            if (text[position] == '"')
            {
                position = PastString(text, position);
                continue;
            }

            var length = text[position..].IndexOfAnyExcept(InNumber);
            if (length < 0)
            {
                length = text.Length - position;
            }

            var number = text.Slice(position, length);

            // Too short to be over the limit and with no exponent, which is nearly every number there is
            if (length > MaxDigits || number.IndexOfAny('e', 'E') >= 0)
            {
                var plain = WrittenOut(number, json, position);
                if (plain != null)
                {
                    rewritten ??= new StringBuilder(json.Length + 32);
                    rewritten.Append(text[copiedTo..position]).Append(plain);
                    copiedTo = position + length;
                }
            }

            position += length;
        }

        return rewritten == null ? json : rewritten.Append(text[copiedTo..]).ToString();
    }

    private static int PastString(ReadOnlySpan<char> text, int opening)
    {
        var position = opening + 1;
        while (position < text.Length)
        {
            var next = text[position..].IndexOfAny('"', '\\');
            if (next < 0)
            {
                break;
            }

            position += next;
            if (text[position] == '"')
            {
                return position + 1;
            }

            // Past the escape as well, which may well be a quote
            position += 2;
        }

        return text.Length;
    }

    /// <summary>
    ///     The number with no exponent and the digits it needs, or null if it is fine as it is.
    /// </summary>
    private static string? WrittenOut(ReadOnlySpan<char> number, string json, int position)
    {
        var unsigned = number[0] == '-' ? number[1..] : number;

        var exponentAt = unsigned.IndexOfAny('e', 'E');
        var mantissa = exponentAt < 0 ? unsigned : unsigned[..exponentAt];

        var exponent = 0L;
        if (exponentAt >= 0 && !TryReadExponent(unsigned[(exponentAt + 1)..], out exponent))
        {
            // Not a number, so it isn't valid JSON either. Refusing that is the server's business.
            return null;
        }

        var point = mantissa.IndexOf('.');
        var whole = point < 0 ? mantissa : mantissa[..point];
        var fraction = point < 0 ? default : mantissa[(point + 1)..];

        if (exponentAt < 0 && DigitsOf(whole, fraction) <= MaxDigits)
        {
            return null;
        }

        var digits = string.Concat(whole, fraction).AsSpan();

        // Where the decimal point sits, counted from the start of digits
        var pointAt = whole.Length + exponent;

        var significant = digits.TrimStart('0');
        pointAt -= digits.Length - significant.Length;
        significant = significant.TrimEnd('0');

        if (significant.IsEmpty)
        {
            return "0";
        }

        var needed = pointAt <= 0 ? significant.Length - pointAt : Math.Max(pointAt, significant.Length);
        if (needed > MaxDigits)
        {
            throw new NativeJsonNumberException(number.ToString(), MemberBefore(json, position));
        }

        var count = (int)pointAt;
        var builder = new StringBuilder(MaxDigits + 3);
        if (number[0] == '-')
        {
            builder.Append('-');
        }

        if (count <= 0)
        {
            builder.Append("0.").Append('0', -count).Append(significant);
        }
        else if (count >= significant.Length)
        {
            builder.Append(significant).Append('0', count - significant.Length);
        }
        else
        {
            builder.Append(significant[..count]).Append('.').Append(significant[count..]);
        }

        return builder.ToString();
    }

    private static int DigitsOf(ReadOnlySpan<char> whole, ReadOnlySpan<char> fraction)
        => (whole is "0" ? 0 : whole.Length) + fraction.Length;

    private static bool TryReadExponent(ReadOnlySpan<char> text, out long exponent)
    {
        var negative = !text.IsEmpty && text[0] == '-';
        var digits = !text.IsEmpty && text[0] is '-' or '+' ? text[1..] : text;

        exponent = 0;
        if (digits.IsEmpty)
        {
            return false;
        }

        foreach (var digit in digits)
        {
            if (digit is < '0' or > '9')
            {
                return false;
            }

            // Far past anything that could be held, and still safe to do arithmetic on
            exponent = Math.Min(exponent * 10 + (digit - '0'), int.MaxValue);
        }

        exponent = negative ? -exponent : exponent;
        return true;
    }

    /// <summary>
    ///     The last key before <paramref name="position" />. Only for a message, so it is worked out
    ///     after the fact rather than kept track of on every document.
    /// </summary>
    private static string? MemberBefore(string json, int position)
    {
        var text = json.AsSpan(0, position);
        var cursor = 0;
        var keyStart = -1;
        var keyEnd = -1;

        while (cursor < text.Length)
        {
            var next = text[cursor..].IndexOf('"');
            if (next < 0)
            {
                break;
            }

            var start = cursor + next;
            cursor = PastString(text, start);

            var rest = text[cursor..].TrimStart();
            if (!rest.IsEmpty && rest[0] == ':')
            {
                keyStart = start;
                keyEnd = cursor;
            }
        }

        if (keyStart < 0)
        {
            return null;
        }

        using var key = JsonDocument.Parse(json.AsMemory(keyStart, keyEnd - keyStart));
        return key.RootElement.GetString();
    }
}
