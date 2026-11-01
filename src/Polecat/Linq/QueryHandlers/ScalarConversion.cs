using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using System.Text.Json.Serialization;
using JasperFx.Core.Reflection;
using Polecat.Storage;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Converts a raw column value to the CLR type a scalar <c>Select()</c> projection asked for.
///     Plain scalars go through <see cref="Convert.ChangeType(object, Type)" />; a strong-typed-id
///     wrapper (Vogen, StronglyTypedId, or a <c>record struct</c>) is rebuilt from its inner value,
///     since it implements neither <see cref="IConvertible" /> nor any SqlClient mapping.
/// </summary>
internal static class ScalarConversion
{
    [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
        Justification = "Reflectively invokes the wrapper's discovered constructor/factory. The wrapper type is the projection's result type, which flows in from caller code the trimmer can see.")]
    [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
        Justification = "Invokes an already-discovered ConstructorInfo/MethodInfo; no generic closing.")]
    internal static object Convert(object value, Type targetType)
    {
        if (targetType.IsEnum)
        {
            return value is string name ? ParseEnum(targetType, name) : Enum.ToObject(targetType, value);
        }

        // SqlClient reads `date` as DateTime and `time` as TimeSpan, and a TimeSpan member is stored
        // as text. Convert.ChangeType knows none of the three.
        if (targetType == typeof(DateOnly) && value is DateTime date) return DateOnly.FromDateTime(date);
        if (targetType == typeof(TimeOnly) && value is TimeSpan time) return TimeOnly.FromTimeSpan(time);
        if (targetType == typeof(TimeSpan) && value is string text)
        {
            return TimeSpan.Parse(text, CultureInfo.InvariantCulture);
        }

        var wrapper = ValueTypes.TryResolve(targetType, allowReferenceTypes: true);
        if (wrapper != null)
        {
            var inner = Convert(value, wrapper.SimpleType);
            return wrapper.Ctor != null
                ? wrapper.Ctor.Invoke([inner])
                : wrapper.Builder!.Invoke(null, [inner])!;
        }

        return System.Convert.ChangeType(value, targetType);
    }

    /// <summary>
    ///     An enum stored as text. The stored name went through the serializer's naming rules, so it
    ///     can be a <c>[JsonStringEnumMemberName]</c>, or the member's name in camel, snake or kebab case.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2070:UnrecognizedReflectionPattern",
        Justification = "Reads the public fields of an enum, which the trimmer keeps for an enum the document references. EnumMember reads the same attribute off the same fields.")]
    private static object ParseEnum(Type enumType, string stored)
    {
        foreach (var field in enumType.GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            if (field.GetCustomAttribute<JsonStringEnumMemberNameAttribute>()?.Name == stored)
            {
                return field.GetValue(null)!;
            }
        }

        var compact = stored.Replace("_", string.Empty).Replace("-", string.Empty);
        return Enum.Parse(enumType, compact, ignoreCase: true);
    }
}
