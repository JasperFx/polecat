using System.Diagnostics.CodeAnalysis;
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
        if (targetType.IsEnum) return Enum.ToObject(targetType, value);

        var wrapper = ValueTypes.TryResolve(targetType, allowReferenceTypes: true);
        if (wrapper != null)
        {
            var inner = System.Convert.ChangeType(value, wrapper.SimpleType);
            return wrapper.Ctor != null
                ? wrapper.Ctor.Invoke([inner])
                : wrapper.Builder!.Invoke(null, [inner])!;
        }

        return System.Convert.ChangeType(value, targetType);
    }
}
