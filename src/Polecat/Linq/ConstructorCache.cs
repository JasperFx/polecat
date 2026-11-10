using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Polecat.Linq;

/// <summary>
///     Resolves the public constructor of a closed handler / selector / queryable type once and reuses
///     it, instead of <c>Activator.CreateInstance(Type, object[])</c> running its binder over the
///     constructor set on every query (#802). The lookup is by argument count, which is unambiguous for
///     every type this is used with. The instantiation itself stays reflective, so call sites keep
///     their existing trimming / AOT annotations.
/// </summary>
internal static class ConstructorCache
{
    private static readonly ConcurrentDictionary<(Type, int), ConstructorInfo> Constructors = new();

    [RequiresUnreferencedCode("Reflects over the public constructors of the closed type.")]
    public static object Create(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type type, params object?[] args)
    {
        var ctor = Constructors.GetOrAdd((type, args.Length), static key =>
        {
            foreach (var candidate in key.Item1.GetConstructors())
            {
                if (candidate.GetParameters().Length == key.Item2) return candidate;
            }

            throw new MissingMethodException(
                $"{key.Item1.FullName} has no public constructor taking {key.Item2} argument(s).");
        });

        return ctor.Invoke(args);
    }
}
