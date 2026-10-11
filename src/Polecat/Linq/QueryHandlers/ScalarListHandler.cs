using System.Collections.Concurrent;
using System.Data.Common;

namespace Polecat.Linq.QueryHandlers;

/// <summary>
///     Reads a list of scalar values from a query (e.g., Select(x => x.Name)).
/// </summary>
internal class ScalarListHandler<T> : IQueryHandler<IReadOnlyList<T>>
{
    public async Task<IReadOnlyList<T>> HandleAsync(DbDataReader reader, CancellationToken token)
    {
        var list = new List<T>();
        while (await reader.ReadAsync(token))
        {
            if (await reader.IsDBNullAsync(0, token))
            {
                list.Add(default!);
            }
            else
            {
                var value = reader.GetValue(0);
                var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                list.Add((T)ScalarConversion.Convert(value, targetType));
            }
        }

        return list;
    }
}

/// <summary>
///     polecat#743: the <see cref="ScalarListHandler{T}" /> instantiations that a STATIC generic context
///     has rooted, so a value-type scalar projection works under Native AOT.
/// </summary>
/// <remarks>
///     <para>
///         The providers close <c>ScalarListHandler&lt;&gt;</c> over <c>TResult</c>'s element type with
///         <c>MakeGenericType</c>. For a REFERENCE type that is fine — every reference instantiation
///         shares one canonical body — but <c>Select(x =&gt; x.Difficulty)</c> or
///         <c>Select(x =&gt; x.Id)</c> over a <see cref="Guid" /> needs <c>ScalarListHandler&lt;Difficulty&gt;</c>
///         itself, which ILC never compiled, and threw <see cref="NotSupportedException" /> ("missing native
///         code"). Found by <c>Polecat.AotRuntimeSmoke</c>.
///     </para>
///     <para>
///         The element type is not a runtime value at the CALL SITE: <c>ToListAsync&lt;T&gt;</c> has it as a
///         type parameter. So <c>ToListAsync&lt;T&gt;</c> reads <see cref="ScalarListHandlerRoot{T}.Rooted" />,
///         whose initializer names <c>new ScalarListHandler&lt;T&gt;()</c> — which makes ILC compile that
///         instantiation as part of compiling <c>ToListAsync&lt;Difficulty&gt;</c>. The same principle as
///         #733's fix: close the generic where the type is a compile-time fact, decide at runtime only which
///         one to use. <c>MakeGenericType</c> stays as the fallback for paths that never went through a
///         generic call site, which under a JIT is every path and still works.
///     </para>
/// </remarks>
internal static class ScalarListHandlers
{
    private static readonly ConcurrentDictionary<Type, Func<IQueryHandler>> _factories = new();

    internal static bool Add(Type scalarType, Func<IQueryHandler> factory)
    {
        _factories.TryAdd(scalarType, factory);
        return true;
    }

    /// <summary>A handler for <paramref name="scalarType" /> if a generic call site rooted one, else null.</summary>
    internal static IQueryHandler? TryCreate(Type scalarType)
        => _factories.TryGetValue(scalarType, out var factory) ? factory() : null;
}

/// <summary>
///     Reading <see cref="Rooted" /> from a method generic in <typeparamref name="T" /> roots
///     <see cref="ScalarListHandler{T}" /> for it. See <see cref="ScalarListHandlers" />.
/// </summary>
internal static class ScalarListHandlerRoot<T>
{
    // A static initializer, so the registration runs once per T and the read on the hot path is a field load.
    internal static readonly bool Rooted =
        ScalarListHandlers.Add(typeof(T), static () => new ScalarListHandler<T>());
}
