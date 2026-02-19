using System.Linq.Expressions;
using Polecat.Internal;

namespace Polecat.Patching;

/// <summary>
///     Extension methods for patching documents via IDocumentOperations.
/// </summary>
public static class PatchingExtensions
{
    /// <summary>
    ///     Patch a single document of type T with the given Guid id.
    /// </summary>
    public static IPatchExpression<T> Patch<T>(this IDocumentOperations operations, Guid id) where T : notnull
    {
        var session = (DocumentSessionBase)operations;
        return new PatchExpression<T>(id, session.TenantId, session);
    }

    /// <summary>
    ///     Patch a single document of type T with the given string id.
    /// </summary>
    public static IPatchExpression<T> Patch<T>(this IDocumentOperations operations, string id) where T : notnull
    {
        var session = (DocumentSessionBase)operations;
        return new PatchExpression<T>(id, session.TenantId, session);
    }

    /// <summary>
    ///     Patch a single document of type T with the given int id.
    /// </summary>
    public static IPatchExpression<T> Patch<T>(this IDocumentOperations operations, int id) where T : notnull
    {
        var session = (DocumentSessionBase)operations;
        return new PatchExpression<T>(id, session.TenantId, session);
    }

    /// <summary>
    ///     Patch a single document of type T with the given long id.
    /// </summary>
    public static IPatchExpression<T> Patch<T>(this IDocumentOperations operations, long id) where T : notnull
    {
        var session = (DocumentSessionBase)operations;
        return new PatchExpression<T>(id, session.TenantId, session);
    }

    /// <summary>
    ///     Patch all documents of type T matching the given filter expression.
    /// </summary>
    public static IPatchExpression<T> Patch<T>(this IDocumentOperations operations,
        Expression<Func<T, bool>> filter) where T : notnull
    {
        var session = (DocumentSessionBase)operations;
        return new PatchExpression<T>(filter, session);
    }
}
