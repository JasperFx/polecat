using System.Linq.Expressions;

namespace Polecat.Patching;

/// <summary>
///     Fluent API for modifying JSON properties in-place using SQL Server's JSON_MODIFY(),
///     without loading/deserializing the full document.
/// </summary>
public interface IPatchExpression<T>
{
    /// <summary>
    ///     Set a single field or property value by name.
    /// </summary>
    IPatchExpression<T> Set<TValue>(string name, TValue value);

    /// <summary>
    ///     Set a single field or property value by name relative to a parent path.
    /// </summary>
    IPatchExpression<T> Set<TParent, TValue>(string name, Expression<Func<T, TParent>> expression, TValue value);

    /// <summary>
    ///     Set a single field or property value by expression.
    /// </summary>
    IPatchExpression<T> Set<TValue>(Expression<Func<T, TValue>> expression, TValue value);

    /// <summary>
    ///     Copy a single field or property value to one or more destinations.
    /// </summary>
    IPatchExpression<T> Duplicate<TElement>(Expression<Func<T, TElement>> expression,
        params Expression<Func<T, TElement>>[] destinations);

    /// <summary>
    ///     Increment an int field or property.
    /// </summary>
    IPatchExpression<T> Increment(Expression<Func<T, int>> expression, int increment = 1);

    /// <summary>
    ///     Increment a long field or property.
    /// </summary>
    IPatchExpression<T> Increment(Expression<Func<T, long>> expression, long increment = 1);

    /// <summary>
    ///     Increment a double field or property.
    /// </summary>
    IPatchExpression<T> Increment(Expression<Func<T, double>> expression, double increment = 1);

    /// <summary>
    ///     Increment a float field or property.
    /// </summary>
    IPatchExpression<T> Increment(Expression<Func<T, float>> expression, float increment = 1);

    /// <summary>
    ///     Increment a decimal field or property.
    /// </summary>
    IPatchExpression<T> Increment(Expression<Func<T, decimal>> expression, decimal increment = 1);

    /// <summary>
    ///     Append an element to the end of a child collection.
    /// </summary>
    IPatchExpression<T> Append<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression, TElement element);

    /// <summary>
    ///     Append an element with the specified key to a child dictionary.
    /// </summary>
    IPatchExpression<T> Append<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression, string key,
        TElement element);

    /// <summary>
    ///     Append an element to the end of a child collection if it does not already exist.
    /// </summary>
    IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element);

    /// <summary>
    ///     Append an element with the specified key to a child dictionary if the key does not already exist.
    /// </summary>
    IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression,
        string key, TElement element);

    /// <summary>
    ///     Insert an element at the designated index into a child collection.
    ///     If index is null, appends to the end.
    /// </summary>
    IPatchExpression<T> Insert<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression, TElement element,
        int? index = null);

    /// <summary>
    ///     Insert an element if it does not already exist at the designated index into a child collection.
    /// </summary>
    IPatchExpression<T> InsertIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, int? index = null);

    /// <summary>
    ///     Remove an element from a child collection.
    /// </summary>
    IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression, TElement element,
        RemoveAction action = RemoveAction.RemoveFirst);

    /// <summary>
    ///     Remove an element with the specified key from a child dictionary.
    /// </summary>
    IPatchExpression<T> Remove<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression, string key);

    /// <summary>
    ///     Rename a property or field in the persisted JSON document.
    /// </summary>
    IPatchExpression<T> Rename(string oldName, Expression<Func<T, object>> expression);

    /// <summary>
    ///     Delete a property by name from the persisted JSON data.
    /// </summary>
    IPatchExpression<T> Delete(string name);

    /// <summary>
    ///     Delete a property by name relative to a parent path.
    /// </summary>
    IPatchExpression<T> Delete<TParent>(string name, Expression<Func<T, TParent>> expression);

    /// <summary>
    ///     Delete a property or field by expression.
    /// </summary>
    IPatchExpression<T> Delete<TElement>(Expression<Func<T, TElement>> expression);
}
