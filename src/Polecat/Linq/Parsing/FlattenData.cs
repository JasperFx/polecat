using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Polecat.Linq.Members;

namespace Polecat.Linq.Parsing;

/// <summary>
///     gh-1016: what a <c>SelectMany()</c> over a collection the documents hold made of the rows. See
///     <see cref="LinqQueryParser" />'s <c>HandleSelectMany</c>.
/// </summary>
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Closes FlattenedValue<> over the element type and builds lambdas over the caller's own types. The expression trees originate at the LINQ registration boundary where document types are preserved per the AOT publishing guide.")]
internal sealed class FlattenData
{
    /// <summary>Each element's position in its collection, which is the order LINQ hands them out in.</summary>
    internal const string Position = "CAST(__pc_elem.[key] AS int)";

    /// <summary>
    ///     Each element as JSON, to deserialize: <c>OPENJSON</c> hands a string out unquoted and unescaped, and
    ///     everything else as the JSON text it is.
    /// </summary>
    internal const string ElementJson =
        "CASE __pc_elem.[type] WHEN 1 THEN CONCAT(N'\"', STRING_ESCAPE(__pc_elem.[value], 'json'), N'\"') "
        + "WHEN 0 THEN N'null' ELSE __pc_elem.[value] END";

    private FlattenData(ParameterExpression parent, ParameterExpression element, Expression elementValue,
        Type elementType, bool isScalar)
    {
        Parent = parent;
        Element = element;
        ElementValue = elementValue;
        ElementType = elementType;
        IsScalar = isScalar;
    }

    /// <summary>The parent document, as the collection selector named it.</summary>
    public ParameterExpression Parent { get; }

    /// <summary>
    ///     The element: of the element type, or for a scalar a <see cref="FlattenedValue{T}" /> whose
    ///     <c>Value</c> is the element (<see cref="ElementValue" />).
    /// </summary>
    public ParameterExpression Element { get; }

    /// <summary>The element itself, as an expression over <see cref="Element" />.</summary>
    public Expression ElementValue { get; }

    public Type ElementType { get; }

    /// <summary>Whether the elements are scalars rather than child documents.</summary>
    public bool IsScalar { get; }

    public static FlattenData For(ParameterExpression parent, Type elementType)
    {
        var isScalar = SelectProjectionAnalyzer.IsScalar(elementType)
                       || Polecat.Storage.ValueTypes.TryResolve(Nullable.GetUnderlyingType(elementType) ?? elementType,
                           allowReferenceTypes: true) != null;

        if (!isScalar)
        {
            var element = Expression.Parameter(elementType, "element");
            return new FlattenData(parent, element, element, elementType, false);
        }

        var standIn = Expression.Parameter(typeof(FlattenedValue<>).MakeGenericType(elementType), "element");
        return new FlattenData(parent, standIn, Expression.Property(standIn, "Value"), elementType, true);
    }

    /// <summary>
    ///     The element type of a collection System.Text.Json writes as a JSON array, or null for anything else: a
    ///     string, a byte[] (base64), a dictionary (an object).
    /// </summary>
    public static Type? ElementTypeOf(Type type)
    {
        if (type == typeof(string) || type == typeof(byte[]) || typeof(IDictionary).IsAssignableFrom(type)) return null;

        var enumerable = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? type
            : type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (enumerable == null) return null;

        var isDictionary = type.GetInterfaces().Concat([type]).Any(i => i.IsGenericType
            && (i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
                || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)));

        return isDictionary ? null : enumerable.GetGenericArguments()[0];
    }

    /// <summary>The JSON path out of a collection's <c>JSON_VALUE(data, '$.path')</c> locator.</summary>
    public static string? PathOf(string rawLocator)
    {
        const string prefix = "JSON_VALUE(data, '";
        if (!rawLocator.StartsWith(prefix, StringComparison.Ordinal) || !rawLocator.EndsWith("')", StringComparison.Ordinal))
        {
            return null;
        }

        return rawLocator[prefix.Length..^2];
    }

    /// <summary>A <c>SelectMany()</c> result selector, <c>(x, c) =&gt; ...</c>, as the row over the parent and the element.</summary>
    public LambdaExpression Row(LambdaExpression resultSelector)
    {
        var body = new Substitution(new Dictionary<ParameterExpression, Expression>
        {
            [resultSelector.Parameters[0]] = Parent,
            [resultSelector.Parameters[1]] = ElementValue
        }).Visit(resultSelector.Body);

        return Expression.Lambda(body, Parent, Element);
    }

    /// <summary>Whether <paramref name="row" />, a lambda over the parent and the element, reads the parent.</summary>
    public bool ReadsParent(LambdaExpression row)
    {
        var finder = new Finder(Parent);
        finder.Visit(row.Body);
        return finder.Found;
    }

    /// <summary>
    ///     <paramref name="row" /> as a lambda over the parent and the element itself, to compile: the stand-in's
    ///     <c>Value</c> becomes the element.
    /// </summary>
    public LambdaExpression ToProjection(LambdaExpression row)
    {
        if (!IsScalar) return Expression.Lambda(row.Body, Parent, Element);

        var value = Expression.Parameter(ElementType, "element");
        var body = new Substitution(new Dictionary<ParameterExpression, Expression>(), (ElementValue, value))
            .Visit(row.Body);

        return Expression.Lambda(body, Parent, value);
    }

    private sealed class Substitution(Dictionary<ParameterExpression, Expression> parameters,
        (Expression From, Expression To)? replace = null) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node)
        {
            if (node != null && replace is { } r && node is MemberExpression member
                && r.From is MemberExpression from && member.Member == from.Member && member.Expression == from.Expression)
            {
                return r.To;
            }

            return base.Visit(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => parameters.TryGetValue(node, out var replacement) ? replacement : node;
    }

    private sealed class Finder(ParameterExpression parameter) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Found |= node == parameter;
            return node;
        }
    }
}
