using JasperFx;
using System.Linq.Expressions;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Polecat.Linq.Parsing;

namespace Polecat.Linq.Members;

/// <summary>
///     #808: recognising the LINQ shapes over a C# union-typed document member.
/// </summary>
/// <remarks>
///     <para>
///         The compiler refuses a union pattern in an expression tree (<c>x.Payment is Card</c> is CS8122),
///         so a query reaches the active case through <c>IUnion.Value</c>:
///     </para>
///     <list type="bullet">
///         <item><c>x.Payment.Value is Card</c>, a type test</item>
///         <item><c>((Card)x.Payment.Value).Last4</c> or <c>(x.Payment.Value as Card).Last4</c>, a case member</item>
///         <item><c>(int)x.Code.Value</c>, a primitive case read as itself</item>
///         <item><c>x.Payment.Value == null</c>, a default union</item>
///     </list>
///     <para>
///         STJ writes the bare case at the union member's own path, so <c>Value</c> is not a key in the
///         stored JSON: <see cref="ClrOnlyMembers.JsonKeys" /> steps over it like <c>Nullable&lt;T&gt;.Value</c>.
///     </para>
/// </remarks>
internal static class UnionMembers
{
    /// <summary>
    ///     <c>x.Payment.Value</c> where <c>Payment</c> is a union. Never true before .NET 11.
    /// </summary>
    public static bool IsUnionValue(Expression? expression)
        => expression is MemberExpression member && UnionShape.IsUnionValue(member);

    /// <summary>
    ///     A cast of a union's <c>Value</c> to a type: <c>(Card)x.Payment.Value</c> or
    ///     <c>x.Payment.Value as Card</c>, through any further conversions around it.
    /// </summary>
    public static bool TryGetCaseCast(Expression? expression, out MemberExpression unionValue, out Type caseType)
    {
        unionValue = null!;
        caseType = null!;

        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs
               } unary)
        {
            if (unary.Operand is MemberExpression member && UnionShape.IsUnionValue(member))
            {
                unionValue = member;
                caseType = unary.Type;
                return true;
            }

            expression = unary.Operand;
        }

        return false;
    }

    /// <summary>
    ///     The member <paramref name="expression" /> is read off, looking through a case cast:
    ///     <c>((Card)x.Payment.Value).Last4</c> is read off <c>x.Payment.Value</c>.
    /// </summary>
    public static MemberExpression? Owner(MemberExpression expression)
        => TryGetCaseCast(expression.Expression, out var unionValue, out _)
            ? unionValue
            : expression.Expression as MemberExpression;

    /// <summary>
    ///     Every case cast along <paramref name="expression" />'s chain, innermost last.
    /// </summary>
    public static IReadOnlyList<(MemberExpression UnionValue, Type CaseType)> CaseCasts(MemberExpression expression)
    {
        var casts = new List<(MemberExpression, Type)>();
        MemberExpression? current = expression;
        while (current != null)
        {
            if (TryGetCaseCast(current.Expression, out var unionValue, out var caseType))
            {
                casts.Add((unionValue, caseType));
            }

            current = Owner(current);
        }

        return casts;
    }

    public static BadLinqExpressionException RefuseComparison(Expression expression, Type unionType)
        => new($"Polecat cannot translate '{expression}' to SQL: a union ({unionType.ShortNameInCode()}) is "
               + "only compared to null. Test its case (x.Member.Value is Case), compare a member of the case "
               + "(((Case)x.Member.Value).Name == \"...\"), or cast a primitive case to compare it "
               + "((int)x.Member.Value > 3).");

    public static BadLinqExpressionException RefuseKey(Expression expression, string position)
        => new($"Polecat cannot translate '{expression}' {position}: a union is stored as its active case, "
               + "whichever that is, so it has no one value SQL can sort or group by. Use a member of a case "
               + "(((Case)x.Member.Value).Name), or a primitive case cast to its type.");

    public static Type UnionTypeOf(MemberExpression unionValue) => unionValue.Expression!.Type;
}

/// <summary>
///     A member of a union's case, or a primitive case itself, read only when the union holds that case:
///     every locator is <c>CASE WHEN &lt;type test&gt; THEN &lt;locator&gt; END</c>. The C# it stands for
///     throws or reads null on any other case, so another case's row must never match through a property
///     that happens to share the name.
/// </summary>
internal sealed class UnionCaseMember : IQueryableMember
{
    private readonly IQueryableMember _inner;
    private readonly string _guard;

    public UnionCaseMember(IQueryableMember inner, string guard)
    {
        _inner = inner;
        _guard = guard;
    }

    /// <summary>The type test for the case, which a comparison ANDs in so it never matches another case.</summary>
    public string Guard => _guard;

    private string Guarded(string locator) => $"CASE WHEN {_guard} THEN {locator} END";

    public Type MemberType => _inner.MemberType;
    public string TypedLocator => Guarded(_inner.TypedLocator);
    public string RawLocator => Guarded(_inner.RawLocator);
    public bool IsBoolean => _inner.IsBoolean;
    public string? LocatorSqlType => _inner.LocatorSqlType;
    public int? TextCapacity => _inner.TextCapacity;
    public string NullLocator => Guarded(_inner.NullLocator);
    public bool MayBeObject => _inner.MayBeObject;
    public string? BareTextLocator => _inner.BareTextLocator is { } bare ? Guarded(bare) : null;
    public string? JsonLocator => _inner.JsonLocator is { } json ? Guarded(json) : null;
    public object? ConvertValue(object? value) => _inner.ConvertValue(value);
}

/// <summary>
///     A union member itself, or its <c>Value</c>: it can be tested against null and nothing else.
/// </summary>
internal sealed class UnionValueMember : IQueryableMember
{
    private readonly IQueryableMember _inner;
    private readonly MemberExpression _expression;
    private readonly Type _unionType;

    public UnionValueMember(IQueryableMember inner, MemberExpression expression, Type unionType)
    {
        _inner = inner;
        _expression = expression;
        _unionType = unionType;
    }

    public Type MemberType => _inner.MemberType;
    public string TypedLocator => _inner.TypedLocator;
    public string RawLocator => _inner.RawLocator;
    public bool IsBoolean => false;
    public string? LocatorSqlType => _inner.LocatorSqlType;
    public string NullLocator => _inner.NullLocator;
    public bool MayBeObject => true;

    public object? ConvertValue(object? value)
        => value == null ? null : throw UnionMembers.RefuseComparison(_expression, _unionType);
}
