using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;
using Polecat.Internal;
using Polecat.Linq.Parsing;

namespace Polecat.Linq.Members;

/// <summary>
///     Decides which members of a member expression are keys in the stored JSON and which exist only
///     in .NET, and translates or refuses the second kind.
/// </summary>
/// <remarks>
///     <para>
///         #840: a member resolver turned every CLR member into a JSON key, so <c>x.N.Value</c> read
///         <c>$.n.value</c> and <c>x.Items.Count</c> read <c>$.items.count</c>. Neither key can exist,
///         the path is always missing, and the query ran and matched nothing. That is the failure
///         <see cref="BadLinqExpressionException" /> exists to prevent, so the rule here has no default
///         of guessing: a member whose owner is written as a JSON scalar or a JSON array has no key, and
///         it either has a translation below or it is refused.
///     </para>
///     <para>
///         ⚠️ <b>Decided from the type the member is read off, never from its name.</b>
///         <c>x.Stats.Count</c> on a document class with a <c>Count</c> property is a real key and has to
///         stay one, and a <c>record struct Point(int X)</c> is a JSON object however much it looks like a
///         value wrapper. A type carrying its own <see cref="JsonConverterAttribute" /> decides its own
///         shape, so it is left to the JSON path as well.
///     </para>
///     <para>
///         Every <see cref="IMemberResolver" /> calls this, which is what makes it hold on every
///         execution path: Where, Select, OrderBy, GroupBy, the JSON streaming and cursor paths, joins,
///         batches and the event and stream-state queries all reach a member through one.
///     </para>
/// </remarks>
internal static class ClrOnlyMembers
{
    private const string JsonValuePrefix = "JSON_VALUE(data,";

    private static readonly PropertyInfo ArrayLength = typeof(Array).GetProperty(nameof(Array.Length))!;

    /// <summary>
    ///     <paramref name="expression" /> with every array length as an ordinary member access.
    /// </summary>
    /// <remarks>
    ///     <c>x.Arr.Length</c> is the one member the compiler does not write as a member: it is a
    ///     <see cref="ExpressionType.ArrayLength" /> node. Everything that recognizes a member, of which
    ///     there are a lot, looks for a <see cref="MemberExpression" />, so one rewrite where a tree comes
    ///     in serves all of them, and the array is then decided by the same rule as a list.
    /// </remarks>
    public static Expression Normalize(Expression expression) => new ArrayLengthRewriter().Visit(expression);

    /// <summary>
    ///     The members of <paramref name="expression" /> that are keys in the stored JSON, root first.
    ///     <c>Nullable&lt;T&gt;.Value</c> is stepped over, because a nullable is stored as its value.
    /// </summary>
    /// <exception cref="BadLinqExpressionException">
    ///     The chain does not start at the query's parameter, so there is no document for a path to be
    ///     relative to. <c>OrderBy(x =&gt; x.Items.First().Name)</c> used to read <c>$.name</c>.
    /// </exception>
    public static IReadOnlyList<MemberInfo> JsonKeys(MemberExpression expression)
    {
        var keys = new List<MemberInfo>();
        var current = Unwrapped(expression);

        while (current is MemberExpression member)
        {
            if (!IsNullableValue(member)) keys.Insert(0, member.Member);
            current = Unwrapped(member.Expression);
        }

        if (current is not ParameterExpression)
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL: it does not start at the queried "
                + "document, so it has no JSON path. Compare or order by a document member, evaluate the "
                + "value before the query, or materialize the query and finish the work in memory.");
        }

        return keys;
    }

    /// <summary>
    ///     The translation of <paramref name="expression" /> when it is a CLR-only member, or null when
    ///     it is an ordinary JSON member the caller should resolve as a path.
    /// </summary>
    /// <param name="expression">The member being resolved.</param>
    /// <param name="resolve">The caller's own resolver, for the member this one is read off.</param>
    /// <exception cref="BadLinqExpressionException">A CLR-only member with no translation.</exception>
    public static IQueryableMember? TryTranslate(
        MemberExpression expression, Func<MemberExpression, IQueryableMember> resolve)
    {
        if (expression.Expression is not { } ownerExpression) return null;

        var ownerType = ownerExpression.Type;
        var nullableOf = Nullable.GetUnderlyingType(ownerType);
        var isCollection = IsSerializedAsCollection(ownerType);

        if (nullableOf == null && !isCollection && !SelectProjectionAnalyzer.IsScalar(ownerType)) return null;

        // The document itself. It can only be a collection class if a converter stores it as something
        // else, and every member of it would be refused if this were left to the rule below.
        if (Unwrapped(ownerExpression) is ParameterExpression && isCollection && !IsFrameworkType(ownerType))
        {
            return null;
        }

        // A scalar projected by an earlier Select, or the result of a method call, has no stored
        // member to read this off: .Select(x => x.Name).Where(n => n.Length > 3) is the common one.
        if (Unwrapped(ownerExpression) is not MemberExpression ownerMember)
        {
            throw Refuse(expression, "It is not read off a stored document member.");
        }

        if (nullableOf != null)
        {
            // HasValue is the only other member Nullable<T> has that can reach here.
            return expression.Member.Name == nameof(Nullable<int>.Value)
                ? null
                : new HasValueMember(resolve(ownerMember), SelectProjectionAnalyzer.IsScalar(nullableOf));
        }

        if (isCollection) return CollectionCount(expression, resolve(ownerMember));

        // Length is the only instance property a string has.
        if (ownerType == typeof(string)) return StringLength(expression, resolve(ownerMember));

        if (IsDateOrTime(ownerType)) return DatePart(expression, resolve(ownerMember));

        throw Refuse(expression, $"No member of {ownerType.Name} translates.");
    }

    /// <summary>
    ///     Refuses <paramref name="member" /> where SQL Server does not allow a subquery, unless it has a
    ///     form without one. The message names the position, because the alternative is Msg 130 or Msg 144
    ///     from the server.
    /// </summary>
    public static IQueryableMember AssertNoSubquery(IQueryableMember member, string position)
    {
        if (member is ComputedMember { IsSubquery: true } computed)
        {
            return computed.WithoutSubquery ?? throw new BadLinqExpressionException(
                $"Polecat cannot translate a collection's Count or Length {position}: it needs a subquery "
                + "over the stored JSON, and SQL Server does not allow one there. Filter or order by it, or "
                + "materialize the query and finish the work in memory.");
        }

        return member;
    }

    private static bool IsNullableValue(MemberExpression member)
        => member.Member.Name == nameof(Nullable<int>.Value)
           && member.Expression is { } owner
           && Nullable.GetUnderlyingType(owner.Type) != null;

    /// <summary>
    ///     A type System.Text.Json writes as an array or, for a dictionary, an object of arbitrary keys.
    ///     Either way a property of the CLR type is not among them.
    /// </summary>
    private static bool IsSerializedAsCollection(Type type)
        => type != typeof(string)
           && typeof(IEnumerable).IsAssignableFrom(type)
           && type.GetCustomAttribute<JsonConverterAttribute>() == null;

    private static bool IsFrameworkType(Type? type)
        => type?.Namespace is { } ns && (ns == "System" || ns.StartsWith("System.", StringComparison.Ordinal));

    private static bool IsDateOrTime(Type type)
        => type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly)
           || type == typeof(TimeOnly);

    private static Expression? Unwrapped(Expression? expression)
    {
        while (expression is UnaryExpression
               {
                   NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs
               } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static IQueryableMember CollectionCount(MemberExpression expression, IQueryableMember collection)
    {
        // Only the framework's own. A custom enumerable's Count or Length can mean anything, and its
        // elements are all the JSON holds of it.
        if (expression.Member.Name is not ("Count" or "Length") || !IsFrameworkType(expression.Member.DeclaringType))
        {
            throw Refuse(expression, "The members of a collection that translate are Count and Length.");
        }

        // Written as a base64 string, which OPENJSON would read as no elements at all.
        if (expression.Expression!.Type == typeof(byte[]))
        {
            throw Refuse(expression, "A byte[] is stored as a base64 string, not as a JSON array.");
        }

        if (!collection.RawLocator.StartsWith(JsonValuePrefix, StringComparison.Ordinal))
        {
            throw Refuse(expression, "The collection is not stored in the JSON body.");
        }

        var elements = collection.RawLocator.Replace("JSON_VALUE(", "OPENJSON(");
        return new ComputedMember($"(SELECT COUNT(*) FROM {elements})", typeof(int), "int", isSubquery: true);
    }

    /// <summary>
    ///     <c>DATALENGTH</c> rather than <c>LEN</c>, which ignores trailing spaces. The cast makes it
    ///     two bytes per UTF-16 unit, which is what <see cref="string.Length" /> counts, for a
    ///     <c>varchar</c> id column as much as for <c>JSON_VALUE</c>. It has to be <c>nvarchar(max)</c>
    ///     because the read underneath is the full one (#835), and <c>DATALENGTH</c> of that is a
    ///     <c>bigint</c>, hence the outer cast.
    /// </summary>
    /// <remarks>
    ///     On <c>nvarchar(max)</c> storage the full read is a subquery. A <c>GROUP BY</c> key and an
    ///     aggregate over a group can't hold one, so those two read through <c>JSON_VALUE</c>, the same
    ///     as a string member does there: <c>NULL</c> past 4000 characters.
    /// </remarks>
    private static IQueryableMember StringLength(MemberExpression expression, IQueryableMember text)
    {
        string length(string locator) => $"CAST(DATALENGTH(CAST({locator} AS nvarchar(max))) / 2 AS int)";

        return SqlText.IsSubquery(text.RawLocator)
            ? new ComputedMember(length(text.RawLocator), expression.Type, "int", isSubquery: true,
                withoutSubquery: new ComputedMember(length(text.TypedLocator), expression.Type, "int"))
            : new ComputedMember(length(text.RawLocator), expression.Type, "int");
    }

    /// <summary>
    ///     Reads the part out of the stored value, which keeps its offset, so a
    ///     <see cref="DateTimeOffset" /> gives the same local answer in SQL as it does in .NET.
    /// </summary>
    private static IQueryableMember DatePart(MemberExpression expression, IQueryableMember value)
    {
        var locator = value.TypedLocator;

        var sql = expression.Member.Name switch
        {
            "Year" => $"DATEPART(year, {locator})",
            "Month" => $"DATEPART(month, {locator})",
            "Day" => $"DATEPART(day, {locator})",
            "Hour" => $"DATEPART(hour, {locator})",
            "Minute" => $"DATEPART(minute, {locator})",
            "Second" => $"DATEPART(second, {locator})",
            "Millisecond" => $"DATEPART(millisecond, {locator})",
            "DayOfYear" => $"DATEPART(dayofyear, {locator})",

            // Counted from a known Monday, because DATEPART(weekday, ...) moves with SET DATEFIRST.
            "DayOfWeek" => $"((DATEDIFF(day, CONVERT(date, '00010101', 112), CONVERT(date, {locator})) + 1) % 7)",
            _ => null
        };

        if (sql != null) return new ComputedMember(sql, expression.Type, "int");

        if (expression.Member.Name == "Date")
        {
            return new ComputedMember($"CONVERT(date, {locator})", expression.Type, "date");
        }

        throw Refuse(expression,
            "The members of a date or time that translate are Year, Month, Day, Hour, Minute, Second, "
            + "Millisecond, DayOfYear, DayOfWeek and Date.");
    }

    private sealed class ArrayLengthRewriter : ExpressionVisitor
    {
        protected override Expression VisitUnary(UnaryExpression node)
            => node.NodeType == ExpressionType.ArrayLength
                ? Expression.Property(Visit(node.Operand), ArrayLength)
                : base.VisitUnary(node);
    }

    private static BadLinqExpressionException Refuse(MemberExpression expression, string why)
        => new($"Polecat cannot translate '{expression}' to SQL. "
               + $"{expression.Member.DeclaringType?.Name.Split('`')[0]}.{expression.Member.Name} exists only "
               + "in .NET and has no key in the stored JSON, so the "
               + $"query could only ever read a path that is always missing. {why} Otherwise run the query "
               + "as raw SQL through session.AdvancedSql.QueryAsync<T>(...), or materialize it and finish "
               + "the work in memory.");
}
