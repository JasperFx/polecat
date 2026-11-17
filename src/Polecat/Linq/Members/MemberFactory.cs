using JasperFx;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Polecat.Internal;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;
using Polecat.Storage;
using Weasel.Core;
using Weasel.Storage;

namespace Polecat.Linq.Members;

/// <summary>
///     Resolves C# member expressions to IQueryableMember instances for SQL generation.
/// </summary>
internal class MemberFactory : IMemberResolver
{
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly EnumStorage _enumStorage;
    private readonly Type _idType;
    private readonly ValueTypeInfo? _valueTypeId;
    private readonly DocumentMapping _mapping;
    private readonly bool _useReturning;
    private readonly ISerializer _serializer;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly StoreOptions _options;

    /// <summary>
    ///     The column the JSON paths are read from: the document's <c>data</c>, or (gh-1016) the element of a
    ///     collection a <c>SelectMany()</c> flattened.
    /// </summary>
    private readonly string _jsonColumn = "data";

    /// <summary>gh-1016: whether this resolves the members of a flattened collection's elements.</summary>
    private readonly bool _isElement;

    /// <summary>
    ///     gh-1016: the column of the flattened elements, from <c>CROSS APPLY OPENJSON(...) AS __pc_elem</c>.
    ///     A scalar element is its <c>value</c> as it stands; an object element is JSON read from it.
    /// </summary>
    internal const string ElementValue = "__pc_elem.[value]";

    /// <summary>
    ///     The mapping these members belong to. Exposed because a method-call parser sometimes needs
    ///     more than the member — the full-text operators need the document table and its tenancy to
    ///     address the token table beside it — and <see cref="IMemberResolver" /> deliberately carries
    ///     only member resolution.
    /// </summary>
    internal DocumentMapping Mapping => _mapping;

    /// <summary>
    ///     gh-1016: the members of the elements of a collection a <c>SelectMany()</c> flattened. They are read
    ///     from <see cref="ElementValue" />, which is text whatever the document's storage, and no index of the
    ///     document covers them and no member of them is the document's id.
    /// </summary>
    private MemberFactory(StoreOptions options, DocumentMapping mapping, bool isElement) : this(options, mapping)
    {
        _isElement = isElement;
        _jsonColumn = ElementValue;
        _useReturning = false;
    }

    public IMemberResolver? ForElements() => _isElement ? null : new MemberFactory(_options, _mapping, true);

    public MemberFactory(StoreOptions options, DocumentMapping mapping)
    {
        _options = options;
        _enumStorage = options.Serializer.EnumStorage;
        _serializer = options.Serializer;
        _idType = mapping.IdType;
        _valueTypeId = mapping.ValueTypeId;
        _mapping = mapping;
        // #217: JSON_VALUE(... RETURNING <type>) is only available when the data column is the
        // native `json` type (SQL Server 2025+), which is exactly what UseNativeJsonType selects.
        // On nvarchar(max) storage the RETURNING clause is a syntax error, so fall back to CAST.
        _useReturning = options.UseNativeJsonType;

        // #510: one resolver, shared with the index DDL builder. These two worked the policy out
        // separately and drifted, which is what made a snake_case store's computed columns index a
        // path the serializer never writes.
        _namingPolicy = SerializedNames.PolicyFor(options);

        // #808: what a union's case types are written as. A custom ISerializer is opaque, so its
        // stand-in is the default options under the policy worked out above, as for every other name.
        _jsonOptions = options.Serializer is Serializer s
            ? s.Options
            : new JsonSerializerOptions
            {
                PropertyNamingPolicy = _namingPolicy,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
    }

    public IQueryableMember ResolveMember(MemberExpression expression)
    {
        // gh-1016: x => x.Values flattens scalars, and each is read as the member Value of a stand-in
        if (_isElement && FlattenedValue.IsValueOf(expression))
        {
            return ElementValueMember(expression.Type);
        }

        // Check if it's the Id property on the root document
        if (!_isElement && expression.Member.Name == "Id" && expression.Expression is ParameterExpression)
        {
            return new IdMember(_idType, _valueTypeId,
                _mapping.IdSqlType);
        }

        // #840: a member that exists only in .NET has no JSON key, so it is translated or refused here,
        // where every execution path comes through, rather than becoming a path that is always missing.
        var clrOnly = ClrOnlyMembers.TryTranslate(expression, ResolveMember, ResolvePath);
        if (clrOnly is ComputedMember computed) return WithStoredEnumForm(computed);

        // #808: a union, or its Value, is the active case in whatever shape that takes. It can be tested
        // against null and nothing else, so it is read as something that may be an object.
        var unionType = UnionTypeAt(expression);
        if (unionType != null && _isElement)
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL: a union-typed member of a flattened element is not "
                + "translated. Materialize the query and finish the work in memory.");
        }

        if (unionType != null)
        {
            return new UnionValueMember(CreateMember(BuildJsonPath(expression), typeof(object)), expression,
                unionType);
        }

        var member = clrOnly ?? CreateMember(BuildJsonPath(expression), GetMemberType(expression.Member));

        // #808: ((Card)x.Payment.Value).Last4 reads $.payment.last4 only while the union holds a Card.
        var casts = UnionMembers.CaseCasts(expression);
        return casts.Count == 0 ? member : new UnionCaseMember(member, CaseGuard(casts, expression));
    }

    /// <summary>
    ///     gh-1014: a member, or an index into a collection the document holds, such as <c>x.Values[1]</c>.
    /// </summary>
    public IQueryableMember ResolvePath(Expression expression)
    {
        expression = LinqQueryParser.StripConvert(expression);
        if (expression is MemberExpression member) return ResolveMember(member);

        if (!DocumentPaths.IsIndexPath(expression))
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL: it is not a member of the document or an index "
                + "into one of its collections.");
        }

        return CreateMember(BuildJsonPath(expression), expression.Type);
    }

    public IQueryableMember ResolveKey(MemberExpression expression, string position)
    {
        if (UnionTypeAt(expression) != null) throw UnionMembers.RefuseKey(expression, position);
        return StoredShape.AssertHasValue(ResolveMember(expression), expression.Member, _serializer, position);
    }

    public ISqlFragment ResolveUnionTypeTest(MemberExpression unionValue, Type tested, Expression expression)
    {
        var test = TypeTestSql(unionValue, tested, expression);
        var outer = UnionMembers.CaseCasts(unionValue);
        return new WhereFragment(outer.Count == 0 ? test : $"({CaseGuard(outer, expression)} and {test})");
    }

    public IQueryableMember ResolveUnionCase(MemberExpression unionValue, Type caseType, Expression expression)
    {
        var member = CreateMember(BuildJsonPath(unionValue), caseType);
        var casts = new List<(MemberExpression, Type)> { (unionValue, caseType) };
        casts.AddRange(UnionMembers.CaseCasts(unionValue));
        return new UnionCaseMember(member, CaseGuard(casts, expression));
    }

    /// <summary>
    ///     The union type <paramref name="expression" /> reads, when it reads a union-typed member or that
    ///     member's <c>Value</c>, rather than a member of one of its cases.
    /// </summary>
    private static Type? UnionTypeAt(MemberExpression expression)
    {
        if (UnionShape.IsUnionValue(expression)) return UnionMembers.UnionTypeOf(expression);

        var type = Nullable.GetUnderlyingType(expression.Type) ?? expression.Type;
        return type.IsUnion() ? type : null;
    }

    private string CaseGuard(IReadOnlyList<(MemberExpression UnionValue, Type CaseType)> casts, Expression expression)
    {
        var tests = casts.Select(x => TypeTestSql(x.UnionValue, x.CaseType, expression)).ToArray();
        return tests.Length == 1 ? tests[0] : "(" + string.Join(" and ", tests) + ")";
    }

    /// <summary>
    ///     #808: the SQL that is true when the union at <paramref name="unionValue" /> holds a
    ///     <paramref name="tested" />, rendered by Weasel's shared <see cref="SqlServerUnionSqlDialect" />.
    /// </summary>
    private string TypeTestSql(MemberExpression unionValue, Type tested, Expression expression)
    {
        var unionType = UnionMembers.UnionTypeOf(unionValue);
        var shape = UnionShape.For(unionType, _jsonOptions, _enumStorage == EnumStorage.AsString);
        var cases = shape.CasesMatching(tested);
        if (cases.Count == 0)
        {
            throw new BadLinqExpressionException(
                $"Polecat cannot translate '{expression}' to SQL: {tested.ShortNameInCode()} is not a case of "
                + $"the union {unionType.ShortNameInCode()} (its cases are "
                + $"{string.Join(", ", shape.CaseTypes.Select(x => x.ShortNameInCode()))}), so no stored value "
                + "can ever be one. Test for one of the cases.");
        }

        var path = ClrOnlyMembers.JsonKeys(unionValue).Select(GetJsonPropertyName).ToArray();
        var tests = cases
            .Select(x => SqlServerUnionSqlDialect.Instance.TypeTest("data", path, shape.TestFor(x, expression.ToString())))
            .ToArray();

        return tests.Length == 1 ? tests[0] : "(" + string.Join(" or ", tests) + ")";
    }

    /// <summary>
    ///     #875: SQL computes <c>DayOfWeek</c> as a number, and that is what a predicate compares and a
    ///     scalar read takes. A JSON projection is different: under <see cref="EnumStorage.AsString" />
    ///     every enum a document holds is written as a name, by the rules <see cref="EnumMember" /> applies,
    ///     and this one has to look like them.
    /// </summary>
    private IQueryableMember WithStoredEnumForm(ComputedMember computed)
    {
        if (computed.MemberType != typeof(DayOfWeek) || _enumStorage != EnumStorage.AsString) return computed;

        var enumMember = new EnumMember(computed.TypedLocator, typeof(DayOfWeek), _enumStorage, _namingPolicy);
        var names = Enum.GetValues<DayOfWeek>().OrderBy(x => (int)x)
            .Select(x => $"N{SqlEscaping.Literal((string)enumMember.ConvertValue(x)!)}");

        return computed.WrittenAs($"CHOOSE({computed.TypedLocator} + 1, {string.Join(", ", names)})");
    }

    /// <summary>
    ///     Formats the JSON key a projection member would serialize to — an explicit
    ///     <c>[JsonPropertyName]</c> wins verbatim over the configured naming policy, exactly as
    ///     System.Text.Json serializes it. Used when translating a simple <c>Select()</c> projection
    ///     to a server-side JSON object so the emitted keys match what a client would deserialize.
    /// </summary>
    public string FormatProjectedKey(MemberInfo member) => GetJsonPropertyName(member);

    /// <summary>
    ///     The JSON key for a bare name, a positional constructor parameter having no
    ///     <see cref="MemberInfo" /> to read an attribute from.
    /// </summary>
    public string FormatProjectedKey(string name) => _namingPolicy?.ConvertName(name) ?? name;

    /// <summary>
    ///     gh-1016: one element of a flattened collection of scalars, which <c>OPENJSON</c> hands out as text:
    ///     a string already unescaped, a number, <c>true</c>/<c>false</c>, an enum's name or number.
    /// </summary>
    private IQueryableMember ElementValueMember(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying.IsEnum) return new EnumMember(ElementValue, underlying, _enumStorage, _namingPolicy);
        if (underlying == typeof(bool)) return new QueryableMember(ElementValue, ElementValue, type, isBoolean: true);

        var valueType = ValueTypes.TryResolve(underlying, allowReferenceTypes: true);
        var storedType = valueType?.SimpleType ?? underlying;
        var sqlType = GetSqlType(storedType);
        var typed = sqlType == null ? ElementValue : $"CAST({ElementValue} AS {sqlType})";

        return valueType != null
            ? new ValueTypeMember(ElementValue, typed, type, valueType, sqlType, null, null)
            : new QueryableMember(ElementValue, typed, type, locatorSqlType: sqlType,
                mayBeObject: !SelectProjectionAnalyzer.IsScalar(underlying));
    }

    private IQueryableMember CreateMember(string jsonPath, Type memberType)
    {
        var bareLocator = $"JSON_VALUE({_jsonColumn}, '{jsonPath}')";
        var rawLocator = bareLocator;
        var underlying = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (underlying.IsEnum)
        {
            return new EnumMember(rawLocator, underlying, _enumStorage, _namingPolicy, jsonPath, _useReturning);
        }

        if (underlying == typeof(bool))
        {
            return new QueryableMember(rawLocator, rawLocator, memberType, isBoolean: true);
        }

        // A strong-typed-id wrapper on a member other than Id (the Id member is short-circuited to
        // IdMember above). Vogen and StronglyTypedId both emit a System.Text.Json converter that
        // writes the inner value, so the JSON holds a scalar and the member is typed, cast and indexed
        // exactly like its inner type. Only the CLR value in the predicate differs — it is still the
        // wrapper, which SqlClient cannot bind, so ValueTypeMember unwraps it.
        var valueType = ValueTypes.TryResolve(underlying, allowReferenceTypes: true);
        var storedType = valueType?.SimpleType ?? underlying;

        var sqlType = GetSqlType(storedType);
        var typedLocator = BuildTypedLocator(jsonPath, rawLocator, sqlType);

        // #223: if a Default-casing Index(...) covers this member, emit the EXACT computed-column
        // expression the index defines instead of the bare/uncast locator. SQL Server then matches
        // the predicate to the persisted computed column and can seek the index. Without this the
        // translator's expression (e.g. bare JSON_VALUE for a string, CAST(... AS datetimeoffset)
        // for a date) never lines up with the index column, so the index is dead weight.
        // #710: the locator's SQL TYPE travels with it, because the #223 rewrite can change it --
        // an index may declare a different type than SqlTypeMap would, and the caller can override
        // it outright (DocumentIndex.SqlType). A fragment binding a value list has to type the other
        // side of the comparison to match, and nothing but this knows what it matches.
        var locatorSqlType = sqlType;

        if (!_isElement && TryGetIndexedLocator(jsonPath, storedType, out var indexedLocator, out var indexedSqlType))
        {
            typedLocator = indexedLocator;
            locatorSqlType = indexedSqlType;
        }

        // #835: RawLocator is what the text predicates are built on, and a bare JSON_VALUE answers NULL
        // past 4000 characters, so a string member's raw read has to be the full one. TypedLocator stays
        // exactly as computed above: it is what an index matches, and what ORDER BY and GROUP BY use, and
        // the nvarchar(max)-storage form of the full read is a subquery GROUP BY refuses. The predicates
        // that may use it are chosen by QueryableMemberExtensions.ComparisonLocator.
        int? textCapacity = null;
        if (storedType == typeof(string))
        {
            rawLocator = SqlText.UnlimitedJsonString(_jsonColumn, jsonPath, _useReturning);
            textCapacity = locatorSqlType == null
                ? SqlText.JsonValueCapacity
                : SqlTypeMap.TextCapacity(locatorSqlType);
        }

        var bareTextLocator = textCapacity == null ? null : bareLocator;

        return valueType != null
            ? new ValueTypeMember(rawLocator, typedLocator, memberType, valueType, locatorSqlType, textCapacity,
                bareTextLocator)
            : new QueryableMember(rawLocator, typedLocator, memberType, locatorSqlType: locatorSqlType,
                textCapacity: textCapacity, bareTextLocator: bareTextLocator,
                mayBeObject: !SelectProjectionAnalyzer.IsScalar(underlying));
    }

    private bool TryGetIndexedLocator(string jsonPath, Type underlying, out string locator,
        out string? sqlType)
    {
        locator = string.Empty;
        sqlType = null;

        // Only Default-casing indexes are predicate-transparent. Upper/Lower computed columns
        // fold case, so a plain equality/range predicate must not be rewritten onto them.
        var index = _mapping.Indexes.FirstOrDefault(i =>
            i.Casing == IndexCasing.Default && Array.IndexOf(i.JsonPaths, jsonPath) >= 0);
        if (index == null) return false;

        sqlType = index.ResolveSqlType(jsonPath, underlying);
        locator = DocumentIndex.ComputedColumnExpression(jsonPath, sqlType, IndexCasing.Default, _useReturning);
        return true;
    }

    /// <summary>
    ///     #1030: the case-folded member over a <c>Casing = Lower</c> or <c>Upper</c> index's computed column.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The cased form of what #223 does for a Default index. <see cref="IQueryableMember.TypedLocator" />
    ///         is the exact expression the column is defined with, <c>LOWER(JSON_VALUE(... RETURNING
    ///         varchar(250)))</c> for instance, so SQL Server matches a predicate to the column and seeks
    ///         the index. Before this a cased index could enforce case-insensitive uniqueness and nothing
    ///         else, because <c>x.Email.ToLower() == value</c> folded the full read of the document and
    ///         scanned.
    ///     </para>
    ///     <para>
    ///         The raw and bare locators are the same case fold over the document, so the comparison
    ///         rules of a Default index carry over unchanged: a value wider than the column, or outside
    ///         a <c>varchar</c> column's code page, is compared with the document, and an equality is
    ///         checked against the document's length (see
    ///         <see cref="QueryableMemberExtensions.ComparisonLocator" />).
    ///     </para>
    /// </remarks>
    public IQueryableMember? ResolveCasedMember(MemberExpression expression, IndexCasing casing)
    {
        if (_isElement || casing == IndexCasing.Default) return null;

        // Only a plain string member of the document, read from its own JSON path. A union case, a
        // member that exists only in .NET, and the id are something else, and keep their translation.
        if (ResolveMember(expression) is not QueryableMember { MemberType: var type } plain
            || plain.GetType() != typeof(QueryableMember) || type != typeof(string))
        {
            return null;
        }

        var jsonPath = BuildJsonPath(expression);
        var index = _mapping.Indexes.FirstOrDefault(i =>
            i.Casing == casing && Array.IndexOf(i.JsonPaths, jsonPath) >= 0);
        if (index == null) return null;

        var sqlType = index.ResolveSqlType(jsonPath, typeof(string));
        var function = casing == IndexCasing.Lower ? "LOWER" : "UPPER";

        return new QueryableMember(
            $"{function}({SqlText.UnlimitedJsonString(_jsonColumn, jsonPath, _useReturning)})",
            DocumentIndex.ComputedColumnExpression(jsonPath, sqlType, casing, _useReturning),
            typeof(string),
            locatorSqlType: sqlType,
            textCapacity: SqlTypeMap.TextCapacity(sqlType),
            bareTextLocator: $"{function}(JSON_VALUE({_jsonColumn}, '{jsonPath}'))");
    }

    /// <summary>
    ///     #217: builds the typed locator, preferring JSON_VALUE(... RETURNING type) on native json
    ///     storage (SQL Server 2025+) over CAST(JSON_VALUE(...) AS type). RETURNING does not support
    ///     uniqueidentifier, so Guid members keep CAST. A null sqlType (string/bool) needs no typing.
    ///     ⚠️ This is the locator for an <b>un-indexed</b> member, and CAST is right for it — there is no
    ///     persisted column for it to line up with. A member covered by a Default-casing Index(...) is
    ///     rewritten in CreateMember to that index's computed-column expression instead, which takes
    ///     precedence and is spelled CONVERT since #684 (because that is what SQL Server stores for the
    ///     column). The two spellings therefore differ on purpose: the one that has to match a persisted
    ///     column matches it, and the one that has nothing to match keeps the simpler form (#223).
    /// </summary>
    internal static string BuildTypedLocator(string jsonPath, string rawLocator, string? sqlType,
        bool useReturning)
    {
        if (sqlType == null) return rawLocator;
        return useReturning && SupportsReturning(sqlType)
            ? $"JSON_VALUE(data, '{jsonPath}' RETURNING {sqlType})"
            : $"CAST({rawLocator} AS {sqlType})";
    }

    private string BuildTypedLocator(string jsonPath, string rawLocator, string? sqlType)
        => BuildTypedLocator(jsonPath, rawLocator, sqlType, _useReturning);

    /// <summary>
    ///     RETURNING supports every type Polecat emits except uniqueidentifier (not in the SQL Server
    ///     JSON_VALUE RETURNING type list), so Guid members fall back to CAST.
    /// </summary>
    internal static bool SupportsReturning(string sqlType) => sqlType != "uniqueidentifier";

    private string BuildJsonPath(Expression expression)
        => SerializedNames.PathFor(
            ClrOnlyMembers.JsonPath(expression)
                .Select(x => (x.Member != null ? GetJsonPropertyName(x.Member)
                    : x.Key != null ? Polecat.Patching.JsonPathHelper.KeyText(x.Key, _serializer)
                    : null, x.Index)), _serializer);

    /// <summary>
    ///     #270: resolves the JSON key for a member exactly as System.Text.Json serializes it — an
    ///     explicit [JsonPropertyName] wins verbatim over the naming policy (STJ does NOT apply the
    ///     policy on top of an explicit name), otherwise the configured naming policy is applied.
    /// </summary>
    private string GetJsonPropertyName(MemberInfo member)
    {
        var attribute = member.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attribute != null)
        {
            return attribute.Name;
        }

        return _namingPolicy?.ConvertName(member.Name) ?? member.Name;
    }

    private static Type GetMemberType(MemberInfo member)
    {
        return member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new BadLinqExpressionException(
                $"Polecat cannot translate the member '{member.Name}': a translatable member is a "
                + $"property or a field, not a {member.MemberType}.")
        };
    }

    private static string? GetSqlType(Type type)
    {
        return SqlTypeMap.For(type);
    }
}
