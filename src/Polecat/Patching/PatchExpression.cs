using System.Diagnostics.CodeAnalysis;
using JasperFx.Events;
using Polecat.Internal;
using Polecat.Serialization;
using System.Linq.Expressions;
using System.Text.Json;
using Weasel.SqlServer;

namespace Polecat.Patching;

[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: invokes ISerializer.ToJson on patch values whose runtime types are derived from the document's reflected properties. Document type T flows in from the registration boundary (Schema.For<T>()) and is preserved per the AOT publishing guide; AOT consumers supply a source-generator-backed ISerializer impl.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: ISerializer.ToJson is annotated RDC for the same reason as IL2026 above. AOT consumers supply a source-generator-backed ISerializer impl.")]
internal class PatchExpression<T> : IPatchExpression<T>
{
    /// <summary>
    ///     What a decimal is read as to increment it. 37 rather than 38 so the sum of two of them still
    ///     fits in 38 and keeps its scale. A <see cref="decimal" /> can hold more than this on either side
    ///     of the point, and no SQL type holds all of it: more integer digits is an overflow error, and
    ///     more than 18 decimal places is rounded.
    /// </summary>
    private const string DecimalArithmeticType = "decimal(37,18)";

    private readonly List<Action<ICommandBuilder>> _actions = new();
    private readonly StoreOptions _options;
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly bool _nativeJson;

    public PatchExpression(object id, string tenantId, DocumentSessionBase session)
    {
        _options = session.Options;
        _namingPolicy = GetNamingPolicy(session);
        _nativeJson = session.Options.UseNativeJsonType;

        var provider = session.Providers.GetProvider<T>();
        var mapping = provider.Mapping;
        var operation = new PatchOperation(mapping, _actions, builder =>
        {
            builder.Append("id = ");
            builder.AppendParameter(id);
            if (mapping.TenancyStyle == TenancyStyle.Conjoined) // #234
            {
                builder.Append(" AND tenant_id = ");
                builder.AppendParameter(tenantId);
            }
        });
        session.WorkTracker.Add(operation);
    }

    public PatchExpression(Expression<Func<T, bool>> filter, DocumentSessionBase session)
    {
        _options = session.Options;
        _namingPolicy = GetNamingPolicy(session);
        _nativeJson = session.Options.UseNativeJsonType;

        var provider = session.Providers.GetProvider<T>();
        var mapping = provider.Mapping;

        var memberFactory = new Linq.Members.MemberFactory(session.Options, mapping);
        var whereParser = new Linq.Parsing.WhereClauseParser(memberFactory);
        var fragment = whereParser.Parse(filter.Body);

        var operation = new PatchOperation(mapping, _actions, builder =>
        {
            if (mapping.TenancyStyle == TenancyStyle.Conjoined) // #234
            {
                builder.Append("tenant_id = ");
                builder.AppendParameter(session.TenantId);
                builder.Append(" AND ");
            }

            fragment.Apply(builder);
        }, builder =>
        {
            if (mapping.TenancyStyle == TenancyStyle.Conjoined)
            {
                builder.Append("tenant_id = ");
                builder.AppendParameter(session.TenantId);
                builder.Append(" AND ");
            }
        });
        session.WorkTracker.Add(operation);
    }

    private static JsonNamingPolicy? GetNamingPolicy(DocumentSessionBase session)
    {
        if (session.Options.Serializer is Serializer s)
        {
            return s.Options.PropertyNamingPolicy;
        }

        return JsonNamingPolicy.CamelCase;
    }

    private string ToPath(Expression expression) => JsonPathHelper.ToPath(expression, _namingPolicy, _options.Serializer);

    /// <summary>
    ///     What goes in the document. Everything a patch writes or looks for has to be this, so that it
    ///     is the same text a stored document has (#879).
    /// </summary>
    private string ToJson<TValue>(TValue value) => _options.ToStorageJson(value!);

    /// <summary>A dictionary key as a path segment.</summary>
    private string Key(string key)
        => JsonPathHelper.Segment(JsonPathHelper.KeyText(key, _options.Serializer), _options.Serializer);

    public IPatchExpression<T> Set<TValue>(string name, TValue value)
    {
        var path = NamePath(name);
        AddSetAction(path, value);
        return this;
    }

    public IPatchExpression<T> Set<TParent, TValue>(string name, Expression<Func<T, TParent>> expression, TValue value)
    {
        var parentPath = ToPath(expression);
        var path = parentPath + "." + NamePath(name);
        AddSetAction(path, value);
        return this;
    }

    public IPatchExpression<T> Set<TValue>(Expression<Func<T, TValue>> expression, TValue value)
    {
        var path = ToPath(expression);
        AddSetAction(path, value);
        return this;
    }

    public IPatchExpression<T> Duplicate<TElement>(Expression<Func<T, TElement>> expression,
        params Expression<Func<T, TElement>>[] destinations)
    {
        if (destinations.Length == 0)
            throw new ArgumentException("At least one destination must be given.");

        var sourcePath = ToPath(expression);
        var destPaths = destinations.Select(d => ToPath(d)).ToArray();

        // A nested destination might share an absent parent with another one, and only an action of
        // its own will see what the one before it created.
        var nested = destPaths.Where(d => JsonPathHelper.SplitPath(d).Length > 1).ToArray();
        var flat = destPaths.Except(nested).ToArray();

        if (flat.Length > 0)
        {
            _actions.Add(PatchOperation.DuplicateProperty(sourcePath, flat, _nativeJson));
        }

        foreach (var destination in nested)
        {
            _actions.Add(PatchOperation.DuplicateProperty(sourcePath, [destination], _nativeJson));
        }

        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, int>> expression, int increment = 1)
    {
        _actions.Add(PatchOperation.IncrementInt(ToPath(expression), increment, "int"));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, long>> expression, long increment = 1)
    {
        _actions.Add(PatchOperation.IncrementInt(ToPath(expression), increment, "bigint"));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, double>> expression, double increment = 1)
    {
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, "float", _nativeJson));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, float>> expression, float increment = 1)
    {
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, "real", _nativeJson));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, decimal>> expression, decimal increment = 1)
    {
        _actions.Add(PatchOperation.IncrementDecimal(ToPath(expression), increment, DecimalArithmeticType));
        return this;
    }

    public IPatchExpression<T> Append<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element)
    {
        var path = ToPath(expression);
        var json = ToJson(element);
        _actions.Add(IsObjectOrArray(json)
            ? PatchOperation.AppendComplex(path, json)
            : PatchOperation.AppendScalar(path, Scalar(element, json).Value));

        return this;
    }

    public IPatchExpression<T> Append<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression,
        string key, TElement element)
    {
        _actions.Add(BuildDictKeySet(ToPath(expression), Key(key), element));
        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element)
    {
        var path = ToPath(expression);
        var json = ToJson(element);
        _actions.Add(IsObjectOrArray(json)
            ? PatchOperation.AppendIfNotExistsComplex(path, json, _nativeJson)
            : PatchOperation.AppendIfNotExistsScalar(path, Scalar(element, json)));

        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(
        Expression<Func<T, IDictionary<string, TElement>>> expression, string key, TElement element)
    {
        var dictPath = ToPath(expression);
        var segment = Key(key);
        _actions.Add(PatchOperation.SetDictKeyIfNotExists(dictPath, segment, BuildDictKeySet(dictPath, segment, element),
            _nativeJson));
        return this;
    }

    public IPatchExpression<T> Insert<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, int? index = null)
    {
        if (!index.HasValue)
        {
            return Append(expression, element);
        }

        _actions.Add(PatchOperation.InsertAtIndex(ToPath(expression), index.Value, ToJson(element)));
        return this;
    }

    public IPatchExpression<T> InsertIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, int? index = null)
    {
        var path = ToPath(expression);
        var json = ToJson(element);
        _actions.Add(IsObjectOrArray(json)
            ? PatchOperation.InsertIfNotExistsComplex(path, json, index, _nativeJson)
            : PatchOperation.InsertIfNotExistsScalar(path, Scalar(element, json), index));

        return this;
    }

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, RemoveAction action = RemoveAction.RemoveFirst)
    {
        var path = ToPath(expression);
        var json = ToJson(element);

        if (IsObjectOrArray(json))
        {
            _actions.Add(action == RemoveAction.RemoveAll
                ? PatchOperation.RemoveComplexAll(path, json, _nativeJson)
                : PatchOperation.RemoveComplexFirst(path, json, _nativeJson));
        }
        else
        {
            var scalar = Scalar(element, json);
            _actions.Add(action == RemoveAction.RemoveAll
                ? PatchOperation.RemoveScalarAll(path, scalar)
                : PatchOperation.RemoveScalarFirst(path, scalar));
        }

        return this;
    }

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression,
        string key)
    {
        _actions.Add(PatchOperation.RemoveDictKey(ToPath(expression), Key(key)));
        return this;
    }

    public IPatchExpression<T> Rename(string oldName, Expression<Func<T, object>> expression)
    {
        // Lifted IPatchExpression<T> (jasperfx#331) uses the object-shaped Rename;
        // Polecat formerly exposed a generic Rename<TElement>. ToPath already unwraps
        // the boxing Convert in Func<T, object>.
        var newPath = ToPath(expression);
        var parts = JsonPathHelper.SplitPath(newPath);
        parts[^1] = NamePath(oldName);
        var oldPath = string.Join(".", parts);

        _actions.Add(PatchOperation.RenameProperty(oldPath, newPath, _nativeJson));
        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, Expression<Func<TElement, bool>> predicate)
        => throw new NotSupportedException(
            "Predicate-based AppendIfNotExists is not yet supported by Polecat's SQL Server JSON_MODIFY patch " +
            "translation (the predicate would need to be translated into a JSON array search). Use the " +
            "AppendIfNotExists(expression, element) overload instead.");

    public IPatchExpression<T> InsertIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, Expression<Func<TElement, bool>> predicate, int? index = null)
        => throw new NotSupportedException(
            "Predicate-based InsertIfNotExists is not yet supported by Polecat's SQL Server JSON_MODIFY patch " +
            "translation (the predicate would need to be translated into a JSON array search). Use the " +
            "InsertIfNotExists(expression, element, index) overload instead.");

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        Expression<Func<TElement, bool>> predicate, RemoveAction action = RemoveAction.RemoveFirst)
        => throw new NotSupportedException(
            "Predicate-based Remove is not yet supported by Polecat's SQL Server JSON_MODIFY patch translation " +
            "(the predicate would need to be translated into a JSON array search). Use the " +
            "Remove(expression, element, action) overload instead.");

    public IPatchExpression<T> Delete(string name)
    {
        _actions.Add(PatchOperation.DeleteProperty(NamePath(name)));
        return this;
    }

    public IPatchExpression<T> Delete<TParent>(string name, Expression<Func<T, TParent>> expression)
    {
        var parentPath = ToPath(expression);
        _actions.Add(PatchOperation.DeleteProperty(parentPath + "." + NamePath(name)));
        return this;
    }

    public IPatchExpression<T> Delete<TElement>(Expression<Func<T, TElement>> expression)
    {
        _actions.Add(PatchOperation.DeleteProperty(ToPath(expression)));
        return this;
    }

    /// <summary>
    ///     A member name from the API as a path. A dot in it has always meant a level down, so each
    ///     piece is a key of its own. Unlike a dictionary key, it takes the property naming policy.
    /// </summary>
    private string NamePath(string name)
        => string.Join(".", (_namingPolicy?.ConvertName(name) ?? name).Split('.')
            .Select(segment => JsonPathHelper.Segment(segment, _options.Serializer)));

    /// <summary>
    ///     Everything a patch needs to know about a value that isn't an object or an array, all of it
    ///     from the JSON it serializes to.
    /// </summary>
    /// <remarks>
    ///     ⚠️ <b>The CLR value is only bound where that is known to be right.</b> <c>JSON_MODIFY</c>
    ///     refuses a <see cref="Guid" /> or a <see cref="DateTimeOffset" /> as an argument, stores the
    ///     integer behind a string enum, and turns a <see cref="double" /> into something else. Only
    ///     what the serializer made of a value decides what is stored (#880), and only those it can be
    ///     trusted with are left as themselves, because a number written as text costs a rewrite of the
    ///     whole document. A bool goes in as a bit, which <c>JSON_MODIFY</c> renders as true or false.
    /// </remarks>
    private static ScalarElement Scalar<TValue>(TValue value, string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        return root.ValueKind switch
        {
            JsonValueKind.String => new ScalarElement(root.GetString(), json, root.GetString()!, PatchOperation.JsonKindString),
            JsonValueKind.True or JsonValueKind.False =>
                new ScalarElement(root.GetBoolean(), json, root.GetRawText(), PatchOperation.JsonKindBoolean),
            JsonValueKind.Null => new ScalarElement(null, json, json, PatchOperation.JsonKindNull),
            _ => new ScalarElement(value is byte or short or int or long or decimal ? value : new NumberText(json), json,
                root.GetRawText(), PatchOperation.JsonKindNumber)
        };
    }

    private void AddSetAction<TValue>(string path, TValue value)
    {
        if (value == null)
        {
            _actions.Add(PatchOperation.DeleteProperty(path));
            return;
        }

        var json = ToJson(value);
        _actions.Add(IsObjectOrArray(json) ? PatchOperation.SetComplex(path, json) : BuildScalarSet(path, value, json));
    }

    private Action<ICommandBuilder> BuildDictKeySet<TElement>(string dictPath, string key, TElement element)
    {
        var json = ToJson(element);
        return IsObjectOrArray(json)
            ? PatchOperation.SetDictKey(dictPath, key, json)
            : BuildScalarSet($"{dictPath}.{key}", element, json);
    }

    /// <summary>
    ///     Whether a value can go through the <c>JSON_QUERY</c> in <see cref="PatchOperation.SetComplex" />,
    ///     which refuses anything else as malformed JSON, and it does so before looking at any row even
    ///     where the expression sits in a <c>CASE</c> branch that would not run. This has to come from
    ///     the serialized value: a dictionary of strings has scalar values, and a <see cref="TimeSpan" />
    ///     or a <see cref="Uri" /> is stored as a JSON string, and what an <see cref="object" /> holds is
    ///     only known here.
    /// </summary>
    private static bool IsObjectOrArray(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array;
    }

    /// <summary>
    ///     Builds a scalar JSON_MODIFY assignment whose value matches exactly how the
    ///     property is persisted in the document body. See <see cref="Scalar{TValue}" />.
    /// </summary>
    private static Action<ICommandBuilder> BuildScalarSet<TValue>(string path, TValue value, string json)
    {
        var scalar = Scalar(value, json);
        return scalar.Kind == PatchOperation.JsonKindNull
            ? PatchOperation.DeleteProperty(path)
            : PatchOperation.SetScalar(path, scalar.Value);
    }
}
