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
    private readonly ISerializer _serializer;
    private readonly JsonNamingPolicy? _namingPolicy;
    private readonly bool _nativeJson;

    public PatchExpression(object id, string tenantId, DocumentSessionBase session)
    {
        _serializer = session.Options.Serializer;
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
        _serializer = session.Options.Serializer;
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

    private string ToPath(Expression expression) => JsonPathHelper.ToPath(expression, _namingPolicy);

    public IPatchExpression<T> Set<TValue>(string name, TValue value)
    {
        var path = FormatName(name);
        AddSetAction(path, value);
        return this;
    }

    public IPatchExpression<T> Set<TParent, TValue>(string name, Expression<Func<T, TParent>> expression, TValue value)
    {
        var parentPath = ToPath(expression);
        var path = parentPath + "." + FormatName(name);
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
        _actions.Add(PatchOperation.DuplicateProperty(sourcePath, destPaths, _nativeJson));
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
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, "float"));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, float>> expression, float increment = 1)
    {
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, "real"));
        return this;
    }

    public IPatchExpression<T> Increment(Expression<Func<T, decimal>> expression, decimal increment = 1)
    {
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, DecimalArithmeticType));
        return this;
    }

    public IPatchExpression<T> Append<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element)
    {
        var path = ToPath(expression);
        if (PatchOperation.IsScalarType(typeof(TElement)))
        {
            _actions.Add(PatchOperation.AppendScalar(path, element!));
        }
        else
        {
            _actions.Add(PatchOperation.AppendComplex(path, _serializer.ToJson(element!)));
        }

        return this;
    }

    public IPatchExpression<T> Append<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression,
        string key, TElement element)
    {
        _actions.Add(BuildDictKeySet(ToPath(expression), key, element));
        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element)
    {
        var path = ToPath(expression);
        if (PatchOperation.IsScalarType(typeof(TElement)))
        {
            _actions.Add(PatchOperation.AppendIfNotExistsScalar(path, element!, ElementText(element)));
        }
        else
        {
            _actions.Add(PatchOperation.AppendIfNotExistsComplex(path, _serializer.ToJson(element!)));
        }

        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(
        Expression<Func<T, IDictionary<string, TElement>>> expression, string key, TElement element)
    {
        var dictPath = ToPath(expression);
        _actions.Add(PatchOperation.SetDictKeyIfNotExists(dictPath, key, BuildDictKeySet(dictPath, key, element),
            _nativeJson));
        return this;
    }

    public IPatchExpression<T> Insert<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, int? index = null)
    {
        var path = ToPath(expression);
        var isComplex = !PatchOperation.IsScalarType(typeof(TElement));
        var jsonElement = isComplex ? _serializer.ToJson(element!) : null;

        if (index.HasValue)
        {
            _actions.Add(PatchOperation.InsertAtIndex(path, index.Value, element!, isComplex, jsonElement,
                _serializer));
        }
        else
        {
            _actions.Add(PatchOperation.InsertAtEnd(path, element!, isComplex, jsonElement));
        }

        return this;
    }

    public IPatchExpression<T> InsertIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, int? index = null)
    {
        var path = ToPath(expression);
        if (PatchOperation.IsScalarType(typeof(TElement)))
        {
            _actions.Add(PatchOperation.InsertIfNotExistsScalar(path, element!, ElementText(element), index, _serializer));
        }
        else
        {
            _actions.Add(PatchOperation.InsertIfNotExistsComplex(path, _serializer.ToJson(element!), index,
                _serializer));
        }

        return this;
    }

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element, RemoveAction action = RemoveAction.RemoveFirst)
    {
        var path = ToPath(expression);
        var isComplex = !PatchOperation.IsScalarType(typeof(TElement));

        if (isComplex)
        {
            var json = _serializer.ToJson(element!);
            _actions.Add(action == RemoveAction.RemoveAll
                ? PatchOperation.RemoveComplexAll(path, json)
                : PatchOperation.RemoveComplexFirst(path, json));
        }
        else
        {
            _actions.Add(action == RemoveAction.RemoveAll
                ? PatchOperation.RemoveScalarAll(path, ElementText(element))
                : PatchOperation.RemoveScalarFirst(path, ElementText(element)));
        }

        return this;
    }

    public IPatchExpression<T> Remove<TElement>(Expression<Func<T, IDictionary<string, TElement>>> expression,
        string key)
    {
        var dictPath = ToPath(expression);
        _actions.Add(PatchOperation.RemoveDictKey(dictPath, key));
        return this;
    }

    public IPatchExpression<T> Rename(string oldName, Expression<Func<T, object>> expression)
    {
        // Lifted IPatchExpression<T> (jasperfx#331) uses the object-shaped Rename;
        // Polecat formerly exposed a generic Rename<TElement>. ToPath already unwraps
        // the boxing Convert in Func<T, object>.
        var newPath = ToPath(expression);
        var parts = newPath.Split('.');
        parts[^1] = FormatName(oldName);
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
        _actions.Add(PatchOperation.DeleteProperty(FormatName(name)));
        return this;
    }

    public IPatchExpression<T> Delete<TParent>(string name, Expression<Func<T, TParent>> expression)
    {
        var parentPath = ToPath(expression);
        _actions.Add(PatchOperation.DeleteProperty(parentPath + "." + FormatName(name)));
        return this;
    }

    public IPatchExpression<T> Delete<TElement>(Expression<Func<T, TElement>> expression)
    {
        _actions.Add(PatchOperation.DeleteProperty(ToPath(expression)));
        return this;
    }

    private string FormatName(string name) => _namingPolicy?.ConvertName(name) ?? name;

    /// <summary>
    ///     The text OPENJSON's <c>value</c> column holds for this element, which is what finding it in an
    ///     array has to compare against. The CLR value bound as a parameter doesn't get there: a bool
    ///     casts to '1' where the column holds 'true', so it is never found, and a double or a date casts
    ///     to a different format from the one the serializer stored.
    /// </summary>
    private string ElementText<TElement>(TElement element)
    {
        using var doc = JsonDocument.Parse(_serializer.ToJson(element!));
        return doc.RootElement.ValueKind == JsonValueKind.String
            ? doc.RootElement.GetString()!
            : doc.RootElement.GetRawText();
    }

    private void AddSetAction<TValue>(string path, TValue value)
    {
        if (value == null)
        {
            _actions.Add(PatchOperation.DeleteProperty(path));
            return;
        }

        var json = _serializer.ToJson(value);
        _actions.Add(IsObjectOrArray(json) ? PatchOperation.SetComplex(path, json) : BuildScalarSet(path, value));
    }

    private Action<ICommandBuilder> BuildDictKeySet<TElement>(string dictPath, string key, TElement element)
    {
        var json = _serializer.ToJson(element!);
        return IsObjectOrArray(json)
            ? PatchOperation.SetDictKey(dictPath, key, json)
            : BuildScalarSet($"{dictPath}.{key}", element);
    }

    /// <summary>
    ///     Whether a value can go through the <c>JSON_QUERY</c> in <see cref="PatchOperation.SetComplex" />,
    ///     which refuses anything else as malformed JSON, and it does so before looking at any row even
    ///     where the expression sits in a <c>CASE</c> branch that would not run. This has to come from
    ///     the serialized value: a dictionary of strings has scalar values, and a <see cref="TimeSpan" />
    ///     or a <see cref="Uri" /> is not a scalar to <see cref="PatchOperation.IsScalarType" /> but is
    ///     stored as a JSON string.
    /// </summary>
    private static bool IsObjectOrArray(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.ValueKind is JsonValueKind.Object or JsonValueKind.Array;
    }

    /// <summary>
    ///     Builds a scalar JSON_MODIFY assignment whose value matches exactly how the
    ///     property is persisted in the document body. The value is serialized through the
    ///     configured serializer first, so enum storage (AsString vs AsInteger) and
    ///     System.Text.Json's date/time formatting are honored rather than binding the raw
    ///     CLR value — which would emit the wrong representation (e.g. the integer behind a
    ///     string enum) or a SQL type JSON_MODIFY rejects (e.g. datetimeoffset).
    /// </summary>
    private Action<ICommandBuilder> BuildScalarSet<TValue>(string path, TValue value)
    {
        var json = _serializer.ToJson(value!);
        using var doc = JsonDocument.Parse(json);
        switch (doc.RootElement.ValueKind)
        {
            case JsonValueKind.String:
                // JSON_MODIFY escapes and stores an nvarchar argument as a JSON string —
                // the right rendering for enum-as-string, DateTime/DateTimeOffset,
                // DateOnly/TimeOnly, Guid, and string values.
                return PatchOperation.SetScalar(path, doc.RootElement.GetString());

            case JsonValueKind.True:
            case JsonValueKind.False:
                // A bit argument is rendered as JSON true/false by JSON_MODIFY.
                return PatchOperation.SetScalar(path, doc.RootElement.GetBoolean());

            case JsonValueKind.Null:
                return PatchOperation.DeleteProperty(path);

            default:
                // Numeric: pass the original CLR value so JSON_MODIFY stores a JSON number.
                return PatchOperation.SetScalar(path, value);
        }
    }
}
