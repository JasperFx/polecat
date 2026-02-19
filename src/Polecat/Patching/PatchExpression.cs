using System.Linq.Expressions;
using System.Text.Json;
using Polecat.Internal;
using Polecat.Linq.SqlGeneration;
using Polecat.Serialization;

namespace Polecat.Patching;

internal class PatchExpression<T> : IPatchExpression<T>
{
    private readonly List<Action<CommandBuilder>> _actions = new();
    private readonly ISerializer _serializer;
    private readonly JsonNamingPolicy? _namingPolicy;

    public PatchExpression(object id, string tenantId, DocumentSessionBase session)
    {
        _serializer = session.Options.Serializer;
        _namingPolicy = GetNamingPolicy(session);

        var provider = session.Providers.GetProvider<T>();
        var mapping = provider.Mapping;
        var operation = new PatchOperation(mapping, _actions, builder =>
        {
            builder.Append("id = ");
            builder.AppendParameter(id);
            builder.Append(" AND tenant_id = ");
            builder.AppendParameter(tenantId);
        });
        session.WorkTracker.Add(operation);
    }

    public PatchExpression(Expression<Func<T, bool>> filter, DocumentSessionBase session)
    {
        _serializer = session.Options.Serializer;
        _namingPolicy = GetNamingPolicy(session);

        var provider = session.Providers.GetProvider<T>();
        var mapping = provider.Mapping;

        var memberFactory = new Linq.Members.MemberFactory(session.Options, mapping);
        var whereParser = new Linq.Parsing.WhereClauseParser(memberFactory);
        var fragment = whereParser.Parse(filter.Body);

        var operation = new PatchOperation(mapping, _actions, builder =>
        {
            builder.Append("tenant_id = ");
            builder.AppendParameter(session.TenantId);
            builder.Append(" AND ");
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
        _actions.Add(PatchOperation.DuplicateProperty(sourcePath, destPaths));
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
        _actions.Add(PatchOperation.IncrementFloat(ToPath(expression), increment, "decimal(18,6)"));
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
        var dictPath = ToPath(expression);
        var jsonElement = _serializer.ToJson(element!);
        _actions.Add(PatchOperation.SetDictKey(dictPath, key, jsonElement));
        return this;
    }

    public IPatchExpression<T> AppendIfNotExists<TElement>(Expression<Func<T, IEnumerable<TElement>>> expression,
        TElement element)
    {
        var path = ToPath(expression);
        if (PatchOperation.IsScalarType(typeof(TElement)))
        {
            _actions.Add(PatchOperation.AppendIfNotExistsScalar(path, element!));
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
        var jsonElement = _serializer.ToJson(element!);
        _actions.Add(PatchOperation.SetDictKeyIfNotExists(dictPath, key, jsonElement));
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
            _actions.Add(PatchOperation.InsertIfNotExistsScalar(path, element!, index, _serializer));
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
                ? PatchOperation.RemoveScalarAll(path, element!)
                : PatchOperation.RemoveScalarFirst(path, element!));
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
        var newPath = ToPath(expression);
        var parts = newPath.Split('.');
        var to = parts[^1];
        parts[^1] = FormatName(oldName);
        var oldPath = string.Join(".", parts);

        _actions.Add(PatchOperation.RenameProperty(oldPath, newPath));
        return this;
    }

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

    private void AddSetAction<TValue>(string path, TValue value)
    {
        if (value == null)
        {
            _actions.Add(PatchOperation.DeleteProperty(path));
        }
        else if (PatchOperation.IsScalarType(typeof(TValue)))
        {
            _actions.Add(PatchOperation.SetScalar(path, value));
        }
        else
        {
            _actions.Add(PatchOperation.SetComplex(path, _serializer.ToJson(value)));
        }
    }
}
