using System.Linq.Expressions;
using Polecat.Batching;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.SqlGeneration;
using Polecat.Metadata;
using Polecat.Serialization;

namespace Polecat.Internal.Batching;

internal class BatchedQueryable<T> : IBatchedQueryable<T> where T : class
{
    private readonly BatchedQuery _batch;
    private readonly DocumentProvider _provider;
    private readonly StoreOptions _options;
    private readonly string _tenantId;
    private readonly ISerializer _serializer;
    private readonly List<Expression<Func<T, bool>>> _predicates = [];

    public BatchedQueryable(BatchedQuery batch, DocumentProvider provider, StoreOptions options,
        string tenantId, ISerializer serializer)
    {
        _batch = batch;
        _provider = provider;
        _options = options;
        _tenantId = tenantId;
        _serializer = serializer;
    }

    public IBatchedQueryable<T> Where(Expression<Func<T, bool>> predicate)
    {
        _predicates.Add(predicate);
        return this;
    }

    public Task<IReadOnlyList<T>> ToList()
    {
        var statement = BuildStatement();
        var item = new QueryListBatchItem<T>(statement, _serializer);
        _batch.AddItem(item);
        return item.Result;
    }

    public Task<int> Count()
    {
        var statement = BuildStatement();
        var item = new QueryCountBatchItem(statement);
        _batch.AddItem(item);
        return item.Result;
    }

    public Task<bool> Any()
    {
        var statement = BuildStatement();
        var item = new QueryAnyBatchItem(statement);
        _batch.AddItem(item);
        return item.Result;
    }

    public Task<T?> FirstOrDefault()
    {
        var statement = BuildStatement();
        var item = new QueryFirstOrDefaultBatchItem<T>(statement, _serializer);
        _batch.AddItem(item);
        return item.Result;
    }

    private Statement BuildStatement()
    {
        var memberFactory = new MemberFactory(_options, _provider.Mapping);
        var whereParser = new WhereClauseParser(memberFactory);

        var statement = new Statement
        {
            FromTable = _provider.Mapping.QualifiedTableName,
            SelectColumns = "data"
        };

        // Apply Where predicates
        foreach (var predicate in _predicates)
        {
            var fragment = whereParser.Parse(predicate.Body);
            statement.Wheres.Add(fragment);
        }

        // Tenant filter
        statement.Wheres.Add(new ComparisonFilter("tenant_id", "=", _tenantId));

        // Soft delete filter
        if (_provider.Mapping.DeleteStyle == DeleteStyle.SoftDelete)
        {
            statement.Wheres.Add(new LiteralSqlFragment("is_deleted = 0"));
        }

        return statement;
    }
}
