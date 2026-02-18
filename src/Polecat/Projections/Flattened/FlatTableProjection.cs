using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JasperFx.Events;
using JasperFx.Events.Daemon;
using JasperFx.Events.Descriptors;
using JasperFx.Events.Projections;
using JasperFx.Events.Subscriptions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Polecat.Events.Daemon;
using Polecat.Internal;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Projections.Flattened;

/// <summary>
///     Projects events directly into a custom SQL table with declarative column mappings.
///     Uses SQL Server MERGE statements for upsert operations.
///
///     Usage:
///     <code>
///     public class QuestMetricsProjection : FlatTableProjection
///     {
///         public QuestMetricsProjection() : base("quest_metrics")
///         {
///             Table.AddColumn("id", "uniqueidentifier").AsPrimaryKey();
///             Table.AddColumn("quest_name", "varchar(200)");
///             Table.AddColumn("member_count", "int");
///
///             Project&lt;QuestStarted&gt;(map =>
///             {
///                 map.Map(x => x.Name, "quest_name");
///                 map.SetValue("member_count", 0);
///             });
///
///             Delete&lt;QuestEnded&gt;();
///         }
///     }
///     </code>
/// </summary>
public abstract class FlatTableProjection : ProjectionBase,
    IProjectionSource<IDocumentSession, IQuerySession>,
    ISubscriptionFactory<IDocumentSession, IQuerySession>,
    IInlineProjection<IDocumentSession>,
    IJasperFxProjection<IDocumentSession>
{
    private readonly Dictionary<Type, IFlatTableEventHandler> _handlers = new();
    private bool _compiled;
    private bool _tableEnsured;

    protected FlatTableProjection(string tableName, string? schemaName = null)
    {
        var schema = schemaName ?? "dbo";
        Table = new Table(new SqlServerObjectName(schema, tableName));
        Name = GetType().FullName ?? GetType().Name;
    }

    public Table Table { get; }

    /// <summary>
    ///     Register a projection handler for an event type with column mapping configuration.
    /// </summary>
    protected void Project<TEvent>(Action<StatementMap<TEvent>> configure,
        Expression<Func<TEvent, object>>? primaryKeySource = null)
    {
        var pkMembers = primaryKeySource != null ? GetMemberPath(primaryKeySource) : null;
        var map = new StatementMap<TEvent>(this, pkMembers);
        configure(map);
        _handlers[typeof(TEvent)] = map;

        IncludeType<TEvent>();
    }

    /// <summary>
    ///     Register a delete handler for an event type.
    /// </summary>
    protected void Delete<TEvent>(Expression<Func<TEvent, object>>? primaryKeySource = null)
    {
        var pkMembers = primaryKeySource != null ? GetMemberPath(primaryKeySource) : null;
        _handlers[typeof(TEvent)] = new EventDeleter<TEvent>(this, pkMembers);

        IncludeType<TEvent>();
    }

    public override void AssembleAndAssertValidity()
    {
        if (Table.PrimaryKeyColumns.Count == 0)
            throw new InvalidOperationException(
                $"FlatTableProjection table '{Table.Identifier}' must have at least one primary key column.");

        if (_handlers.Count == 0)
            throw new InvalidOperationException(
                $"FlatTableProjection '{GetType().Name}' must have at least one Project<T>() or Delete<T>() registration.");
    }

    internal void Compile(Events.EventGraph events)
    {
        if (_compiled) return;

        foreach (var handler in _handlers.Values)
        {
            handler.Compile(events);
        }

        _compiled = true;
    }

    private void LazyCompile(DocumentSessionBase sessionBase)
    {
        if (!_compiled)
        {
            Compile(sessionBase.EventGraph);
        }
    }

    // IInlineProjection<IDocumentSession>
    public async Task ApplyAsync(IDocumentSession operations, IReadOnlyList<StreamAction> streams,
        CancellationToken cancellation)
    {
        if (operations is not DocumentSessionBase sessionBase) return;

        LazyCompile(sessionBase);

        // Ensure table exists on first use
        if (!_tableEnsured)
        {
            await EnsureTableAsync(sessionBase, cancellation);
            _tableEnsured = true;
        }

        foreach (var stream in streams)
        {
            foreach (var e in stream.Events)
            {
                if (_handlers.TryGetValue(e.EventType, out var handler))
                {
                    var op = handler.CreateOperation(e);
                    sessionBase.WorkTracker.Add(op);
                }
            }
        }
    }

    // IJasperFxProjection<IDocumentSession> — used by async daemon
    public async Task ApplyAsync(IDocumentSession operations, IReadOnlyList<IEvent> events,
        CancellationToken cancellation)
    {
        if (operations is not DocumentSessionBase sessionBase) return;

        LazyCompile(sessionBase);

        // Ensure table exists on first use
        if (!_tableEnsured)
        {
            await EnsureTableAsync(sessionBase, cancellation);
            _tableEnsured = true;
        }

        foreach (var e in events)
        {
            if (_handlers.TryGetValue(e.EventType, out var handler))
            {
                var op = handler.CreateOperation(e);
                sessionBase.WorkTracker.Add(op);
            }
        }
    }

    // IProjectionSource<IDocumentSession, IQuerySession>
    public bool TryBuildReplayExecutor(IEventStore<IDocumentSession, IQuerySession> store,
        IEventDatabase database, [NotNullWhen(true)] out IReplayExecutor? executor)
    {
        executor = null;
        return false;
    }

    public IInlineProjection<IDocumentSession> BuildForInline()
    {
        return this;
    }

    public override IEnumerable<Type> PublishedTypes()
    {
        return Enumerable.Empty<Type>();
    }

    // ISubscriptionSource members
    public SubscriptionType Type => SubscriptionType.EventProjection;
    public ShardName[] ShardNames() => [new ShardName(Name, ShardName.All, Version)];
    public Type ImplementationType => GetType();

    public SubscriptionDescriptor Describe(IEventStore store)
    {
        return new SubscriptionDescriptor(this, store);
    }

    IReadOnlyList<AsyncShard<IDocumentSession, IQuerySession>>
        ISubscriptionSource<IDocumentSession, IQuerySession>.Shards()
    {
        return
        [
            new AsyncShard<IDocumentSession, IQuerySession>(
                Options, ShardRole.Projection,
                new ShardName(Name, ShardName.All, Version),
                this, this)
        ];
    }

    // ISubscriptionFactory<IDocumentSession, IQuerySession>
    public ISubscriptionExecution BuildExecution(
        IEventStore<IDocumentSession, IQuerySession> store,
        IEventDatabase database,
        ILoggerFactory loggerFactory,
        ShardName shardName)
    {
        var logger = loggerFactory.CreateLogger(GetType());
        return new ProjectionExecution<IDocumentSession, IQuerySession>(
            shardName, Options, store, database, this, logger);
    }

    public ISubscriptionExecution BuildExecution(
        IEventStore<IDocumentSession, IQuerySession> store,
        IEventDatabase database,
        ILogger logger,
        ShardName shardName)
    {
        return new ProjectionExecution<IDocumentSession, IQuerySession>(
            shardName, Options, store, database, this, logger);
    }

    private async Task EnsureTableAsync(DocumentSessionBase session, CancellationToken cancellation)
    {
        var conn = await session.GetConnectionAsync(cancellation);

        await using var cmd = conn.CreateCommand();
        if (session.ActiveTransaction != null) cmd.Transaction = session.ActiveTransaction;

        cmd.CommandText = $"""
            IF NOT EXISTS (SELECT * FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{Table.Identifier.Schema}' AND t.name = '{Table.Identifier.Name}')
            BEGIN
                {GenerateCreateTableDdl()}
            END
            """;

        await cmd.ExecuteNonQueryAsync(cancellation);
    }

    private string GenerateCreateTableDdl()
    {
        var columns = Table.Columns.Select(c =>
        {
            var nullable = c.AllowNulls && !c.IsPrimaryKey ? "NULL" : "NOT NULL";
            var identity = c.IsAutoNumber ? " IDENTITY(1,1)" : "";
            return $"[{c.Name}] {c.Type}{identity} {nullable}";
        });

        var pkColumns = Table.PrimaryKeyColumns;
        var pkConstraint = pkColumns.Count > 0
            ? $", CONSTRAINT [PK_{Table.Identifier.Name}] PRIMARY KEY ({string.Join(", ", pkColumns.Select(c => $"[{c}]"))})"
            : "";

        return $"""
            CREATE TABLE [{Table.Identifier.Schema}].[{Table.Identifier.Name}] (
                {string.Join(",\n            ", columns)}{pkConstraint}
            );
            """;
    }

    private static MemberInfo[]? GetMemberPath<T>(Expression<Func<T, object>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression unary) body = unary.Operand;

        var members = new List<MemberInfo>();
        while (body is MemberExpression memberExpr)
        {
            members.Insert(0, memberExpr.Member);
            body = memberExpr.Expression!;
        }

        return members.Count > 0 ? members.ToArray() : null;
    }
}
