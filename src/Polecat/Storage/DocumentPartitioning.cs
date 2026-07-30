using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Weasel.Core.Partitioning;
using Weasel.SqlServer.Tables.Partitioning;

namespace Polecat.Storage;

/// <summary>
///     Describes declarative SQL Server RANGE partitioning for a document table on a caller-chosen
///     member (the SQL Server companion to Marten's <c>PartitionOn</c>). When the member is not the
///     identity, its value is promoted into a real "duplicated" column written on every upsert so the
///     row lands in the correct partition; that column is added to the primary key because SQL Server
///     requires the partitioning column to participate in the table's unique (clustered) index.
/// </summary>
internal sealed class DocumentPartitioning
{
    private readonly Func<object, object?>? _getter;

    private DocumentPartitioning(string columnName, string sqlDataType, bool partitionOnId,
        IReadOnlyList<object> boundaries, Func<object, object?>? getter, bool externallyManaged,
        ManagedRangePartitions? rollingWindow = null)
    {
        ColumnName = columnName;
        SqlDataType = sqlDataType;
        PartitionOnId = partitionOnId;
        Boundaries = boundaries;
        _getter = getter;
        ExternallyManaged = externallyManaged;
        RollingWindow = rollingWindow;
    }

    /// <summary>The partition column name. <c>id</c> when <see cref="PartitionOnId" /> is true.</summary>
    public string ColumnName { get; }

    /// <summary>The SQL Server data type of the partition column / function parameter.</summary>
    public string SqlDataType { get; }

    /// <summary>
    ///     True when partitioning directly on the identity column — no duplicated column is needed
    ///     and nothing extra is written, the existing <c>id</c> column is the partition column.
    /// </summary>
    public bool PartitionOnId { get; }

    /// <summary>The RANGE boundary values, as typed objects, in ascending order.</summary>
    public IReadOnlyList<object> Boundaries { get; }

    /// <summary>True when a real duplicated column must be created and written on upsert.</summary>
    public bool RequiresDuplicatedColumn => !PartitionOnId;

    /// <summary>
    ///     #255: when true, Polecat creates the partition function/scheme + table once (with the
    ///     initial <see cref="Boundaries" />) and thereafter does NOT reconcile the partitioning — the
    ///     partition boundaries are managed externally (app/DBA SPLIT/MERGE/SWITCH for monthly
    ///     time-series retention). The table migration runs with <c>AutoCreate.CreateOnly</c> so a
    ///     later schema apply never clobbers externally-managed partitions. When false (the default),
    ///     Polecat owns the boundaries and rolls them forward in place via SPLIT RANGE.
    /// </summary>
    public bool ExternallyManaged { get; }

    /// <summary>
    ///     #386: when set, the partition boundaries are a pure function of a
    ///     <see cref="Weasel.Core.Partitioning.RollingWindowPolicy" /> and the clock rather than a
    ///     caller-supplied list, and Weasel owns every statement that moves the window: NEXT USED +
    ///     SPLIT RANGE at the leading edge, partition TRUNCATE + MERGE RANGE at the trailing one.
    ///     <see cref="Boundaries" /> is empty in this mode — ask the manager for the current window.
    /// </summary>
    public ManagedRangePartitions? RollingWindow { get; }

    /// <summary>Extract the partition value from a document instance for the write path.</summary>
    public object GetValue(object document)
    {
        if (PartitionOnId)
        {
            throw new InvalidOperationException(
                "Partitioning is on the identity column; the id parameter is used directly.");
        }

        return _getter!(document)
               ?? throw new InvalidOperationException(
                   $"The partition column '{ColumnName}' resolved to null. A range-partition column is " +
                   "part of the primary key and must always have a value.");
    }

    /// <summary>
    ///     Resolve a member expression into a partitioning descriptor: the column name, its SQL Server
    ///     data type, whether it is the identity, a compiled value getter, and the typed boundaries.
    /// </summary>
    public static DocumentPartitioning For<T, TValue>(
        Expression<Func<T, TValue>> member,
        IReadOnlyList<TValue> boundaries,
        string idMemberName,
        bool externallyManaged = false)
    {
        var boxed = boundaries.Select(b => (object)b!).ToArray();
        var (column, sqlType, partitionOnId, getter) = Resolve(member, idMemberName);

        return new DocumentPartitioning(column, sqlType, partitionOnId, boxed, getter, externallyManaged);
    }

    /// <summary>
    ///     #386: resolve a member expression into a descriptor whose boundaries are owned by a rolling
    ///     time window rather than declared up front. Pass <paramref name="prebuilt" /> to share one
    ///     manager across several document types so a single pass rolls all of their tables forward;
    ///     otherwise a manager is built for the resolved column.
    /// </summary>
    public static DocumentPartitioning ForRollingWindow<T, TValue>(
        Expression<Func<T, TValue>> member,
        string idMemberName,
        RollingWindowPolicy policy,
        TimeProvider? timeProvider,
        ManagedRangePartitions? prebuilt)
    {
        var (column, sqlType, partitionOnId, getter) = Resolve(member, idMemberName);
        AssertTemporalPartitionKey<T>(policy, typeof(TValue), column);

        ManagedRangePartitions manager;
        if (prebuilt == null)
        {
            manager = new ManagedRangePartitions(policy, column, sqlType, timeProvider);
        }
        else
        {
            // A shared manager carries the column and function-parameter type with it, so it only fits
            // a document type whose partition member resolves to exactly that pair. Silently going with
            // the manager's column would partition the table on a column Polecat never promoted.
            AssertManagerMatchesMember<T>(prebuilt, column, sqlType);
            manager = prebuilt;
        }

        return new DocumentPartitioning(column, sqlType, partitionOnId, [], getter,
            externallyManaged: false, manager);
    }

    private static (string Column, string SqlDataType, bool PartitionOnId, Func<object, object?>? Getter)
        Resolve<T, TValue>(Expression<Func<T, TValue>> member, string idMemberName)
    {
        var memberInfo = ResolveMember(member);
        var sqlType = ToSqlServerType(typeof(TValue));

        if (string.Equals(memberInfo.Name, idMemberName, StringComparison.Ordinal))
        {
            return ("id", sqlType, true, null);
        }

        var compiled = member.Compile();

        return (ToSnakeCase(memberInfo.Name), sqlType, false, doc => compiled((T)doc));
    }

    /// <summary>
    ///     A rolling window is a function of the clock, so the partition key has to actually be a point
    ///     in time. Failing here — at configuration — turns what would otherwise surface as an opaque
    ///     SQL Server partition-function type error during the first migration into a message that names
    ///     the member.
    /// </summary>
    private static void AssertTemporalPartitionKey<T>(RollingWindowPolicy policy, Type valueType, string column)
    {
        var memberType = Nullable.GetUnderlyingType(valueType) ?? valueType;

        if (memberType != typeof(DateTimeOffset) && memberType != typeof(DateTime))
        {
            throw new InvalidOperationException(
                $"A rolling range partition ({policy}) has to be keyed on a DateTime or DateTimeOffset " +
                $"member, but '{column}' of {typeof(T).Name} is {memberType.Name}. Use " +
                "PartitionOn(...).ByRange(...) for a non-temporal partition key.");
        }
    }

    private static void AssertManagerMatchesMember<T>(ManagedRangePartitions manager, string column,
        string sqlDataType)
    {
        if (!string.Equals(manager.Column, column, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The supplied ManagedRangePartitions partitions on column '{manager.Column}', but the " +
                $"partition member of {typeof(T).Name} resolves to column '{column}'. A shared " +
                "rolling-window manager can only be used by document types whose partition member maps " +
                "to the same column name.");
        }

        if (!string.Equals(manager.SqlDataType, sqlDataType, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The supplied ManagedRangePartitions declares its partition function over " +
                $"'{manager.SqlDataType}', but the partition member of {typeof(T).Name} maps to " +
                $"'{sqlDataType}'.");
        }
    }

    private static MemberInfo ResolveMember<T, TValue>(Expression<Func<T, TValue>> member)
    {
        var body = member.Body;

        // Unwrap a Convert/ConvertChecked the compiler may insert (e.g. for object-typed lambdas).
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression memberExpression)
        {
            return memberExpression.Member;
        }

        throw new ArgumentException(
            "PartitionByRange requires a simple member expression such as 'x => x.BucketEnd'.", nameof(member));
    }

    internal static string ToSqlServerType(Type type)
    {
        var t = Nullable.GetUnderlyingType(type) ?? type;

        if (t == typeof(DateTimeOffset)) return "datetimeoffset";
        if (t == typeof(DateTime)) return "datetime2";
        if (t == typeof(DateOnly)) return "date";
        if (t == typeof(int)) return "int";
        if (t == typeof(long)) return "bigint";
        if (t == typeof(short)) return "smallint";
        if (t == typeof(Guid)) return "uniqueidentifier";

        throw new NotSupportedException(
            $"Range partitioning a document table on a '{t.Name}' column is not supported. " +
            "Use a date (DateTimeOffset/DateTime/DateOnly) or integer member.");
    }

    internal static string ToSnakeCase(string name)
    {
        var builder = new StringBuilder(name.Length + 8);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0 && (!char.IsUpper(name[i - 1]) || (i + 1 < name.Length && char.IsLower(name[i + 1]))))
                {
                    builder.Append('_');
                }

                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }
}
