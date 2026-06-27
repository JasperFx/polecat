using System.Linq.Expressions;
using System.Reflection;
using System.Text;

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
        IReadOnlyList<object> boundaries, Func<object, object?>? getter, bool externallyManaged)
    {
        ColumnName = columnName;
        SqlDataType = sqlDataType;
        PartitionOnId = partitionOnId;
        Boundaries = boundaries;
        _getter = getter;
        ExternallyManaged = externallyManaged;
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
        var memberInfo = ResolveMember(member);
        var sqlType = ToSqlServerType(typeof(TValue));
        var boxed = boundaries.Select(b => (object)b!).ToArray();

        if (string.Equals(memberInfo.Name, idMemberName, StringComparison.Ordinal))
        {
            return new DocumentPartitioning("id", sqlType, partitionOnId: true, boxed, getter: null,
                externallyManaged);
        }

        var column = ToSnakeCase(memberInfo.Name);
        var compiled = member.Compile();
        Func<object, object?> getter = doc => compiled((T)doc);

        return new DocumentPartitioning(column, sqlType, partitionOnId: false, boxed, getter,
            externallyManaged);
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
