using System.Linq.Expressions;
using System.Reflection;

namespace Polecat.Storage.Metadata;

/// <summary>
///     #243: fluent accessor for one metadata column inside the
///     <c>Schema.For&lt;T&gt;().Metadata(m => ...)</c> DSL. Mirrors Marten's per-column metadata
///     surface: toggle <see cref="Enabled" /> and/or <see cref="MapTo" /> a document member.
/// </summary>
public class MetadataColumnExpression<T>
{
    private readonly MetadataColumn _column;

    internal MetadataColumnExpression(MetadataColumn column)
    {
        _column = column;
    }

    /// <summary>
    ///     Whether this metadata column is persisted for <typeparamref name="T" />.
    /// </summary>
    public bool Enabled
    {
        get => _column.Enabled;
        set => _column.Enabled = value;
    }

    /// <summary>
    ///     Map the stored metadata value onto a member of the document, and enable the column.
    ///     Mirrors Marten's <c>MapTo(x => x.Member)</c>.
    /// </summary>
    public MetadataColumnExpression<T> MapTo(Expression<Func<T, object?>> member)
    {
        _column.Member = MetadataMemberResolver.Resolve(member);
        _column.Enabled = true;
        return this;
    }
}

/// <summary>
///     #243: the object passed to <c>Schema.For&lt;T&gt;().Metadata(m => ...)</c>. Exposes each
///     metadata column as a <see cref="MetadataColumnExpression{T}" /> over a shared
///     <see cref="DocumentMetadataConfig" />. Mirrors Marten's <c>MetadataConfig</c>.
/// </summary>
public class MetadataConfig<T>
{
    private readonly DocumentMetadataConfig _config;

    internal MetadataConfig(DocumentMetadataConfig config)
    {
        _config = config;
    }

    public MetadataColumnExpression<T> CorrelationId => new(_config.CorrelationId);
    public MetadataColumnExpression<T> CausationId => new(_config.CausationId);
    public MetadataColumnExpression<T> LastModifiedBy => new(_config.LastModifiedBy);
    public MetadataColumnExpression<T> Headers => new(_config.Headers);
    public MetadataColumnExpression<T> CreatedAt => new(_config.CreatedAt);
    public MetadataColumnExpression<T> LastModified => new(_config.LastModified);
    public MetadataColumnExpression<T> Version => new(_config.Version);
    public MetadataColumnExpression<T> TenantId => new(_config.TenantId);
    public MetadataColumnExpression<T> IsSoftDeleted => new(_config.IsSoftDeleted);
    public MetadataColumnExpression<T> DotNetType => new(_config.DotNetType);
}

/// <summary>
///     Resolves a <c>x =&gt; x.Member</c> lambda (optionally wrapped in a Convert for value types)
///     to the underlying <see cref="MemberInfo" />.
/// </summary>
internal static class MetadataMemberResolver
{
    public static MemberInfo Resolve<T>(Expression<Func<T, object?>> expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression member)
        {
            return member.Member;
        }

        throw new ArgumentException(
            $"Expression '{expression}' is not a supported metadata member mapping. " +
            "Use a single property (x => x.Member).");
    }
}
