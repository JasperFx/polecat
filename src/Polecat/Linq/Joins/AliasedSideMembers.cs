using JasperFx;
using System.Linq.Expressions;
using Polecat.Linq.Members;

namespace Polecat.Linq.Joins;

/// <summary>
///     gh-1012 (marten#5968): the members of a joined row for <see cref="Parsing.ScalarTranslation" />, each read
///     from its own side's table alias, so a key computed from both sides, <c>r =&gt; r.Rank * -3 + r.Amount</c>,
///     reads each member from the table it belongs to.
/// </summary>
internal sealed class AliasedSideMembers : IMemberResolver
{
    private readonly ParameterExpression _outerParameter;
    private readonly ParameterExpression _innerParameter;
    private readonly IMemberResolver _outer;
    private readonly IMemberResolver _inner;

    public AliasedSideMembers(LambdaExpression lambda, IMemberResolver outer, IMemberResolver inner)
    {
        _outerParameter = lambda.Parameters[0];
        _innerParameter = lambda.Parameters[1];
        _outer = outer;
        _inner = inner;
    }

    private (IMemberResolver Members, string Alias) Side(Expression expression)
    {
        var root = FlattenedRowMembers.RootOf(expression);
        if (root == _outerParameter) return (_outer, JoinSides.OuterAlias);
        if (root == _innerParameter) return (_inner, JoinSides.InnerAlias);

        throw new BadLinqExpressionException(
            $"Polecat cannot translate '{expression}' to SQL: it is not a member of either joined document.");
    }

    public IQueryableMember ResolveMember(MemberExpression expression)
    {
        var (members, alias) = Side(expression);
        return new AliasedMember(members.ResolveMember(expression), alias);
    }

    public IQueryableMember ResolvePath(Expression expression)
    {
        var (members, alias) = Side(expression);
        return new AliasedMember(members.ResolvePath(expression), alias);
    }

    /// <summary>A member of one side, its locators qualified with that side's alias.</summary>
    private sealed class AliasedMember(IQueryableMember inner, string alias) : IQueryableMember
    {
        private string Aliased(string locator) => JoinStatement.AliasLocator(locator, alias);

        public Type MemberType => inner.MemberType;
        public string TypedLocator => Aliased(inner.TypedLocator);
        public string RawLocator => Aliased(inner.RawLocator);
        public bool IsBoolean => inner.IsBoolean;
        public string? LocatorSqlType => inner.LocatorSqlType;
        public int? TextCapacity => inner.TextCapacity;
        public string NullLocator => Aliased(inner.NullLocator);
        public bool MayBeObject => inner.MayBeObject;
        public string? BareTextLocator => inner.BareTextLocator == null ? null : Aliased(inner.BareTextLocator);
        public string? JsonLocator => inner.JsonLocator == null ? null : Aliased(inner.JsonLocator);
        public object? ConvertValue(object? value) => inner.ConvertValue(value);
    }
}
