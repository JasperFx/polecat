using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Linq;

/// <summary>
///     Compiles the <see cref="LinqExtensions.HasTag{TTag}" /> marker method into the same tag SQL that
///     <c>QueryByTagsAsync</c> emits: a correlated <c>seq_id IN (SELECT seq_id FROM pc_event_tag_{suffix}
///     WHERE value = @p)</c> subquery against the registered tag type's table. Under conjoined tenancy the
///     subquery also correlates on tenant_id so a tag value shared across tenants can't leak rows. Only
///     wired into event-store LINQ queries (QueryAllRawEvents), where the outer table is pc_events.
/// </summary>
internal class HasTagParser : IMethodCallParser
{
    private readonly EventGraph _events;

    public HasTagParser(EventGraph events)
    {
        _events = events;
    }

    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == nameof(LinqExtensions.HasTag)
               && expression.Method.DeclaringType == typeof(LinqExtensions);
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var tagType = expression.Method.GetGenericArguments()[0];
        var value = WhereClauseParser.ExtractValue(expression.Arguments[^1])
                    ?? throw new ArgumentException("HasTag() requires a non-null tag value.", nameof(expression));

        var registration = _events.FindTagType(tagType)
                           ?? throw new InvalidOperationException(
                               $"Tag type '{tagType.Name}' is not registered. Call RegisterTagType<{tagType.Name}>() first.");

        var extracted = registration.ExtractValue(value);
        var tagTable = _events.TagTableName(registration);

        // Under conjoined tenancy a tag value is only unique per tenant, so the correlated subquery must
        // also match the outer event row's tenant_id (the outer query is already tenant-scoped).
        var correlation = _events.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined
            ? " AND pt.tenant_id = [pc_events].[tenant_id]"
            : string.Empty;

        return new HasTagFilter(
            $"seq_id IN (SELECT pt.seq_id FROM {tagTable} pt WHERE pt.value = ",
            extracted,
            $"{correlation})");
    }
}

/// <summary>
///     Compiles <see cref="LinqExtensions.HasTagValue{TTag}" /> — the lossy name/value tag match behind
///     <c>EventQuery.TagValues</c> (jasperfx#801 / polecat#575) — into the same correlated
///     <c>seq_id IN (SELECT …)</c> shape <see cref="HasTagParser" /> emits, differing only in the
///     comparison: the stored value is rendered to <c>nvarchar</c> and compared case-insensitively,
///     because the caller supplied a string and there is no typed value to compare.
/// </summary>
/// <remarks>
///     A sub-select rather than a join, for the same reason the DCB path uses one: an event carrying
///     the tag twice must read back once, or <c>PagedEvents.TotalCount</c> counts condition hits
///     instead of distinct events and paging walks a longer list than it reports.
/// </remarks>
internal class HasTagValueParser : IMethodCallParser
{
    private readonly EventGraph _events;

    public HasTagValueParser(EventGraph events)
    {
        _events = events;
    }

    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == nameof(LinqExtensions.HasTagValue)
               && expression.Method.DeclaringType == typeof(LinqExtensions);
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        var tagType = expression.Method.GetGenericArguments()[0];
        var value = WhereClauseParser.ExtractValue(expression.Arguments[^1]) as string
                    ?? throw new ArgumentException("HasTagValue() requires a non-null tag value.",
                        nameof(expression));

        var registration = _events.FindTagType(tagType)
                           ?? throw new InvalidOperationException(
                               $"Tag type '{tagType.Name}' is not registered. Call RegisterTagType<{tagType.Name}>() first.");

        var tagTable = _events.TagTableName(registration);

        // Same tenancy correlation as HasTagParser: under conjoined tenancy a tag value is unique only
        // per tenant, so without this a value shared across tenants leaks rows into a tenant-scoped read.
        var correlation = _events.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined
            ? " AND pt.tenant_id = [pc_events].[tenant_id]"
            : string.Empty;

        // CONVERT before LOWER so a non-string value column (uniqueidentifier, int, datetimeoffset)
        // is compared on the same rendering the caller typed. LOWER on both sides rather than relying
        // on the database collation, which a deployment can set case-sensitive.
        return new HasTagFilter(
            $"seq_id IN (SELECT pt.seq_id FROM {tagTable} pt WHERE LOWER(CONVERT(nvarchar(4000), pt.value)) = LOWER(",
            value,
            $"){correlation})");
    }
}

/// <summary>
///     WHERE fragment with a single bound parameter spliced between two literal SQL segments.
/// </summary>
internal class HasTagFilter : ISqlFragment
{
    private readonly string _prefix;
    private readonly object _value;
    private readonly string _suffix;

    public HasTagFilter(string prefix, object value, string suffix)
    {
        _prefix = prefix;
        _value = value;
        _suffix = suffix;
    }

    public void Apply(ICommandBuilder builder)
    {
        builder.Append(_prefix);
        builder.AppendParameter(_value);
        builder.Append(_suffix);
    }
}
