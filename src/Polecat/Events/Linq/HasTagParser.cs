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
        var schema = _events.DatabaseSchemaName;
        var suffix = registration.TableSuffix;

        // Under conjoined tenancy a tag value is only unique per tenant, so the correlated subquery must
        // also match the outer event row's tenant_id (the outer query is already tenant-scoped).
        var correlation = _events.TenancyStyle == JasperFx.MultiTenancy.TenancyStyle.Conjoined
            ? " AND pt.tenant_id = [pc_events].[tenant_id]"
            : string.Empty;

        return new HasTagFilter(
            $"seq_id IN (SELECT pt.seq_id FROM [{schema}].[pc_event_tag_{suffix}] pt WHERE pt.value = ",
            extracted,
            $"{correlation})");
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
