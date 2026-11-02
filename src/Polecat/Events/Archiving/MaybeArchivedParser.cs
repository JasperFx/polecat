using System.Linq.Expressions;
using Polecat.Linq.Members;
using Polecat.Linq.Parsing.Methods;
using Polecat.Linq.SqlGeneration;
using Weasel.SqlServer;

namespace Polecat.Events.Archiving;

/// <summary>
///     Compiles the <see cref="ArchivedEventExtensions.MaybeArchived" /> marker. It matches every row, and
///     the opt-in itself is <see cref="ArchivalPredicates" />' to see. Only wired into event-store LINQ
///     queries (QueryAllRawEvents), where the rows are events.
/// </summary>
internal class MaybeArchivedParser : IMethodCallParser
{
    public bool Matches(MethodCallExpression expression)
    {
        return expression.Method.Name == nameof(ArchivedEventExtensions.MaybeArchived)
               && expression.Method.DeclaringType == typeof(ArchivedEventExtensions);
    }

    public ISqlFragment Parse(IMemberResolver memberFactory, MethodCallExpression expression)
    {
        return new WhereFragment("1=1");
    }
}
