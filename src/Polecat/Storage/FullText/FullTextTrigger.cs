using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Triggers;

namespace Polecat.Storage.FullText;

/// <summary>
///     #685 — the trigger that keeps <see cref="FullTextTokenTable" /> in step with the document
///     table, modeled as a Weasel <see cref="Trigger" /> rather than rendered as raw DDL at first use.
/// </summary>
/// <remarks>
///     <para>
///         <b>One trigger per table, covering every declared member, and that is load-bearing.</b>
///         A per-index trigger would have the second declared member's create replace the first
///         member's trigger — leaving the first silently unmaintained, which is the same shape of
///         quiet failure the backfill exists to prevent. The body therefore folds every member into
///         one pass, which is why this takes the whole collection.
///     </para>
///     <para>
///         Weasel renders <c>DROP TRIGGER IF EXISTS</c> followed by a <c>CREATE TRIGGER</c> inside
///         <c>EXEC sp_executesql</c> (a create has to begin its batch), where this used to emit
///         <c>CREATE OR ALTER</c>. Same end state, and the drop/create pair is what makes the object
///         expressible as a delta: <see cref="Trigger.CreateDeltaAsync" /> compares the body
///         <c>sys.sql_modules</c> hands back against the one declared here, so a re-declaration is an
///         Update and an unchanged declaration is None.
///     </para>
/// </remarks>
internal class FullTextTrigger: Trigger
{
    public FullTextTrigger(DocumentMapping mapping, IReadOnlyList<FullTextIndex> indexes)
        : base(
            new SqlServerObjectName(mapping.DatabaseSchemaName, NameFor(mapping)),
            new SqlServerObjectName(mapping.DatabaseSchemaName, mapping.TableName),
            BodyFor(mapping, indexes))
    {
        Timing = TriggerTiming.After;
        Events = TriggerEvents.Insert | TriggerEvents.Update | TriggerEvents.Delete;
    }

    internal static string NameFor(DocumentMapping mapping)
        => "tr_" + FullTextIndex.TableNameFor(mapping);

    private static string BodyFor(DocumentMapping mapping, IReadOnlyList<FullTextIndex> indexes)
    {
        var ftTable = SqlEscaping.QualifiedName(mapping.DatabaseSchemaName, FullTextIndex.TableNameFor(mapping));
        var tenant = FullTextIndex.TenantColumn(mapping);
        var deletedTenant = FullTextIndex.DeletedTenantPredicate(mapping);

        // One SELECT per declared member, fused into a single INSERT.
        var legs = string.Join("\n    UNION ALL\n", indexes.Select(x =>
            $"""
                 SELECT i.id, {tenant}, {SqlEscaping.Literal(x.MemberName)}, s.value, s.ordinal
                 FROM inserted i
                 {x.TokenizeClause("i")}
             """));

        return $"""
                BEGIN
                    SET NOCOUNT ON;
                    DELETE ft FROM {ftTable} ft INNER JOIN deleted d ON ft.doc_id = d.id{deletedTenant};
                    INSERT INTO {ftTable} (doc_id, tenant_id, member, term, pos)
                {legs}
                END
                """;
    }
}
