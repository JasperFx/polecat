using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage.FullText;

/// <summary>
///     #685 — the inverted-index side table behind <see cref="FullTextIndex" />, modeled as a Weasel
///     table rather than rendered as raw DDL at first use. One row per token, keyed for lookup by
///     term.
/// </summary>
/// <remarks>
///     <para>
///         Why a table and a trigger as <b>two objects</b> rather than one composite
///         <c>ISchemaObject</c> owning the group: Weasel already has a home for each, and a composite
///         would have to re-implement fetch and delta for a set whose members compare in completely
///         different ways (a column/index diff against <c>sys.columns</c>, versus a body comparison
///         against <c>sys.sql_modules</c>). Weasel's own reasoning for triggers being independent
///         objects that merely name a target (weasel#452) applies here unchanged. The accepted cost is
///         that the two can drift apart in the model, which <see cref="DocumentFeatureSchema" /> answers
///         by yielding them together.
///     </para>
///     <para>
///         ⚠️ <b>No primary key, and that is deliberate rather than an omission.</b> A token row is
///         (doc_id, tenant_id, member, term, pos) and nothing about it is unique — the same term can
///         appear at several positions, and <c>pos</c> is a recorded ordinal rather than a key. Adding
///         one now would also be a migration against every table the raw-DDL renderer already created,
///         which is the opposite of what moving an object into the model should cost: the shape here
///         matches what that renderer emitted exactly, so the delta against an existing database is
///         <c>None</c>.
///     </para>
/// </remarks>
internal class FullTextTokenTable: Table
{
    public FullTextTokenTable(DocumentMapping mapping)
        : base(new SqlServerObjectName(mapping.DatabaseSchemaName, FullTextIndex.TableNameFor(mapping)))
    {
        // doc_id carries the INNER type of a strongly-typed id, because it has to match the document
        // table's own id column exactly -- the same rule #296/#302 settled for DocumentTable, and the
        // same varchar landmine if it is got wrong.
        AddColumn("doc_id", DocumentTable.SqlTypeForIdentity(mapping.InnerIdType)).NotNull();

        // Always present, even on a single-tenant table whose DOCUMENT table has no tenant_id (#234):
        // the token rows store the '*DEFAULT*' literal there so one search shape serves both tenancy
        // styles. See FullTextIndex.TenantColumn.
        AddColumn(JasperFx.StorageConstants.TenantIdColumn, "varchar(250)").NotNull();
        AddColumn("member", "varchar(200)").NotNull();
        AddColumn("term", "varchar(255)").NotNull();
        AddColumn("pos", "int").NotNull();

        // (term, doc_id) leading, covering everything a search projects: the hot path is "find the
        // documents carrying this term", and the INCLUDE list keeps it from touching the base table.
        Indexes.Add(new IndexDefinition(IndexNameFor(mapping))
        {
            Columns = ["term", "doc_id"],
            IncludedColumns = ["pos", "member", JasperFx.StorageConstants.TenantIdColumn]
        });
    }

    internal static string IndexNameFor(DocumentMapping mapping)
        => "ix_" + FullTextIndex.TableNameFor(mapping) + "_term";
}
