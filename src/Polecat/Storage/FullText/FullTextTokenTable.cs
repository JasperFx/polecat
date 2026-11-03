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
///         which is the opposite of what moving an object into the model should cost.
///     </para>
///     <para>
///         ⚠️ <b><c>term</c> is <c>nvarchar</c> (#877), and that is the one change to the shape the raw-DDL
///         renderer emitted.</b> As <c>varchar</c> it held what the database's code page could, so under
///         the default collation <c>東京</c> was stored as <c>??</c> and nothing outside Western European
///         text could be found. And a search scanned, because the term binds as <c>nvarchar</c> and that
///         puts <c>CONVERT_IMPLICIT</c> on the column: a Table Scan at 3,582 reads over half a million
///         tokens against an Index Seek at 5 once the two agree. Binding <c>varchar</c> instead also
///         seeks, and turns the first defect from a miss into a false match, because <c>大阪</c> then binds
///         as <c>??</c> as well. It costs 13% more space over short terms, and 25% over the 8 million
///         tokens measured below.
///     </para>
///     <para>
///         ⚠️ <b>Widening an existing table is a size-of-data operation, so nothing does it at first
///         use.</b> 8 million tokens took 72 seconds to alter and 36 to re-index, which is what 8,000 long
///         documents amount to. Inside the first request that would run into the 30 second command
///         timeout, roll back, and do it again on the next one, leaving the type unusable. The first-use
///         path therefore creates this table and never updates it: an existing table keeps working as it
///         did, and <c>ApplyAllConfiguredChangesToDatabaseAsync()</c> or the script from <c>db-patch</c>
///         is the step that widens it, followed by <c>RebuildFullTextIndexAsync</c> for what the old
///         column already lost.
///     </para>
///     <para>
///         ⚠️ <b>The <c>doc_id</c> index is the same kind of change, so it takes the same road (#916).</b>
///         The trigger finds a document's old tokens by <c>doc_id</c>, which led no index, so every write
///         to the type scanned the table: 3,819 logical reads for a document of 25 tokens among half a
///         million, growing with the table, against 253 that do not. An existing table keeps scanning
///         until the schema is applied.
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
        AddColumn("term", $"nvarchar({FullTextIndex.MaxTermLength})").NotNull();
        AddColumn("pos", "int").NotNull();

        // term leading, covering everything a search projects: the hot path is "find the documents
        // carrying this term", and the INCLUDE list keeps it from touching the base table. member is in
        // the key because every search filters on it.
        //
        // ⚠️ That key is also what gets an existing table widened. ALTER COLUMN is refused while an index
        // depends on the column (Msg 5074), and Weasel only takes down an index it is replacing: one that
        // matches the model stays put unless the column is a computed one (weasel#638). An index whose
        // definition differs is dropped before the columns change and built after them, so the change of
        // key and the change of type have to arrive together.
        Indexes.Add(new IndexDefinition(IndexNameFor(mapping))
        {
            Columns = ["term", "member", "doc_id"],
            IncludedColumns = ["pos", JasperFx.StorageConstants.TenantIdColumn]
        });

        // What the trigger deletes by. The tenant is in the key because an id is only unique per tenant
        // under conjoined tenancy, and the trigger's join names it. See FullTextIndex.DeletedTenantPredicate.
        Indexes.Add(new IndexDefinition(DocumentIndexNameFor(mapping))
        {
            Columns = ["doc_id", JasperFx.StorageConstants.TenantIdColumn]
        });
    }

    internal static string IndexNameFor(DocumentMapping mapping)
        => "ix_" + FullTextIndex.TableNameFor(mapping) + "_term";

    internal static string DocumentIndexNameFor(DocumentMapping mapping)
        => "ix_" + FullTextIndex.TableNameFor(mapping) + "_doc";
}
