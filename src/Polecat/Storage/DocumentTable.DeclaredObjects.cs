using JasperFx.Core;
using Weasel.SqlServer;
using Weasel.SqlServer.Tables;

namespace Polecat.Storage;

/// <summary>
///     #684 — the persisted computed columns, secondary indexes and foreign keys a document type
///     declares, modeled as Weasel schema objects on the table rather than emitted as raw DDL.
/// </summary>
/// <remarks>
///     <para>
///         These used to be "raw-DDL managed": <c>DocumentIndex.ToDdlStatements</c> and friends
///         rendered strings, <see cref="Internal.DocumentTableEnsurer" /> executed them at first use,
///         and <see cref="DocumentTable.CreateDeltaAsync" /> deliberately <em>stripped</em> them out of
///         the fetched table so a migration would never drop them. Each step was locally reasonable and
///         the result was that everything reading the schema-object model disagreed with the database:
///         the generated creation script omitted every one of them, and
///         <c>AssertDatabaseMatchesConfigurationAsync()</c> reported a match over a database that had
///         none of them — which is what kept it invisible.
///     </para>
///     <para>
///         Modeling them fixes all of those at once, because every consumer reads the same model:
///         <c>db-dump</c> and <c>Advanced.ToDatabaseScript()</c> render them, the delta creates what is
///         missing and re-creates what changed, and <c>AutoCreate.None</c> can finally refuse what it
///         could see all along.
///     </para>
///     <para>
///         ⚠️ <b><see cref="DocumentTable.CreateDeltaAsync" /> needs no change, and that is the point.</b>
///         It strips indexes, foreign keys and columns that are <em>not in this model</em> — so the
///         moment Polecat's own are declared here, they reconcile normally while a user's hand-added
///         index or an EF-migration column is still left alone. The #267 behaviour it exists for is
///         unchanged; it was only ever too broad because this model was too narrow.
///     </para>
/// </remarks>
internal partial class DocumentTable
{
    /// <summary>
    ///     Declare everything the mapping asks for, in the order Weasel's delta applies it.
    /// </summary>
    /// <remarks>
    ///     Columns before indexes and foreign keys because they are what those sit on, and
    ///     <c>TableDelta.WriteUpdate</c> emits missing columns ahead of missing indexes for the same
    ///     reason. Worth recording that they land in <b>one batch</b> and that this is fine: an
    ///     <c>ALTER TABLE … ADD … AS … PERSISTED</c> followed by a <c>CREATE INDEX</c> on that column in
    ///     the same batch succeeds — measured, guarded and unguarded — because index DDL is compiled
    ///     per statement at execution. No <c>GO</c> is needed, which is why none of this depends on
    ///     Weasel's batch splitter.
    /// </remarks>
    private void AddDeclaredSchemaObjects(DocumentMapping mapping, bool includeForeignKeys)
    {
        AddIndexComputedColumns(mapping);
        AddVectorComputedColumns(mapping);
        AddDeclaredForeignKeys(mapping, includeForeignKeys);
        AddDeclaredIndexes(mapping);
        AddDeclaredJsonIndexes(mapping);
    }

    /// <summary>
    ///     One persisted computed column per indexed JSON path, plus one per <c>INCLUDE</c> path.
    /// </summary>
    /// <remarks>
    ///     The expression comes from <see cref="DocumentIndex.ComputedColumnExpression" />, which is the
    ///     same method the LINQ translator uses (#223) — that textual identity is what lets SQL Server
    ///     match a predicate to the column and seek the index, so it must not be re-derived here.
    /// </remarks>
    private void AddIndexComputedColumns(DocumentMapping mapping)
    {
        foreach (var index in mapping.Indexes)
        {
            foreach (var path in index.JsonPaths)
            {
                AddComputedColumn(mapping, path, index.Casing, index.ResolveSqlType(path, mapping.ResolveClrMemberType(path)));
            }

            // INCLUDE columns are always Default casing — an UPPER()/LOWER() wrapper exists to make a
            // key column case-insensitively seekable, and a covering column is never seeked.
            foreach (var path in index.IncludeColumns)
            {
                AddComputedColumn(mapping, path, IndexCasing.Default,
                    index.ResolveSqlType(path, mapping.ResolveClrMemberType(path)));
            }
        }
    }

    /// <summary>
    ///     The persisted computed <c>VECTOR(n)</c> column behind a vector search. No index follows one —
    ///     Polecat's vector search is an exact scan over the column.
    /// </summary>
    /// <remarks>
    ///     ⚠️ A server without the <c>VECTOR</c> type (anything before SQL Server 2025, and Azure SQL
    ///     Edge) fails this column with "Type VECTOR is not a defined system type", which names neither
    ///     Polecat nor the declaration that asked for it. That translation now lives in
    ///     <see cref="Internal.DocumentTableEnsurer" /> around the migration itself rather than around
    ///     this one statement — see <c>TranslateMissingVectorType</c> there.
    /// </remarks>
    private void AddVectorComputedColumns(DocumentMapping mapping)
    {
        foreach (var vectorIndex in mapping.VectorIndexes)
        {
            AddComputedColumnNamed(vectorIndex.ColumnName, $"VECTOR({vectorIndex.Dimensions})",
                vectorIndex.ColumnExpression());
        }
    }

    /// <summary>
    ///     Each declared foreign key: its persisted computed column, then the constraint.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The referenced mapping is built fresh rather than resolved through
    ///         <c>StoreOptions.Providers</c> on purpose. <c>DocumentFeatureSchema.schemaObjects()</c>
    ///         builds these tables while iterating <c>AllProviders</c>, and materializing a provider
    ///         from inside that loop would mutate the collection being enumerated. Everything a foreign
    ///         key needs — schema, table name, inner id type, tenancy style — is derived from the type
    ///         and the options, so a fresh mapping answers identically.
    ///     </para>
    ///     <para>
    ///         ⚠️ Weasel pairs <c>ColumnNames[i]</c> with <c>LinkedNames[i]</c> positionally and
    ///         preserves declaration order on both sides. It did not always: the setters used to sort
    ///         each side independently, which paired the wrong columns on a composite key whose two
    ///         sides do not sort into the same relative order — which is exactly the conjoined
    ///         <c>(tenant_id, col) → (tenant_id, id)</c> shape below. That is fixed in the Weasel this
    ///         pins, and it is the reason a conjoined foreign key can be modeled at all.
    ///     </para>
    /// </remarks>
    private void AddDeclaredForeignKeys(DocumentMapping mapping, bool includeConstraints)
    {
        foreach (var fk in mapping.ForeignKeys)
        {
            var reference = new DocumentMapping(fk.ReferenceDocumentType, mapping.StoreOptions);

            var columnName = DocumentIndex.ColumnNameForPath(fk.JsonPath);
            var sqlType = SqlTypeForIdentity(reference.InnerIdType);

            // The column is declared either way. Only the CONSTRAINT is conditional, so the table's
            // column set is identical across both of DocumentTableEnsurer's passes and the second pass
            // has nothing to add but the constraint itself.
            AddComputedColumn(mapping, fk.JsonPath, IndexCasing.Default, sqlType);

            if (!includeConstraints) continue;

            // Conjoined on BOTH sides, because the reference is (tenant_id, id) only when the
            // referenced table actually has a tenant_id in its key.
            var conjoined = mapping.TenancyStyle == TenancyStyle.Conjoined
                            && reference.TenancyStyle == TenancyStyle.Conjoined;

            var foreignKey = new ForeignKey(fk.ConstraintName ?? $"fk_{mapping.TableName}_{columnName}")
            {
                LinkedTable = new SqlServerObjectName(reference.DatabaseSchemaName, reference.TableName),
                ColumnNames = conjoined
                    ? [JasperFx.StorageConstants.TenantIdColumn, columnName]
                    : [columnName],
                LinkedNames = conjoined
                    ? [JasperFx.StorageConstants.TenantIdColumn, "id"]
                    : ["id"],
                // DocumentForeignKey carries the DIALECT-NEUTRAL Weasel.Core.CascadeAction, which is
                // what the public DSL takes; Weasel's SQL Server ForeignKey.OnDelete is the SQL
                // Server enum. DeleteAction is the base's neutral property, so assigning it skips a
                // hand-written mapping between two enums that already know how to convert.
                DeleteAction = fk.OnDelete
            };

            if (ForeignKeys.All(x => !x.Name.EqualsIgnoreCase(foreignKey.Name)))
            {
                ForeignKeys.Add(foreignKey);
            }
        }
    }

    /// <summary>
    ///     Each declared index, over the computed columns added above.
    /// </summary>
    private void AddDeclaredIndexes(DocumentMapping mapping)
    {
        foreach (var index in mapping.Indexes)
        {
            var keyColumns = new List<string>();

            // A per-tenant index leads with tenant_id so a tenant-scoped query can seek it.
            if (index.TenancyScope == TenancyScope.PerTenant)
            {
                keyColumns.Add(JasperFx.StorageConstants.TenantIdColumn);
            }

            var pathColumns = index.JsonPaths
                .Select(path => DocumentIndex.ColumnNameForPath(path, index.Casing))
                .ToArray();

            keyColumns.AddRange(pathColumns);

            var definition = new IndexDefinition(index.GetIndexName(mapping.TableName))
            {
                IsUnique = index.IsUnique,
                Columns = keyColumns.ToArray(),
                IncludedColumns = index.IncludeColumns
                    .Select(path => DocumentIndex.ColumnNameForPath(path, IndexCasing.Default))
                    .ToArray(),
                Predicate = index.Predicate
            };

            // Polecat's SortOrder is one setting for the whole index and has always meant "every key
            // path descending". Weasel's own SortOrder means something narrower -- a single trailing
            // DESC, i.e. only the last key column -- so the per-column set is the faithful mapping, and
            // CompareColumnDirection is what makes the diff hold it rather than read it and shrug.
            //
            // tenant_id is deliberately NOT included: it is a scoping prefix, not one of the paths the
            // caller asked to order.
            if (index.SortOrder == Polecat.Storage.SortOrder.Descending)
            {
                foreach (var column in pathColumns)
                {
                    definition.DescendingColumns.Add(column);
                }

                definition.CompareColumnDirection = true;
            }

            if (Indexes.All(x => !x.Name.EqualsIgnoreCase(definition.Name)))
            {
                Indexes.Add(definition);
            }
        }
    }

    /// <summary>
    ///     #685 — SQL Server 2025 <c>CREATE JSON INDEX</c> as a modeled schema object rather than raw
    ///     DDL rendered at first use.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The consequences #684 documented applied here unchanged while this was a rendered
    ///         string: absent from <c>Advanced.ToDatabaseScript()</c> and <c>db-dump</c>, invisible to
    ///         <c>AssertDatabaseMatchesConfigurationAsync()</c>, and unenforceable under
    ///         <c>AutoCreate.None</c> — which cannot refuse what it cannot see.
    ///     </para>
    ///     <para>
    ///         Weasel's <c>JsonIndexDefinition</c> (weasel#661) also fixed a bug on the way in: a JSON
    ///         index IS in <c>sys.indexes</c> with <c>type_desc = 'JSON'</c>, but its
    ///         <c>sys.index_columns</c> row carries <c>key_ordinal = 0</c>, so the ordinary read
    ///         returned it with no columns and a migration dropped it as an extra. Polecat was shielded
    ///         only by <c>CreateDeltaAsync</c> stripping unmodeled indexes, which is precisely the
    ///         hand-maintained second description of the schema that never stays in sync.
    ///     </para>
    ///     <para>
    ///         Only one JSON index can exist per <c>json</c> column, so a table has at most one — but
    ///         the mapping is a collection, and a second declaration is a configuration error worth
    ///         naming rather than silently dropping.
    ///     </para>
    /// </remarks>
    private void AddDeclaredJsonIndexes(DocumentMapping mapping)
    {
        if (mapping.JsonIndexes.Count == 0) return;

        if (mapping.JsonColumnType != "json")
        {
            throw new InvalidOperationException(
                $"A JSON index on '{mapping.DocumentType.Name}' requires the native json column type. " +
                "Set UseNativeJsonType = true (SQL Server 2025+), or use a computed-column Index(...) instead.");
        }

        if (mapping.JsonIndexes.Count > 1)
        {
            throw new InvalidOperationException(
                $"'{mapping.DocumentType.Name}' declares {mapping.JsonIndexes.Count} JSON indexes, but SQL Server "
                + "allows only one JSON index per json column. Combine the paths into a single JsonIndex(...).");
        }

        var jsonIndex = mapping.JsonIndexes[0];

        var definition = new JsonIndexDefinition(jsonIndex.GetIndexName(mapping.TableName), "data")
        {
            JsonPaths = jsonIndex.JsonPaths,
            OptimizeForArraySearch = jsonIndex.OptimizeForArraySearch,
            FillFactor = jsonIndex.FillFactor
        };

        if (Indexes.All(x => !x.Name.EqualsIgnoreCase(definition.Name)))
        {
            Indexes.Add(definition);
        }
    }

    /// <summary>
    ///     Add a persisted computed column for a JSON path, unless the table already has one by that
    ///     name.
    /// </summary>
    /// <remarks>
    ///     The guard is not defensive tidiness. Two indexes over the same member — a plain one and a
    ///     covering one, say — resolve to the same column name, and a foreign key's column is commonly
    ///     also indexed. Weasel's <c>AddColumn</c> appends unconditionally, so a duplicate would render
    ///     the column twice in one <c>CREATE TABLE</c> and fail.
    /// </remarks>
    private void AddComputedColumn(DocumentMapping mapping, string jsonPath, IndexCasing casing, string sqlType)
    {
        var name = DocumentIndex.ColumnNameForPath(jsonPath, casing);
        var expression = DocumentIndex.ComputedColumnExpression(
            jsonPath, sqlType, casing, DocumentIndex.UsesNativeJson(mapping));

        AddComputedColumnNamed(name, sqlType, expression);
    }

    private void AddComputedColumnNamed(string name, string sqlType, string expression)
    {
        if (Columns.Any(x => x.Name.EqualsIgnoreCase(name))) return;

        // The declared type is never emitted for a computed column — SQL Server derives it from the
        // expression, and Weasel's delta comparison skips it for the same reason. Passed anyway so the
        // model reads honestly and so a future non-computed use of this helper is not a trap.
        AddColumn(name, sqlType).ComputedAs(expression, persisted: true);
    }

    internal static string SqlTypeForIdentity(Type innerIdType)
        => innerIdType == typeof(Guid) ? "uniqueidentifier"
            : innerIdType == typeof(int) ? "int"
                : innerIdType == typeof(long) ? "bigint"
                    : "varchar(250)";
}
