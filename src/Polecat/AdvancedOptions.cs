namespace Polecat;

/// <summary>
///     Store settings most applications never touch. Reached through <see cref="StoreOptions.Advanced" />.
/// </summary>
public class AdvancedOptions
{
    /// <summary>
    ///     #935: store the key and metadata columns as <c>nvarchar</c> rather than <c>varchar</c>, so a
    ///     tenant id, string document id, string stream key, string tag value or natural key can hold any
    ///     character. Off by default, and nothing changes for a store that leaves it off.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The columns are <c>tenant_id</c> everywhere it appears, a string document id, a string stream
    ///         id (<c>pc_streams.id</c>, <c>pc_events.stream_id</c>, <c>pc_natural_key_*.stream_key</c>), a
    ///         string tag value, <c>pc_dcb_tag_version.tag_value</c>, and the metadata columns
    ///         <c>correlation_id</c>, <c>causation_id</c>, <c>user_name</c>, <c>last_modified_by</c>,
    ///         <c>dotnet_type</c> and <c>doc_type</c>. Each keeps its width: <c>varchar(250)</c> becomes
    ///         <c>nvarchar(250)</c>.
    ///     </para>
    ///     <para>
    ///         <b>The 450 character limit.</b> SQL Server allows 900 bytes in a clustered key, and an
    ///         <c>nvarchar</c> character is two of them. Some keys are declared wider than that under this
    ///         setting, a conjoined document table with a string id for one, and SQL Server allows it with
    ///         warning 1945. A row is refused only when its own key passes 900 bytes, so a key's string
    ///         columns together have to stay under 450 characters. Polecat logs the keys that can pass the
    ///         limit once when the store starts, and a write that does pass it fails with a
    ///         <see cref="Exceptions.KeyTooLongException" /> naming the tenant and id.
    ///     </para>
    ///     <para>
    ///         Turning this on for an existing store is an ordinary schema migration: each key column is
    ///         altered from <c>varchar</c> to <c>nvarchar</c>, with its primary key, indexes and foreign keys
    ///         dropped and put back around it. Every row of each table is rewritten, so on a large store
    ///         export the migration with <c>db-patch</c> and run it in a maintenance window rather than at
    ///         startup. Turning it off again is refused once the columns are <c>nvarchar</c>, because
    ///         narrowing them would turn every character the code page doesn't have into <c>?</c>.
    ///     </para>
    /// </remarks>
    public bool UseUnicodeKeys { get; set; }
}
