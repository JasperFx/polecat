using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Polecat.Internal.Batching;
using Polecat.Serialization;
using Polecat.Storage.ClosedShape;
using Weasel.SqlServer;
using Weasel.Storage;

namespace Polecat.Events.Fetching;

/// <summary>
///     What one fetch by natural key reads, after the key itself.
/// </summary>
/// <param name="ForUpdate">
///     Lock the stream row until the session's transaction ends, as <c>FetchForExclusiveWriting</c> does.
/// </param>
/// <param name="ReadEvents">Read the stream's events, from version 1.</param>
/// <param name="Document">
///     Read the stream's Inline projected document through this storage, rather than its events.
/// </param>
internal readonly record struct NaturalKeyFetchShape(bool ForUpdate, bool ReadEvents, object? Document);

/// <summary>
///     What one fetch by natural key found.
/// </summary>
internal sealed class NaturalKeyFetchResult<T> where T : class
{
    /// <summary>A live stream has the key.</summary>
    public bool Found { get; init; }

    /// <summary>The stream that has the key. Only meaningful when <see cref="Found" />.</summary>
    public object StreamId { get; init; } = null!;

    /// <summary>The stream's version, as the first statement read it.</summary>
    public long Version { get; init; }

    /// <summary>
    ///     For an exclusive fetch, whether the key still named the stream once its row was locked. A key can only
    ///     change hands by an append to the stream that has it, which the lock then holds off, so a key that still
    ///     names the stream after the lock goes on naming it until the transaction ends.
    /// </summary>
    public bool StillNamed { get; init; } = true;

    public IReadOnlyList<IEvent> Events { get; init; } = [];

    public T? Document { get; init; }
}

/// <summary>
///     #1131 / #1050 (marten#6150): fetches an aggregate by one of its natural keys, owned or mapped, in ONE
///     command. The first statement resolves the key through the lookup table of the aggregate that owns it into
///     a T-SQL variable, together with the stream's version, and the statements after it read the stream's
///     events, or its Inline document, by that variable. Nothing depends on a result the client has to read
///     first, so the same SQL runs standalone and as an item of a batched query.
/// </summary>
/// <remarks>
///     <para>
///         Marten resolves the key again in a subselect in every statement, and then once more at the end to see
///         whether it moved mid-fetch. SQL Server can hold the resolved stream id in a variable instead, so every
///         read after the first is of the very stream the first one found: there is nothing to move.
///     </para>
///     <para>
///         An exclusive fetch is the exception. Its lock is taken by the same locked read
///         <c>FetchForExclusiveWriting</c> by stream id uses (<see cref="EventGraph.LockedStreamReadSqlWithOrdinal" />),
///         which runs after the key is resolved, so the key is resolved once more under the lock, and
///         <see cref="NaturalKeyFetchResult{T}.StillNamed" /> says whether it still names that stream.
///     </para>
///     <para>
///         A mapped key (<c>[MappedNaturalKey&lt;TSource&gt;]</c>) reads its source projection's lookup, which the
///         source keeps current inline. The aggregate being fetched projects the same stream, so an Async read
///         model is found by key as soon as its events commit.
///     </para>
/// </remarks>
[UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "Class-level: hydrates IEvent batches via PcEventsRowReader, as FetchStreamBatchItem does. Event types are preserved by EventGraph registration on the caller side per the AOT publishing guide.")]
[UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode",
    Justification = "Class-level: as FetchStreamBatchItem. AOT consumers register concrete event types ahead of time.")]
internal sealed class NaturalKeyFetchPlan<T> where T : class
{
    private const string StreamVariable = "@pc_nk_stream";
    private const string TenantVariable = "@pc_nk_tenant";
    private const string KeyVariable = "@pc_nk_key";

    private readonly EventGraph _events;
    private readonly string _lookupTable;
    private readonly string _streamColumn;
    private readonly string _streamType;
    private readonly string _keyType;
    private readonly bool _conjoined;
    private readonly bool _isGuidStream;
    private readonly bool _byStreamId;

    /// <summary>
    ///     The same reads for a stream named by its id, for the batched fetches by stream id, which have no
    ///     lookup to go through.
    /// </summary>
    public NaturalKeyFetchPlan(EventGraph events)
    {
        _events = events;
        _byStreamId = true;
        Key = null!;
        Lookup = null!;
        _lookupTable = string.Empty;
        _keyType = string.Empty;
        _isGuidStream = events.StreamIdentity == StreamIdentity.AsGuid;
        _streamColumn = _isGuidStream ? "stream_id" : "stream_key";
        _streamType = _isGuidStream ? "uniqueidentifier" : events.KeyText();
        _conjoined = events.TenancyStyle == TenancyStyle.Conjoined;
    }

    public NaturalKeyFetchPlan(EventGraph events, NaturalKeyDefinition key)
    {
        _events = events;
        Key = key;

        Lookup = key.IsMapped
            ? key.SourceProjectionDefinition ?? throw new InvalidOperationException(
                $"The mapped natural key {typeof(T).FullNameInCode()}.{key.Member.Name} was never resolved to its source projection {key.SourceProjectionType!.FullNameInCode()}. Mapped keys are resolved when the DocumentStore is built.")
            : key;

        _lookupTable = events.NaturalKeyTableName(Lookup.AggregateType);
        _isGuidStream = events.StreamIdentity == StreamIdentity.AsGuid;
        _streamColumn = _isGuidStream ? "stream_id" : "stream_key";
        _streamType = _isGuidStream ? "uniqueidentifier" : events.KeyText();
        _conjoined = events.TenancyStyle == TenancyStyle.Conjoined;

        // The lookup column's own type, see NaturalKeyTable
        _keyType = Lookup.InnerType == typeof(int) ? "int"
            : Lookup.InnerType == typeof(long) ? "bigint"
            : "nvarchar(200)";
    }

    /// <summary>A plan for a stream named by its id, rather than by a natural key.</summary>
    public bool IsByStreamId => _byStreamId;

    /// <summary>The aggregate's key that is being fetched by: its owned key, or one of its mapped keys.</summary>
    public NaturalKeyDefinition Key { get; }

    /// <summary>The owned key whose lookup table is read: <see cref="Key" />, or the source's key for a mapped one.</summary>
    public NaturalKeyDefinition Lookup { get; }

    /// <summary>
    ///     The key's stored value: unwrapped from a strong-typed identifier, or null for a null key or a wrapper
    ///     whose value is null. No stream can have a null key.
    /// </summary>
    public object? KeyValueFor(object? id) => id is null ? null : _byStreamId ? id : Key.Unwrap(id);

    /// <summary>
    ///     An <c>int</c> or <c>long</c> key that was never set reads as 0 on an aggregate, so in memory an aggregate
    ///     without a key can't be told from one keyed 0. A fetch by 0 always goes to the database, which only has a
    ///     lookup row for a stream that was actually given the key.
    /// </summary>
    public static bool CouldBeUnset(object keyValue) => keyValue is 0 or 0L;

    /// <summary>
    ///     Does <paramref name="candidate" />, an aggregate a session already holds, have this key value now?
    /// </summary>
    public bool HasKey(object candidate, object keyValue)
        => !CouldBeUnset(keyValue) && Equals(Key.ReadValue(candidate), keyValue);

    /// <param name="keyValue">The key's stored value, or the stream id for a plan by stream id.</param>
    public void WriteFetch(ICommandBuilder builder, object keyValue, string tenantId, NaturalKeyFetchShape shape)
    {
        if (_byStreamId)
        {
            writeStreamResolution(builder, keyValue, tenantId);
        }
        else
        {
            writeKeyResolution(builder, keyValue, tenantId);
        }

        if (shape.ForUpdate)
        {
            writeLockedRead(builder, tenantId);
        }
        else
        {
            builder.Append($"SELECT {StreamVariable}, @pc_nk_version WHERE @pc_nk_version IS NOT NULL;\n");
        }

        if (shape.ReadEvents)
        {
            FetchStreamBatchItem.WriteStreamEventsSql(builder, _events, StreamVariable, TenantVariable);
        }

        if (shape.Document is IPolecatBatchLoadStorage<T> document)
        {
            document.WriteLoadByIdExpressionSql(builder, StreamVariable, tenantId);
            builder.Append(";\n");
        }
    }

    private void writeStreamResolution(ICommandBuilder builder, object streamId, string tenantId)
    {
        builder.Append($"DECLARE {StreamVariable} {_streamType} = ");
        builder.AppendParameter(streamId, Polecat.Internal.SqlParameterCollectionExtensions.BindingForKey(streamId, _events.UnicodeKeys));
        builder.Append($", @pc_nk_version bigint, {TenantVariable} {_events.KeyText()} = ");
        builder.AppendParameter(tenantId, _events.KeyDbType);
        builder.Append(";\n");

        // As FetchForWriting by stream id reads it, archived or not
        builder.Append($"SELECT @pc_nk_version = s.version FROM {_events.StreamsTableName} s WHERE s.id = {StreamVariable} AND s.tenant_id = {TenantVariable};\n");
    }

    private void writeKeyResolution(ICommandBuilder builder, object keyValue, string tenantId)
    {
        builder.Append($"DECLARE {StreamVariable} {_streamType}, @pc_nk_version bigint, {KeyVariable} {_keyType} = ");
        builder.AppendParameter(keyValue, keyDbType(keyValue));
        builder.Append($", {TenantVariable} {_events.KeyText()} = ");
        builder.AppendParameter(tenantId, _events.KeyDbType);
        builder.Append(";\n");

        // The key's live stream. tenant_id is part of the primary key of both tables, so under conjoined tenancy
        // the join matches it too, or the same key in two tenants would join across them. An archived stream is
        // filtered on both rows: ArchiveStream marks the lookup row, but the Archived event alone marks only the
        // stream until NaturalKeyProjection catches up with it.
        builder.Append($"SELECT {StreamVariable} = nk.{_streamColumn}, @pc_nk_version = s.version FROM {_lookupTable} nk ");
        builder.Append($"INNER JOIN {_events.StreamsTableName} s ON s.id = nk.{_streamColumn}");
        if (_conjoined) builder.Append(" AND s.tenant_id = nk.tenant_id");
        builder.Append($" WHERE nk.natural_key_value = {KeyVariable} AND nk.is_archived = 0 AND s.is_archived = 0");
        builder.Append($" AND s.tenant_id = {TenantVariable}");
        if (_conjoined) builder.Append($" AND nk.tenant_id = {TenantVariable}");
        builder.Append(";\n");
    }

    /// <summary>
    ///     The locked read <c>FetchForExclusiveWriting</c> by stream id makes, for the stream the key resolved to,
    ///     then the key resolved again under that lock.
    /// </summary>
    private void writeLockedRead(ICommandBuilder builder, string tenantId)
    {
        // The locked read names its stream @id and its tenant @tenant_id, as the parameters of the by-id fetch.
        // Here they are variables holding what the first statement resolved.
        var locked = _events.LockedStreamReadSqlWithOrdinal("version", "id = @id AND tenant_id = @tenant_id",
            tenantId, out var ordinal);

        builder.Append($"DECLARE @id {_streamType} = {StreamVariable}, @tenant_id {_events.KeyText()} = {TenantVariable}");
        if (ordinal.HasValue)
        {
            builder.Append(", @tenant_ordinal int = ");
            builder.AppendParameter(ordinal.Value, SqlDbType.Int);
        }

        builder.Append(";\n");

        builder.Append("IF @id IS NOT NULL\nBEGIN\n");
        builder.Append(locked);
        builder.Append("\nEND\nELSE\n    SELECT CAST(NULL AS bigint) AS version WHERE 1 = 0;\n");

        if (_byStreamId)
        {
            // The id is what was asked for, so there is no key to have moved
            builder.Append("SELECT 1, @id;\n");
            return;
        }

        builder.Append($"SELECT CASE WHEN EXISTS (SELECT 1 FROM {_lookupTable} nk WHERE nk.natural_key_value = {KeyVariable}");
        builder.Append($" AND nk.{_streamColumn} = @id AND nk.is_archived = 0");
        if (_conjoined) builder.Append(" AND nk.tenant_id = @tenant_id");
        builder.Append(") THEN 1 ELSE 0 END, @id;\n");
    }

    private static SqlDbType keyDbType(object keyValue) => keyValue switch
    {
        int => SqlDbType.Int,
        long => SqlDbType.BigInt,
        _ => SqlDbType.NVarChar
    };

    /// <summary>
    ///     Reads every result set <see cref="WriteFetch" /> produced, whether or not the key was found, so that a
    ///     batch can move on to its next item. Leaves the reader on the last of them.
    /// </summary>
    public async Task<NaturalKeyFetchResult<T>> ReadFetchAsync(DbDataReader reader, ISerializer serializer,
        IStorageSession session, string tenantId, NaturalKeyFetchShape shape, CancellationToken token)
    {
        var found = false;
        object streamId = null!;
        long version = 0;
        var stillNamed = true;

        if (shape.ForUpdate)
        {
            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                found = true;
                version = await reader.GetFieldValueAsync<long>(0, token).ConfigureAwait(false);
            }

            await reader.NextResultAsync(token).ConfigureAwait(false);
            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                stillNamed = await reader.GetFieldValueAsync<int>(0, token).ConfigureAwait(false) == 1;
                if (found && !await reader.IsDBNullAsync(1, token).ConfigureAwait(false))
                {
                    streamId = await readStreamIdAsync(reader, 1, token).ConfigureAwait(false);
                }
                else
                {
                    found = false;
                }
            }
            else
            {
                found = false;
            }
        }
        else if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            found = true;
            streamId = await readStreamIdAsync(reader, 0, token).ConfigureAwait(false);
            version = await reader.GetFieldValueAsync<long>(1, token).ConfigureAwait(false);
        }

        IReadOnlyList<IEvent> events = [];
        if (shape.ReadEvents)
        {
            await reader.NextResultAsync(token).ConfigureAwait(false);

            // A key that named no stream reads no events, but its result set is there all the same
            events = await FetchStreamBatchItem.ReadEventsAsync(reader, _events, serializer,
                found ? streamId : _isGuidStream ? Guid.Empty : string.Empty, tenantId, token).ConfigureAwait(false);
        }

        T? document = null;
        if (shape.Document is IPolecatBatchLoadStorage<T> storage)
        {
            await reader.NextResultAsync(token).ConfigureAwait(false);
            if (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                document = await storage.BuildLoadSelector(session).ResolveAsync(reader, token).ConfigureAwait(false);
            }
        }

        return new NaturalKeyFetchResult<T>
        {
            Found = found,
            StreamId = streamId,
            Version = version,
            StillNamed = stillNamed,
            Events = events,
            Document = found ? document : null
        };
    }

    private async Task<object> readStreamIdAsync(DbDataReader reader, int ordinal, CancellationToken token)
        => _isGuidStream
            ? await reader.GetFieldValueAsync<Guid>(ordinal, token).ConfigureAwait(false)
            : await reader.GetFieldValueAsync<string>(ordinal, token).ConfigureAwait(false);
}
