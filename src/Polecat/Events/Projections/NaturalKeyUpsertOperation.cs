using System.Data.Common;
using JasperFx.Events;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Storage operation that upserts a natural key → stream id mapping
///     using SQL Server MERGE statement. For conjoined tenancy, includes
///     tenant_id in the match condition and insert columns.
/// </summary>
/// <remarks>
///     <para>
///         #549 / jasperfx#764: a key already mapped to a <em>different live</em> stream is REFUSED,
///         not repointed. The <c>WHEN MATCHED</c> arm used to overwrite the stream column
///         unconditionally, which left the original stream in place but unreachable by the identifier
///         it was created with, and reported nothing. Of the two failure modes the silent one is
///         worse, so the matched arm is now conditional and a matched-but-untouched row raises
///         <see cref="DuplicateNaturalKeyException" />.
///     </para>
///     <para>
///         Two neighbouring behaviours are deliberately NOT refusals and both still work. Re-asserting
///         the SAME mapping is idempotent — every key-carrying event on a stream rewrites its own row,
///         so the guard has to let a stream match itself. And a row whose stream was ARCHIVED is
///         claimable: the ruling protects a key mapped to a <em>live</em> stream, and an archived
///         stream has already dropped out of the lookup, so <c>is_archived = 1</c> is an alternative
///         way for the matched arm to fire. Renaming a key on its own stream is handled a step
///         earlier by <see cref="NaturalKeyRetireOperation" />, which is what legitimately frees an
///         identifier for a later stream.
///     </para>
/// </remarks>
internal class NaturalKeyUpsertOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _naturalKeyValue;
    private readonly object _streamId;
    private readonly bool _isGuidStream;
    private readonly bool _isConjoined;
    private readonly string? _tenantId;
    private readonly Type _aggregateType;

    public NaturalKeyUpsertOperation(string tableName, object naturalKeyValue, object streamId, bool isGuidStream,
        Type aggregateType, bool isConjoined = false, string? tenantId = null)
    {
        _tableName = tableName;
        _naturalKeyValue = naturalKeyValue;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
        _aggregateType = aggregateType;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        // The matched arm fires only for this stream's own row, or for a row whose stream is already
        // archived. Anything else is a second claimant on a live key and is left untouched, which is
        // what PostprocessAsync detects. OUTPUT is how it detects it: MERGE emits one row per action
        // it actually performs, so an untouched match emits nothing at all.
        var matchedArm =
            $"WHEN MATCHED AND (target.{streamColumn} = source.{streamColumn} OR target.is_archived = 1) "
            + $"THEN UPDATE SET {streamColumn} = source.{streamColumn}, is_archived = 0";

        // deleted.* is the pre-update image, so this hands the throw site the stream the key was
        // already mapped to on the update path. It is NULL on the insert path, where there was no
        // prior mapping -- and unreachable on the refusal path, which is precisely why the refusal
        // has to probe for the existing id separately below.
        var output = $"OUTPUT $action AS merge_action, deleted.{streamColumn} AS previous_stream";

        // HOLDLOCK on both branches: two sessions first-writing the same natural key would
        // otherwise both probe, both miss, and both insert against the unique key. See #500.
        // The same lock is what makes the #549 refusal sound -- the matched row cannot be changed
        // by another session between the MERGE and the probe that reads its stream back.
        if (_isConjoined)
        {
            builder.Append($"""
                MERGE {_tableName} WITH (UPDLOCK, HOLDLOCK) AS target
                USING (VALUES (
                """);
            builder.AppendParameter(_naturalKeyValue);
            builder.Append(", ");
            builder.AppendParameter(_tenantId!, System.Data.SqlDbType.VarChar);
            builder.Append(", ");
            builder.AppendParameter(_streamId, _streamId is string ? System.Data.SqlDbType.VarChar : null);
            builder.Append($"""
                , 0)) AS source (natural_key_value, tenant_id, {streamColumn}, is_archived)
                ON target.natural_key_value = source.natural_key_value AND target.tenant_id = source.tenant_id
                {matchedArm}
                WHEN NOT MATCHED THEN INSERT (natural_key_value, tenant_id, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.tenant_id, source.{streamColumn}, source.is_archived)
                {output};
                """);
        }
        else
        {
            builder.Append($"""
                MERGE {_tableName} WITH (UPDLOCK, HOLDLOCK) AS target
                USING (VALUES (
                """);
            builder.AppendParameter(_naturalKeyValue);
            builder.Append(", ");
            builder.AppendParameter(_streamId, _streamId is string ? System.Data.SqlDbType.VarChar : null);
            builder.Append($"""
                , 0)) AS source (natural_key_value, {streamColumn}, is_archived)
                ON target.natural_key_value = source.natural_key_value
                {matchedArm}
                WHEN NOT MATCHED THEN INSERT (natural_key_value, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.{streamColumn}, source.is_archived)
                {output};
                """);
        }
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        // One OUTPUT row means the MERGE inserted or updated -- the ordinary case. No row means the
        // key matched a live row belonging to a DIFFERENT stream and the conditional matched arm
        // declined to touch it, which is the #549 refusal.
        if (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return;
        }

        // ExistingStreamId stays null: the refusal path produces no OUTPUT row, so the id the key was
        // already mapped to is not in hand here, and re-reading it would need a second round trip
        // inside a failing unit of work. The shared exception makes both id members nullable for
        // exactly this "inferred the conflict from a write that affected no rows" case.
        exceptions.Add(new DuplicateNaturalKeyException(_aggregateType, _naturalKeyValue, null, _streamId));
    }
}
