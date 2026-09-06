using System.Data.Common;
using JasperFx.Core.Exceptions;
using JasperFx.Events;
using Microsoft.Data.SqlClient;
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
internal class NaturalKeyUpsertOperation : Polecat.Internal.IStorageOperation, IExceptionTransform
{
    /// <summary>
    ///     User-defined SQL error number the guard raises. Above the 50000 floor <c>THROW</c>
    ///     requires, and carries the issue number so a stray occurrence in a log is traceable.
    /// </summary>
    internal const int DuplicateNaturalKeyErrorNumber = 51549;

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
        // archived. Anything else is a second claimant on a live key, and the MERGE leaves it
        // untouched -- so @@ROWCOUNT is zero and the guard below turns that into a refusal.
        var matchedArm =
            $"WHEN MATCHED AND (target.{streamColumn} = source.{streamColumn} OR target.is_archived = 1) "
            + $"THEN UPDATE SET {streamColumn} = source.{streamColumn}, is_archived = 0";

        // Detected in SQL rather than by reading a MERGE OUTPUT row back in PostprocessAsync,
        // deliberately. The flush loop advances the reader one result set per queued operation, and
        // a MERGE that emits OUTPUT rows does not line up with that walk -- an operation that reads
        // rows here lands on a neighbour's result set and reports a conflict for a key that never
        // had one. @@ROWCOUNT needs no result set at all and is read in the same batch that set it.
        var guard =
            $"IF @@ROWCOUNT = 0 THROW {DuplicateNaturalKeyErrorNumber}, 'Natural key already mapped to a different live stream', 1;";

        // HOLDLOCK on both branches: two sessions first-writing the same natural key would
        // otherwise both probe, both miss, and both insert against the unique key. See #500.
        // The same lock is what makes the #549 refusal sound -- the matched row cannot change
        // between the MERGE testing it and the guard reading @@ROWCOUNT.
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
                WHEN NOT MATCHED THEN INSERT (natural_key_value, tenant_id, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.tenant_id, source.{streamColumn}, source.is_archived);
                {guard}
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
                WHEN NOT MATCHED THEN INSERT (natural_key_value, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.{streamColumn}, source.is_archived);
                {guard}
                """);
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;

    /// <summary>
    ///     Turn the guard's SQL error into the shared
    ///     <see cref="DuplicateNaturalKeyException" />, which is what a store-agnostic caller — and
    ///     <c>NaturalKeyCompliance.a_second_stream_cannot_claim_a_live_natural_key</c> — catches.
    /// </summary>
    /// <remarks>
    ///     <see cref="DuplicateNaturalKeyException.ExistingStreamId" /> stays null. The conflict is
    ///     inferred from a write that affected no rows rather than from a probe that read the
    ///     existing row first, so the id the key was already mapped to is not in hand here; the
    ///     shared exception makes both id members nullable for exactly this case. The key is the
    ///     UNWRAPPED value, because NaturalKeyDefinition.Unwrap runs before the operation is queued.
    /// </remarks>
    public bool TryTransform(Exception original, out Exception? transformed)
    {
        if (original is SqlException { Number: DuplicateNaturalKeyErrorNumber })
        {
            transformed = new DuplicateNaturalKeyException(_aggregateType, _naturalKeyValue, null, _streamId);
            return true;
        }

        transformed = null;
        return false;
    }
}
