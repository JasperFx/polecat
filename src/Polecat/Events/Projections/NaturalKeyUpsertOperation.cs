using System.Data.Common;
using Polecat.Internal;
using Weasel.Core;
using Weasel.SqlServer;

namespace Polecat.Events.Projections;

/// <summary>
///     Storage operation that upserts a natural key → stream id mapping
///     using SQL Server MERGE statement. For conjoined tenancy, includes
///     tenant_id in the match condition and insert columns.
/// </summary>
internal class NaturalKeyUpsertOperation : Polecat.Internal.IStorageOperation
{
    private readonly string _tableName;
    private readonly object _naturalKeyValue;
    private readonly object _streamId;
    private readonly bool _isGuidStream;
    private readonly bool _isConjoined;
    private readonly string? _tenantId;

    public NaturalKeyUpsertOperation(string tableName, object naturalKeyValue, object streamId, bool isGuidStream,
        bool isConjoined = false, string? tenantId = null)
    {
        _tableName = tableName;
        _naturalKeyValue = naturalKeyValue;
        _streamId = streamId;
        _isGuidStream = isGuidStream;
        _isConjoined = isConjoined;
        _tenantId = tenantId;
    }

    public Type DocumentType => typeof(object);
    public OperationRole Role() => OperationRole.Upsert;

    public void ConfigureCommand(ICommandBuilder builder)
    {
        var streamColumn = _isGuidStream ? "stream_id" : "stream_key";

        if (_isConjoined)
        {
            builder.Append($"""
                MERGE {_tableName} AS target
                USING (VALUES (
                """);
            builder.AppendParameter(_naturalKeyValue);
            builder.Append(", ");
            builder.AppendParameter(_tenantId!);
            builder.Append(", ");
            builder.AppendParameter(_streamId);
            builder.Append($"""
                , 0)) AS source (natural_key_value, tenant_id, {streamColumn}, is_archived)
                ON target.natural_key_value = source.natural_key_value AND target.tenant_id = source.tenant_id
                WHEN MATCHED THEN UPDATE SET {streamColumn} = source.{streamColumn}, is_archived = 0
                WHEN NOT MATCHED THEN INSERT (natural_key_value, tenant_id, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.tenant_id, source.{streamColumn}, source.is_archived);
                """);
        }
        else
        {
            builder.Append($"""
                MERGE {_tableName} AS target
                USING (VALUES (
                """);
            builder.AppendParameter(_naturalKeyValue);
            builder.Append(", ");
            builder.AppendParameter(_streamId);
            builder.Append($"""
                , 0)) AS source (natural_key_value, {streamColumn}, is_archived)
                ON target.natural_key_value = source.natural_key_value
                WHEN MATCHED THEN UPDATE SET {streamColumn} = source.{streamColumn}, is_archived = 0
                WHEN NOT MATCHED THEN INSERT (natural_key_value, {streamColumn}, is_archived) VALUES (source.natural_key_value, source.{streamColumn}, source.is_archived);
                """);
        }
    }

    public Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
        => Task.CompletedTask;
}
