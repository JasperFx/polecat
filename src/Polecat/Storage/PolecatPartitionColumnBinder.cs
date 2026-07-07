using System.Data.Common;
using Weasel.Storage;

namespace Polecat.Storage;

/// <summary>
///     Client-side write binder for a range-partitioned document's partition column (#211,
///     #273 phase D slice 3). Binds <see cref="DocumentPartitioning.GetValue" /> on every
///     write; also serves as the descriptor's partition-PK binder so the shared update
///     operations can target the correct partition row (Marten bug #4223 analog — the MERGE
///     ON clause carries the partition predicate).
/// </summary>
internal sealed class PolecatPartitionColumnBinder<TDoc> : IDocumentMetadataBinder<TDoc>
    where TDoc : notnull
{
    private readonly DocumentPartitioning _partitioning;

    public PolecatPartitionColumnBinder(DocumentPartitioning partitioning)
    {
        _partitioning = partitioning;
    }

    public string ColumnName => _partitioning.ColumnName;

    public string ValueSql => "?";

    public void BindParameter(DbParameter parameter, TDoc document, IStorageSession session)
    {
        var value = _partitioning.GetValue(document);
        parameter.Value = value ?? DBNull.Value;
        // The SqlServer command builder pre-types fresh parameters as strings; retype from
        // the partition value's CLR type so SqlClient doesn't coerce (e.g. DateTimeOffset).
        parameter.DbType = value switch
        {
            int => System.Data.DbType.Int32,
            long => System.Data.DbType.Int64,
            Guid => System.Data.DbType.Guid,
            bool => System.Data.DbType.Boolean,
            DateTimeOffset => System.Data.DbType.DateTimeOffset,
            DateTime => System.Data.DbType.DateTime2,
            DateOnly => System.Data.DbType.Date,
            decimal => System.Data.DbType.Decimal,
            double => System.Data.DbType.Double,
            _ => System.Data.DbType.String
        };
    }

    public void Apply(DbDataReader reader, int columnOrdinal, TDoc document, IStorageSession session)
    {
        // The canonical value lives in the document's JSON; the duplicated partition column
        // is never projected back.
    }

    public BulkColumnValue GetBulkValue(TDoc document)
    {
        var value = _partitioning.GetValue(document);
        var type = value switch
        {
            int => StorageColumnType.Int,
            long => StorageColumnType.Long,
            Guid => StorageColumnType.Guid,
            bool => StorageColumnType.Boolean,
            DateTimeOffset or DateTime => StorageColumnType.Timestamp,
            _ => StorageColumnType.String
        };
        return new BulkColumnValue(value ?? DBNull.Value, type);
    }
}
