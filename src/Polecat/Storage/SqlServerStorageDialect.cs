using System.Buffers;
using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Weasel.SqlServer;
using Weasel.SqlServer.SqlGeneration;
using Weasel.Storage;

namespace Polecat.Storage;

/// <summary>
///     SQL Server implementation of the shared closed-shape storage runtime's
///     <see cref="IStorageDialect" /> strategy (#273 phase D) — the per-database-engine half of
///     document load/parameter binding. Mirrors Marten's <c>PostgresStorageDialect&lt;TId&gt;</c>.
///     SQL conventions (named <c>@id</c>/<c>@tenant_id</c>/<c>@ids</c> parameters, OPENJSON for
///     id arrays) are co-designed with the SQL Server <c>DocumentStorageDescriptor</c> builder
///     that generates the loader SQL.
/// </summary>
internal sealed class SqlServerStorageDialect<TId> : IStorageDialect
{
    public static readonly IStorageDialect Instance = new SqlServerStorageDialect<TId>();

    private static readonly SqlDbType IdParameterType =
        SqlServerProvider.Instance.ToParameterType(typeof(TId));

    private SqlServerStorageDialect()
    {
    }

    public System.Data.Common.DbCommand BuildLoadCommand(string loaderSql, object rawId, string? tenant)
    {
        var command = new SqlCommand(loaderSql);
        command.Parameters.Add(new SqlParameter("id", rawId) { SqlDbType = IdParameterType });
        if (tenant is not null)
        {
            command.Parameters.Add(new SqlParameter("tenant_id", tenant) { SqlDbType = SqlDbType.NVarChar });
        }

        return command;
    }

    /// <summary>
    ///     SQL Server has no array parameters; ids travel as a JSON array string the load-many SQL
    ///     unpacks with <c>OPENJSON(@ids)</c>. The JSON is written manually (scalar ids only) so
    ///     the dialect stays trim/AOT-clean — no reflection-based serialization.
    /// </summary>
    public System.Data.Common.DbParameter CreateIdArrayParameter(Array rawIds, Type rawSqlType)
    {
        return new SqlParameter("ids", WriteIdsAsJsonArray(rawIds)) { SqlDbType = SqlDbType.NVarChar };
    }

    public System.Data.Common.DbCommand BuildLoadManyCommand(string loadArraySql,
        System.Data.Common.DbParameter idArrayParameter, string? tenant)
    {
        var command = new SqlCommand(loadArraySql);
        command.Parameters.Add((SqlParameter)idArrayParameter);
        if (tenant is not null)
        {
            command.Parameters.Add(new SqlParameter("tenant_id", tenant) { SqlDbType = SqlDbType.NVarChar });
        }

        return command;
    }

    public Weasel.Core.SqlGeneration.ISqlFragment ByIdFilter(object rawId) => new SqlServerByIdFilter(rawId, IdParameterType);

    /// <summary>SQL Server error 208: "Invalid object name '%s'."</summary>
    public bool IsUndefinedTable(Exception exception)
        => exception is SqlException sql && sql.Number == 208;

    public void SetParameterType(System.Data.Common.DbParameter parameter, StorageColumnType type)
        => ((SqlParameter)parameter).SqlDbType = type switch
        {
            StorageColumnType.String => SqlDbType.NVarChar,
            StorageColumnType.Guid => SqlDbType.UniqueIdentifier,
            StorageColumnType.Long => SqlDbType.BigInt,
            StorageColumnType.Int => SqlDbType.Int,
            StorageColumnType.Boolean => SqlDbType.Bit,
            StorageColumnType.Timestamp => SqlDbType.DateTimeOffset,
            // Polecat binds JSON as string values throughout; SQL Server 2025's native JSON
            // column type (and nvarchar-backed JSON) accepts nvarchar input.
            StorageColumnType.Json => SqlDbType.NVarChar,
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };

    public void SetIdParameterType(System.Data.Common.DbParameter parameter, Type rawSqlType)
        => ((SqlParameter)parameter).SqlDbType = SqlServerProvider.Instance.ToParameterType(rawSqlType);

    private static string WriteIdsAsJsonArray(Array rawIds)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var id in rawIds)
            {
                switch (id)
                {
                    case Guid guid:
                        writer.WriteStringValue(guid);
                        break;
                    case string text:
                        writer.WriteStringValue(text);
                        break;
                    case int number:
                        writer.WriteNumberValue(number);
                        break;
                    case long number:
                        writer.WriteNumberValue(number);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(rawIds),
                            $"Unsupported raw id type {id?.GetType().FullName ?? "null"}; expected Guid, string, int, or long.");
                }
            }

            writer.WriteEndArray();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}

/// <summary>
///     "id = @p" filter over the shared SQL-generation contract, the SQL Server counterpart of
///     Marten's <c>ByIdFilter</c>. Implements the Weasel.SqlServer fragment interface (whose
///     default interface method bridges the shared <c>Weasel.Core.SqlGeneration.ISqlFragment</c>).
/// </summary>
internal sealed class SqlServerByIdFilter : ISqlFragment
{
    private readonly CommandParameter _parameter;

    public SqlServerByIdFilter(object value, SqlDbType dbType)
    {
        _parameter = new CommandParameter(value, dbType);
    }

    public void Apply(CommandBuilder builder)
    {
        builder.Append("id = ");
        _parameter.Apply(builder);
    }

    public bool Contains(string sqlText) => false;
}
