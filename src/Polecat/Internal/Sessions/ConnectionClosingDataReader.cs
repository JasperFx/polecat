using System.Collections;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Polecat.Internal.Sessions;

/// <summary>
///     A <see cref="DbDataReader" /> that owns its connection and closes it when the reader is
///     disposed — the behaviour <see cref="System.Data.CommandBehavior.CloseConnection" /> gives a
///     <see cref="SqlCommand" /> reader, and does <b>not</b> give a <see cref="SqlBatch" /> one.
/// </summary>
/// <remarks>
///     <para>
///         Microsoft.Data.SqlClient honours <c>CloseConnection</c> for
///         <c>SqlCommand.ExecuteReaderAsync</c> and silently ignores it for
///         <c>SqlBatch.ExecuteReaderAsync</c>. <see cref="AutoClosingLifetime" /> passed the flag on
///         both paths and relied on it for both, so every batch-executed read leaked its pooled
///         connection: the reader was disposed, the connection was not, and it never returned to the
///         pool.
///     </para>
///     <para>
///         That is nearly every read on a query session — document and event LINQ, batched queries,
///         session loads all execute through <see cref="SqlBatch" />. The symptom is the pool ceiling
///         rather than an error at the call site, so it surfaces far from its cause and only under
///         enough queries per process: "Timeout expired. The timeout period elapsed prior to obtaining
///         a connection from the pool", on whichever query happens to be the one past the limit.
///     </para>
/// </remarks>
internal sealed class ConnectionClosingDataReader : DbDataReader
{
    private readonly DbDataReader _inner;
    private readonly SqlConnection _connection;
    private bool _disposed;

    public ConnectionClosingDataReader(DbDataReader inner, SqlConnection connection)
    {
        _inner = inner;
        _connection = connection;
    }

    /// <summary>
    ///     The reader this wraps, for the few call sites that need the concrete
    ///     <see cref="SqlDataReader" /> rather than the <see cref="DbDataReader" /> contract.
    ///     Reach it through <see cref="DbDataReaderExtensions.AsSqlDataReader" />, never by casting.
    /// </summary>
    internal DbDataReader Inner => _inner;

    public override int Depth => _inner.Depth;
    public override int FieldCount => _inner.FieldCount;
    public override bool HasRows => _inner.HasRows;
    public override bool IsClosed => _inner.IsClosed;
    public override int RecordsAffected => _inner.RecordsAffected;
    public override object this[int ordinal] => _inner[ordinal];
    public override object this[string name] => _inner[name];

    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

    public override char GetChar(int ordinal) => _inner.GetChar(ordinal);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

    public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
    public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
    public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
    public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
    public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);
    public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
    public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
    public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    public override string GetName(int ordinal) => _inner.GetName(ordinal);
    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
    public override string GetString(int ordinal) => _inner.GetString(ordinal);
    public override object GetValue(int ordinal) => _inner.GetValue(ordinal);
    public override int GetValues(object[] values) => _inner.GetValues(values);
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);

    public override T GetFieldValue<T>(int ordinal) => _inner.GetFieldValue<T>(ordinal);

    public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        => _inner.GetFieldValueAsync<T>(ordinal, cancellationToken);

    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        => _inner.IsDBNullAsync(ordinal, cancellationToken);

    public override Stream GetStream(int ordinal) => _inner.GetStream(ordinal);
    public override TextReader GetTextReader(int ordinal) => _inner.GetTextReader(ordinal);
    public override DataTable? GetSchemaTable() => _inner.GetSchemaTable();

    public override Task<DataTable?> GetSchemaTableAsync(CancellationToken cancellationToken = default)
        => _inner.GetSchemaTableAsync(cancellationToken);

    public override bool Read() => _inner.Read();

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        => _inner.ReadAsync(cancellationToken);

    public override bool NextResult() => _inner.NextResult();

    public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        => _inner.NextResultAsync(cancellationToken);

    public override IEnumerator GetEnumerator() => _inner.GetEnumerator();

    public override void Close()
    {
        _inner.Close();
        _connection.Close();
    }

    public override Task CloseAsync() => CloseAsyncCore();

    private async Task CloseAsyncCore()
    {
        await _inner.CloseAsync().ConfigureAwait(false);
        await _connection.CloseAsync().ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;

        if (!disposing) return;

        // The connection is disposed even if the reader's own disposal throws — leaking it is the
        // failure this class exists to prevent, and it is the more expensive of the two to lose.
        try
        {
            _inner.Dispose();
        }
        finally
        {
            _connection.Dispose();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}


/// <summary>
///     Unwrapping helper for the call sites that need SQL Server's concrete reader.
/// </summary>
internal static class DbDataReaderExtensions
{
    /// <summary>
    ///     The <see cref="SqlDataReader" /> behind a reader that may have been wrapped by
    ///     <see cref="ConnectionClosingDataReader" />.
    /// </summary>
    /// <remarks>
    ///     A plain <c>(SqlDataReader)reader</c> cast is what broke when the wrapper was introduced,
    ///     and it broke at runtime rather than at compile time because the parameter is typed as the
    ///     <see cref="DbDataReader" /> base. Casting is therefore a mistake this codebase can make
    ///     silently; this method is the one place that knows about the wrapper, so a future decorator
    ///     only has to be handled here.
    /// </remarks>
    internal static SqlDataReader AsSqlDataReader(this DbDataReader reader)
    {
        return reader switch
        {
            SqlDataReader sql => sql,
            ConnectionClosingDataReader wrapper => wrapper.Inner.AsSqlDataReader(),
            _ => throw new InvalidOperationException(
                $"Expected a {nameof(SqlDataReader)} but got {reader.GetType().FullName}.")
        };
    }
}
