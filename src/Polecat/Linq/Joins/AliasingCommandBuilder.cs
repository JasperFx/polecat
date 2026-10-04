using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Weasel.SqlServer;

namespace Polecat.Linq.Joins;

/// <summary>
///     Wraps an ICommandBuilder and applies table alias replacement to string fragments.
///     Replaces bare "data" → "alias.data" and bare "id" → "alias.id" in SQL text.
/// </summary>
internal class AliasingCommandBuilder : ICommandBuilder
{
    private readonly ICommandBuilder _inner;
    private readonly string _alias;

    public AliasingCommandBuilder(ICommandBuilder inner, string alias)
    {
        _inner = inner;
        _alias = alias;
    }

    public string TenantId
    {
        get => _inner.TenantId;
        set => _inner.TenantId = value;
    }

    public string? LastParameterName => _inner.LastParameterName;

    /// <summary>
    ///     Weasel 9.40.0 (weasel#675): how many parameters are already bound on the command being
    ///     built, so a fragment rendering a value list can decide against
    ///     <c>SqlServerMigrator.MaxParametersPerCommand</c> instead of guessing (#710, #721).
    /// </summary>
    /// <remarks>
    ///     The wrapper owns no parameters of its own — it rewrites locators and delegates every bind —
    ///     so the inner builder's count IS this builder's count. Returning 0 here would be the
    ///     dangerous answer: a join's fragment would read "nothing bound yet" on a command that had
    ///     already spent its budget, which is precisely the miscount
    ///     <see cref="JsonValueList.ShouldBindAsJsonArray" /> consults this to avoid.
    /// </remarks>
    public int ParameterCount => _inner.ParameterCount;

    public void Append(string sql)
    {
        _inner.Append(JoinStatement.AliasLocator(sql, _alias));
    }

    public void Append(char character) => _inner.Append(character);

    public SqlParameter AppendParameter<T>(T value) => _inner.AppendParameter(value);
    public SqlParameter AppendParameter<T>(T value, SqlDbType dbType) => _inner.AppendParameter(value, dbType);
    public SqlParameter AppendParameter(object value) => _inner.AppendParameter(value);
    public SqlParameter AppendParameter(object? value, SqlDbType? dbType) => _inner.AppendParameter(value, dbType);
    public void AppendParameters(params object[] parameters) => _inner.AppendParameters(parameters);
    public IGroupedParameterBuilder CreateGroupedParameterBuilder(char? seperator = null) => _inner.CreateGroupedParameterBuilder(seperator);

    // Weasel 9.16.0 (weasel#339): the SqlServer ICommandBuilder members now `new`-shadow the
    // dialect-neutral Weasel.Core.ICommandBuilder declarations (which return DbParameter /
    // Weasel.Core.IGroupedParameterBuilder). Satisfy the Core contract explicitly by delegating
    // to the inner builder — the SqlClient-typed overloads above satisfy the SqlServer shadows.
    System.Data.Common.DbParameter Weasel.Core.ICommandBuilder.AppendParameter(object value) => _inner.AppendParameter(value);
    Weasel.Core.IGroupedParameterBuilder Weasel.Core.ICommandBuilder.CreateGroupedParameterBuilder(char? seperator) => _inner.CreateGroupedParameterBuilder(seperator);
    public SqlParameter[] AppendWithParameters(string text) => _inner.AppendWithParameters(JoinStatement.AliasLocator(text, _alias));
    public SqlParameter[] AppendWithParameters(string text, char placeholder) => _inner.AppendWithParameters(JoinStatement.AliasLocator(text, _alias), placeholder);
    // Weasel 9.7.0 (weasel#324): dialect-neutral DbParameter[] variants on the shared Weasel.Core.ICommandBuilder.
    public System.Data.Common.DbParameter[] AppendWithDbParameters(string text) => _inner.AppendWithDbParameters(JoinStatement.AliasLocator(text, _alias));
    public System.Data.Common.DbParameter[] AppendWithDbParameters(string text, char placeholder) => _inner.AppendWithDbParameters(JoinStatement.AliasLocator(text, _alias), placeholder);
    public void StartNewCommand() => _inner.StartNewCommand();

    // Mirror the RUC annotation that Weasel.SqlServer's ICommandBuilder.AddParameters(object)
    // declaration carries (added in Weasel 9.0.0-alpha.6). The AOT-trim-clean route is the
    // IDictionary<string, T> overload below; this override exists so AliasingCommandBuilder
    // satisfies the interface contract verbatim.
    [RequiresUnreferencedCode("AddParameters(object) reflects on the parameters object's public properties via Type.GetProperties(). Use the IDictionary<string, T> overload when publishing AOT-trim-clean.")]
    public void AddParameters(object parameters) => _inner.AddParameters(parameters);
    public void AddParameters(IDictionary<string, object?> parameters) => _inner.AddParameters(parameters);
    public void AddParameters<T>(IDictionary<string, T> parameters) => _inner.AddParameters(parameters);
}
