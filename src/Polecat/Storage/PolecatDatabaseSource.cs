using JasperFx.Descriptors;
using Weasel.Core.Migrations;

namespace Polecat.Storage;

/// <summary>
///     Adapts Polecat's <see cref="ITenancy" /> to Weasel's <see cref="IDatabaseSource" /> so the
///     <c>db-apply</c> / <c>db-assert</c> / <c>db-dump</c> CLI commands discover the Polecat
///     database(s). #501.
/// </summary>
/// <remarks>
///     <para>
///         Weasel's CLI resolves <see cref="IDatabaseSource" /> out of the container
///         (<c>WeaselInput.FilterDatabases</c>). Marten satisfies this because its own
///         <c>ITenancy</c> extends <c>IDatabaseSource</c> directly. Polecat's does not, and
///         registering nothing meant every <c>db-*</c> command failed with "No Weasel databases were
///         registered in this application" — which reads as a misconfigured host rather than an
///         unsupported command, especially to anyone carrying a Marten habit.
///     </para>
///     <para>
///         This is an adapter rather than widening <see cref="ITenancy" /> to extend
///         <c>IDatabaseSource</c> the way Marten's does. <see cref="ITenancy" /> is public and has
///         implementers outside this repo, so extending it is a breaking change — and it would pull
///         Weasel's migration contract into Polecat's tenancy abstraction for what is purely a CLI
///         concern. The pieces needed are already here: <see cref="PolecatDatabase" /> extends
///         <c>DatabaseBase&lt;SqlConnection&gt;</c> and is therefore already a Weasel
///         <see cref="IDatabase" />, and <see cref="ITenancy.BuildDatabasesAsync" /> already
///         enumerates every tenant database including the dynamic <c>MasterTableTenancy</c> case.
///     </para>
///     <para>
///         The store is resolved lazily, not injected, for the same reason
///         <c>PolecatSystemPart</c> is: the <c>IConfigurePolecat</c> chain has to have run before the
///         tenancy is meaningful, and that only happens on first <c>IDocumentStore</c> resolution.
///     </para>
/// </remarks>
internal class PolecatDatabaseSource : IDatabaseSource
{
    private readonly Func<IDocumentStore> _store;

    public PolecatDatabaseSource(Func<IDocumentStore> store)
    {
        _store = store;
    }

    private ITenancy Tenancy => _store().Options.Tenancy!;

    public DatabaseCardinality Cardinality => Tenancy.Cardinality;

    public async ValueTask<IReadOnlyList<IDatabase>> BuildDatabases()
    {
        var databases = await Tenancy.BuildDatabasesAsync().ConfigureAwait(false);
        return databases;
    }

    public async ValueTask<DatabaseUsage> DescribeDatabasesAsync(CancellationToken token)
    {
        var tenancy = Tenancy;
        var databases = await tenancy.BuildDatabasesAsync(token).ConfigureAwait(false);

        if (tenancy.Cardinality == DatabaseCardinality.Single)
        {
            return new DatabaseUsage
            {
                Cardinality = DatabaseCardinality.Single,
                MainDatabase = databases.Count == 1
                    ? databases[0].Describe()
                    : tenancy.GetDatabase(tenancy.DefaultTenantId).Describe()
            };
        }

        return new DatabaseUsage
        {
            Cardinality = tenancy.Cardinality,
            Databases = databases.Select(x => x.Describe()).ToList()
        };
    }
}
