using Microsoft.Data.SqlClient;
using Polecat.Tests.Harness;
using Weasel.Storage;

namespace Polecat.Tests.Storage;

/// <summary>
///     Polecat sessions implement the dialect-neutral Weasel.Storage.IStorageSession — the
///     operation/session context of the shared closed-shape storage runtime (#273). StorageFor
///     stays guarded until Polecat's document storage retargets onto the shared bases (phases D/E).
/// </summary>
public class storage_session_seam_tests : OneOffConfigurationsContext
{
    [Fact]
    public async Task sessions_are_the_shared_storage_session_seam()
    {
        await using var query = theStore.QuerySession();
        query.ShouldBeAssignableTo<IStorageSession>();

        await using var lightweight = theStore.LightweightSession();
        lightweight.ShouldBeAssignableTo<IStorageSession>();

        await using var identity = theStore.IdentitySession();
        identity.ShouldBeAssignableTo<IStorageSession>();
    }

    [Fact]
    public async Task serializer_database_and_versions_are_wired()
    {
        await using var session = theStore.LightweightSession();
        var seam = (IStorageSession)session;

        // The default Serializer implements IStorageSerializer natively — no adapter wrapping
        seam.Serializer.ShouldBeSameAs(theStore.Options.Serializer);
        seam.Database.ShouldBeSameAs(theDatabase);

        seam.Versions.StoreVersion<Target, Guid>(Guid.NewGuid(), Guid.NewGuid());
        var id = Guid.NewGuid();
        var version = Guid.NewGuid();
        seam.Versions.StoreVersion<Target, Guid>(id, version);
        seam.Versions.VersionFor<Target, Guid>(id).ShouldBe(version);

        seam.Versions.StoreRevision<Target, Guid>(id, 42);
        seam.Versions.RevisionFor<Target, Guid>(id).ShouldBe(42);

        seam.Versions.ClearVersion<Target, Guid>(id);
        seam.Versions.VersionFor<Target, Guid>(id).ShouldBeNull();

        seam.ChangeTrackers.ShouldBeEmpty();
        seam.ItemMap.ShouldBeEmpty();
        seam.Concurrency.ShouldBe(ConcurrencyChecks.Enabled);
    }

    [Fact]
    public async Task execute_reader_async_routes_db_command_through_the_session()
    {
        await using var session = theStore.QuerySession();
        var seam = (IStorageSession)session;

        var command = new SqlCommand("SELECT 42");
        await using var reader = await seam.ExecuteReaderAsync(command);

        (await reader.ReadAsync()).ShouldBeTrue();
        reader.GetInt32(0).ShouldBe(42);
    }

    [Fact]
    public async Task execute_reader_async_rejects_non_sqlclient_commands()
    {
        await using var session = theStore.QuerySession();
        var seam = (IStorageSession)session;

        await using var foreign = new NotASqlCommand();
        await Should.ThrowAsync<ArgumentException>(() => seam.ExecuteReaderAsync(foreign));
    }

    private sealed class NotASqlCommand : System.Data.Common.DbCommand
    {
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override System.Data.CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override System.Data.UpdateRowSource UpdatedRowSource { get; set; }
        protected override System.Data.Common.DbConnection? DbConnection { get; set; }
        protected override System.Data.Common.DbParameterCollection DbParameterCollection => null!;
        protected override System.Data.Common.DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => 0;
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }
        protected override System.Data.Common.DbParameter CreateDbParameter() => null!;
        protected override System.Data.Common.DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior) => null!;
    }

    [Fact]
    public async Task metadata_context_flows_the_session_metadata()
    {
        ConfigureStore(opts =>
        {
            opts.Events.EnableCorrelationId = true;
            opts.Events.EnableHeaders = true;
        });

        await using var session = theStore.LightweightSession();
        var seam = (IStorageSession)session;

        session.CorrelationId = "corr-1";
        session.LastModifiedBy = "jeremy";
        session.SetHeader("k", "v");

        seam.CorrelationId.ShouldBe("corr-1");
        seam.CurrentUserName.ShouldBe("jeremy");
        seam.Headers!["k"].ShouldBe("v");
        seam.CorrelationIdEnabled.ShouldBeTrue();
        seam.CausationIdEnabled.ShouldBeFalse();
        seam.HeadersEnabled.ShouldBeTrue();
        seam.UserNameEnabled.ShouldBeFalse();
    }

    [Fact]
    public async Task identity_map_session_routes_mark_as_loaded_into_the_identity_map()
    {
        await theStore.Advanced.Clean.CompletelyRemoveAllAsync();

        var target = new Target { Number = 7 };

        await using (var setup = theStore.LightweightSession())
        {
            setup.Store(target);
            await setup.SaveChangesAsync();
        }

        await using var session = theStore.IdentitySession();
        var seam = (IStorageSession)session;

        var replacement = new Target { Id = target.Id, Number = 99 };
        seam.MarkAsDocumentLoaded(replacement.Id, replacement);

        // The identity map should now return the marked instance instead of loading from the db
        var loaded = await session.LoadAsync<Target>(target.Id);
        loaded.ShouldBeSameAs(replacement);
    }

    [Fact]
    public async Task lightweight_session_ignores_identity_map_hooks()
    {
        await using var session = theStore.LightweightSession();
        var seam = (IStorageSession)session;

        // no-op, must not throw
        seam.MarkAsDocumentLoaded(Guid.NewGuid(), new Target());
        seam.MarkAsAddedForStorage(Guid.NewGuid(), new Target());
    }

    [Fact]
    public async Task next_temp_table_name_is_session_unique_and_incrementing()
    {
        await using var session = theStore.QuerySession();
        var seam = (IStorageSession)session;

        var first = seam.NextTempTableName();
        var second = seam.NextTempTableName();

        first.ShouldStartWith("#pc_temp_");
        second.ShouldNotBe(first);
    }

    [Fact]
    public async Task storage_for_is_guarded_until_closed_shape_storage_lands()
    {
        await using var session = theStore.QuerySession();
        var seam = (IStorageSession)session;

        Should.Throw<NotSupportedException>(() => seam.StorageFor(typeof(Target)))
            .Message.ShouldContain("polecat#273");
        Should.Throw<NotSupportedException>(() => seam.StorageFor<Target>());
    }
}
