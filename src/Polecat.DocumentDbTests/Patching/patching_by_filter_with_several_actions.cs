using System.Data;
using JasperFx;
using JasperFx.Metadata;
using Microsoft.Data.SqlClient;
using Polecat.Patching;
using Polecat.Tests.Harness;
using Polecat.Tests.Session;

namespace Polecat.Tests.Patching;

/// <summary>
///     #916. Each action of a patch is its own <c>UPDATE</c>, and a patch by filter ran the filter on
///     each, so an action that changed what the filter looks at took the row away from the actions
///     after it. Every fact runs on both storage shapes, because each has its own <c>JSON_MODIFY</c>.
/// </summary>
public class patching_by_filter_with_several_actions : OneOffConfigurationsContext
{
    public class Ticket
    {
        public Guid Id { get; set; }
        public string Status { get; set; } = "";
        public int Count { get; set; }
        public string? Note { get; set; }
    }

    public class NamedTicket
    {
        public string Id { get; set; } = "";
        public string Status { get; set; } = "";
        public int Count { get; set; }
    }

    public class NumberedTicket
    {
        public int Id { get; set; }
        public string Status { get; set; } = "";
        public int Count { get; set; }
    }

    public class LongTicket
    {
        public long Id { get; set; }
        public string Status { get; set; } = "";
        public int Count { get; set; }
    }

    public class RevisionedTicket : IRevisioned
    {
        public Guid Id { get; set; }
        public int Version { get; set; }
    }

    public class TenantedTicket
    {
        public Guid Id { get; set; }
        public string Status { get; set; } = "";
        public int Count { get; set; }
    }

    private const int SnapshotUpdateConflict = 3960;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private void ConfigureFor(bool nativeJson)
    {
        if (nativeJson)
        {
            Assert.SkipUnless(ConnectionSource.SupportsNativeJson, "This server has no native json type.");
        }

        ConfigureStore(opts =>
        {
            opts.UseNativeJsonType = nativeJson;
            opts.Schema.For<TenantedTicket>().MultiTenanted();
        });
    }

    private async Task<Guid> SeedAsync(string status, int count = 0)
    {
        var ticket = new Ticket { Id = Guid.NewGuid(), Status = status, Count = count };
        await using var session = theStore.LightweightSession();
        session.Store(ticket);
        await session.SaveChangesAsync(Ct);
        return ticket.Id;
    }

    private async Task<Ticket> LoadAsync(Guid id)
    {
        await using var query = theStore.QuerySession();
        return (await query.LoadAsync<Ticket>(id, Ct)).ShouldNotBeNull();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_action_that_changes_what_the_filter_reads_does_not_stop_the_next(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await SeedAsync("new");

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count)
            .Set(x => x.Note, "seen");
        await session.SaveChangesAsync(Ct);

        var ticket = await LoadAsync(id);
        ticket.Status.ShouldBe("done");
        ticket.Count.ShouldBe(1);
        ticket.Note.ShouldBe("seen");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task an_action_whose_own_change_stops_the_filter_matching_runs_each_time(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var id = await SeedAsync("new", 1);

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Count == 1)
            .Increment(x => x.Count)
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        (await LoadAsync(id)).Count.ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_row_the_filter_does_not_match_is_left_alone(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        var matched = await SeedAsync("new");
        var other = await SeedAsync("old");

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        (await LoadAsync(matched)).Count.ShouldBe(1);

        var untouched = await LoadAsync(other);
        untouched.Status.ShouldBe("old");
        untouched.Count.ShouldBe(0);
    }

    [Fact]
    public async Task a_filter_that_matches_nothing_writes_nothing()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var other = await SeedAsync("old");

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Status == "nothing like this")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        (await LoadAsync(other)).Status.ShouldBe("old");
    }

    [Fact]
    public async Task a_string_id()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var id = Guid.NewGuid().ToString();

        await using var session = theStore.LightweightSession();
        session.Store(new NamedTicket { Id = id, Status = "new" });
        await session.SaveChangesAsync(Ct);

        session.Patch<NamedTicket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        var ticket = await session.LoadAsync<NamedTicket>(id, Ct);
        ticket.ShouldNotBeNull().Status.ShouldBe("done");
        ticket.Count.ShouldBe(1);
    }

    [Fact]
    public async Task an_int_id()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var document = new NumberedTicket { Status = "new" };

        await using var session = theStore.LightweightSession();
        session.Store(document);
        await session.SaveChangesAsync(Ct);

        session.Patch<NumberedTicket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        var ticket = await session.LoadAsync<NumberedTicket>(document.Id, Ct);
        ticket.ShouldNotBeNull().Status.ShouldBe("done");
        ticket.Count.ShouldBe(1);
    }

    [Fact]
    public async Task a_long_id()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var document = new LongTicket { Status = "new" };

        await using var session = theStore.LightweightSession();
        session.Store(document);
        await session.SaveChangesAsync(Ct);

        session.Patch<LongTicket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        await session.SaveChangesAsync(Ct);

        var ticket = await session.LoadAsync<LongTicket>(document.Id, Ct);
        ticket.ShouldNotBeNull().Status.ShouldBe("done");
        ticket.Count.ShouldBe(1);
    }

    [Fact]
    public async Task only_the_sessions_tenant_is_patched_for_an_id_two_tenants_share()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var id = Guid.NewGuid();

        foreach (var tenant in new[] { "tenant_a", "tenant_b" })
        {
            await using var seed = theStore.LightweightSession(new SessionOptions { TenantId = tenant });
            seed.Store(new TenantedTicket { Id = id, Status = "new" });
            await seed.SaveChangesAsync(Ct);
        }

        await using (var session = theStore.LightweightSession(new SessionOptions { TenantId = "tenant_a" }))
        {
            session.Patch<TenantedTicket>(x => x.Status == "new")
                .Set(x => x.Status, "done")
                .Increment(x => x.Count);
            await session.SaveChangesAsync(Ct);

            var patched = await session.LoadAsync<TenantedTicket>(id, Ct);
            patched.ShouldNotBeNull().Status.ShouldBe("done");
            patched.Count.ShouldBe(1);
        }

        await using var other = theStore.LightweightSession(new SessionOptions { TenantId = "tenant_b" });
        var untouched = await other.LoadAsync<TenantedTicket>(id, Ct);
        untouched.ShouldNotBeNull().Status.ShouldBe("new");
        untouched.Count.ShouldBe(0);
    }

    [Fact]
    public async Task two_filtered_patches_in_one_save()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var first = await SeedAsync("first");
        var second = await SeedAsync("second");

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Status == "first")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        session.Patch<Ticket>(x => x.Status == "second")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count, 2);
        await session.SaveChangesAsync(Ct);

        (await LoadAsync(first)).Count.ShouldBe(1);
        (await LoadAsync(second)).Count.ShouldBe(2);
    }

    /// <summary>
    ///     The ids go into a table variable, which must add no result set for the operations after it to
    ///     read theirs out of (#831).
    /// </summary>
    [Fact]
    public async Task an_operation_after_it_in_the_save_still_reads_its_own_result()
    {
        ConfigureFor(ConnectionSource.SupportsNativeJson);
        var id = await SeedAsync("new");

        var revisioned = new RevisionedTicket { Id = Guid.NewGuid() };

        await using var session = theStore.LightweightSession();
        session.Patch<Ticket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);
        session.Store(revisioned);
        await session.SaveChangesAsync(Ct);

        revisioned.Version.ShouldBe(1);
        (await LoadAsync(id)).Count.ShouldBe(1);
    }

    /// <summary>
    ///     Reading the rows first must not turn a conflict into a lost write.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task a_snapshot_session_still_gets_its_conflict(bool nativeJson)
    {
        ConfigureFor(nativeJson);
        await session_isolation_level_tests.AllowSnapshotIsolationAsync(Ct);

        var id = await SeedAsync("new");

        await using var session = theStore.LightweightSession(new SessionOptions { IsolationLevel = IsolationLevel.Snapshot });
        (await session.LoadAsync<Ticket>(id, Ct)).ShouldNotBeNull();

        await using (var writer = theStore.LightweightSession())
        {
            writer.Store(new Ticket { Id = id, Status = "new", Note = "theirs" });
            await writer.SaveChangesAsync(Ct);
        }

        session.Patch<Ticket>(x => x.Status == "new")
            .Set(x => x.Status, "done")
            .Increment(x => x.Count);

        var conflict = await Should.ThrowAsync<SqlException>(() => session.SaveChangesAsync(Ct));
        conflict.Number.ShouldBe(SnapshotUpdateConflict);

        (await LoadAsync(id)).Note.ShouldBe("theirs");
    }
}
