using System;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Polecat.Projections;
using Polecat.Tests.Harness;
using Shouldly;
using Xunit;

namespace Polecat.Tests.Projections;

// Regression for Polecat#145 / marten#4542 (jasperfx#359): a record aggregate with a
// primary constructor + required members, projected by a partial SingleStreamProjection
// with conventional Create/Apply, used to generate `new T { Required = default! }` in the
// Apply-only / null-snapshot branch — which doesn't compile for a primary-ctor record
// (no parameterless ctor → CS7036). The SG fix emits RuntimeHelpers.GetUninitializedObject
// in that branch and suppresses the redundant standalone evolver. This must build AND run.
public record DiagnostiekActiviteit(string Id)
{
    public required Guid? SubContractorId { get; set; }
    public required string Aanlevercode { get; set; }
    public required int Prestatiecodelijst { get; set; }
}

public record CareReceived(string ProvidedCareId, Guid? SubContractorId, string Prestatiecode, int Prestatiecodelijst);
public record CareImported(string Note);

public partial class DiagnostiekActiviteitProjection : SingleStreamProjection<DiagnostiekActiviteit, string>
{
    public static DiagnostiekActiviteit Create(CareReceived e) => new(e.ProvidedCareId)
    {
        SubContractorId = e.SubContractorId,
        Aanlevercode = e.Prestatiecode,
        Prestatiecodelijst = e.Prestatiecodelijst,
    };

    public void Apply(CareImported e, DiagnostiekActiviteit a) { }
}

[Collection("integration")]
public class required_member_primary_ctor_projection_tests : IntegrationContext
{
    public required_member_primary_ctor_projection_tests(DefaultStoreFixture fixture) : base(fixture) { }

    [Fact]
    public async Task required_member_record_projection_builds_and_runs()
    {
        await StoreOptions(opts =>
        {
            opts.DatabaseSchemaName = "required_member_proj";
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<DiagnostiekActiviteitProjection>(ProjectionLifecycle.Inline);
        });

        var streamKey = "diag-" + Guid.NewGuid();
        var subContractor = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        session.Events.StartStream(streamKey,
            new CareReceived(streamKey, subContractor, "ABC123", 42),
            new CareImported("imported"));
        await session.SaveChangesAsync();

        await using var query = theStore.QuerySession();
        var doc = await query.LoadAsync<DiagnostiekActiviteit>(streamKey);

        doc.ShouldNotBeNull();
        doc.Id.ShouldBe(streamKey);
        doc.SubContractorId.ShouldBe(subContractor);
        doc.Aanlevercode.ShouldBe("ABC123");
        doc.Prestatiecodelijst.ShouldBe(42);
    }
}
