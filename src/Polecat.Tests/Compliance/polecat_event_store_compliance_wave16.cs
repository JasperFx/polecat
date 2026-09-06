using JasperFx.Events.ComplianceTests;

namespace Polecat.Tests.Compliance;

/*
 * Wave 16 -- the upcasting suite (#561 / jasperfx#752), enrolled separately from wave 15 because it
 * is the definition of done for a feature Polecat did not have at all rather than the adoption of a
 * suite for behaviour that already existed.
 */

/// <summary>
///     Event upcasting: an event stored under an older schema is transformed on READ into the
///     current CLR type, so aggregations, projections and subscriptions only ever see the new type.
/// </summary>
/// <remarks>
///     <para>
///         The suite writes its "legacy" rows through a store configured WITHOUT the upcasters and
///         then hands the same schema to a store that has them, which is the only honest way to
///         produce a row that predates the migration. So every fact is reading real old-shaped JSON.
///     </para>
///     <para>
///         The fact worth knowing about is
///         <c>a_typed_append_of_the_old_event_type_does_not_shadow_the_upcaster</c> -- the marten#4680
///         authority rule. Polecat stores a <c>dotnet_type</c> hint next to the event type name, and
///         a registered transformation has to beat that hint: otherwise appending the OLD CLR type
///         into the store that carries the upcaster reads back as the old type, and the upcaster
///         silently does nothing for exactly the rows most likely to exist.
///     </para>
/// </remarks>
public class upcasting_compliance
    : UpcastingCompliance<PolecatComplianceFixture, IDocumentSession, IQuerySession>;
