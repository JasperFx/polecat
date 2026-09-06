using JasperFx.Events;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

// String-keyed sibling of QuestAggregate. The SG keys the generated evolver on
// the document's `Id` property type, so a Guid-id aggregate can't double-serve
// string-keyed streams under Polecat 4 the way it did under 3.x's FEC fallback.
public partial class StringQuestAggregate
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public List<string> Members { get; } = [];
    public int MonstersSlain { get; set; }

    public static StringQuestAggregate Create(QuestStarted e) => new() { Name = e.Name };
    public void Apply(MembersJoined e) => Members.AddRange(e.Members);
    public void Apply(MonsterSlain e) => MonstersSlain++;
}

/* The six facts this file carried were retired when Polecat enrolled
 * AlwaysEnforceConsistencyCompliance (#556). The shared suite covers all six and four more, across
 * both stream identities:
 *
 *   no_events_appended_without_flag_does_not_throw       -> with_the_flag_off_an_empty_unit_of_work_does_not_check_the_version
 *   always_enforce_consistency_throws_when_version_changed -> with_the_flag_on_an_empty_unit_of_work_fails_on_version_drift
 *   always_enforce_consistency_succeeds_when_version_unchanged -> with_the_flag_on_and_no_drift_an_empty_unit_of_work_still_commits
 *   always_enforce_consistency_with_events_still_checks_version -> with_the_flag_on_and_events_present_the_version_is_still_checked
 *   always_enforce_consistency_with_string_stream_id     -> with_the_flag_on_and_no_drift_a_string_identified_stream_commits
 *   always_enforce_consistency_stream_not_found_throws   -> with_the_flag_on_a_never_created_stream_commits
 *
 * That last pairing is worth reading twice: the local test's NAME said "throws", and its body
 * asserted the opposite -- that a never-created stream commits, because 0 == 0 is not a conflict.
 * The suite spells the behaviour the assertions actually pinned.
 *
 * StringQuestAggregate stays: it is a string-identified aggregate that
 * Projections/projection_sg_dispatch_audit_tests.cs enumerates as a source-generator dispatch case,
 * so it outlives the tests it was introduced for.
 */
