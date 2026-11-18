#nullable enable
using JasperFx.Events;
using JasperFx.Events.Tags;
using Polecat.Events.Dcb;
using Polecat.Tests.Harness;

namespace Polecat.Tests.Events;

#region sample_polecat_dcb_tag_type_definitions
// Strong-typed tag identifiers
public record StudentId(Guid Value);
public record CourseId(Guid Value);
#endregion

#region sample_polecat_dcb_domain_events
// Domain events
public record StudentEnrolled(string StudentName, string CourseName);
public record AssignmentSubmitted(string AssignmentName, int Score);
public record StudentDropped(string Reason);
#endregion

// Event with tag-typed properties for inference testing
public record StudentGraded(StudentId StudentId, CourseId CourseId, int Grade);

// Event with NO tag-typed properties — should fail inference
public record SystemNotification(string Message);

#region sample_polecat_dcb_aggregate
// Aggregate for DCB
public partial class StudentCourseEnrollment
{
    public Guid Id { get; set; }
    public string StudentName { get; set; } = "";
    public string CourseName { get; set; } = "";
    public List<string> Assignments { get; set; } = new();
    public bool IsDropped { get; set; }

    public void Apply(StudentEnrolled e)
    {
        StudentName = e.StudentName;
        CourseName = e.CourseName;
    }

    public void Apply(AssignmentSubmitted e)
    {
        Assignments.Add(e.AssignmentName);
    }

    public void Apply(StudentDropped e)
    {
        IsDropped = true;
    }
}
#endregion
