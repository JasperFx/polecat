using System.Data.SqlTypes;
using Polecat.Internal;
using Shouldly;

namespace Polecat.Tests.Utilities;

/// <summary>
/// #235: auto-assigned Guid ids should be generated sequentially so they sort in SQL Server's
/// <c>uniqueidentifier</c> order, avoiding clustered-index fragmentation. SQL Server compares
/// uniqueidentifier values by their last six bytes first, which <see cref="SqlGuid"/> models
/// exactly, so consecutively generated values must be monotonically increasing under SqlGuid.
/// </summary>
public class SequentialGuidTests
{
    [Fact]
    public void generates_distinct_values()
    {
        var set = new HashSet<Guid>();
        for (var i = 0; i < 1000; i++)
        {
            set.Add(SequentialGuid.NewGuid()).ShouldBeTrue();
        }
    }

    [Fact]
    public void values_increase_in_sql_server_ordering()
    {
        var previous = new SqlGuid(SequentialGuid.NewGuid());
        for (var i = 0; i < 500; i++)
        {
            var current = new SqlGuid(SequentialGuid.NewGuid());
            current.CompareTo(previous).ShouldBeGreaterThan(0);
            previous = current;
        }
    }

    [Fact]
    public void is_not_an_empty_guid()
    {
        SequentialGuid.NewGuid().ShouldNotBe(Guid.Empty);
    }
}
