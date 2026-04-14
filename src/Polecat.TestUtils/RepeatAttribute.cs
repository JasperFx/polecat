using System.Reflection;
using Xunit.Sdk;

namespace Polecat.TestUtils;

public class RepeatAttribute(int count) : DataAttribute
{
    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        return Enumerable.Range(1, count).Select(i => new object[] { i });
    }
}