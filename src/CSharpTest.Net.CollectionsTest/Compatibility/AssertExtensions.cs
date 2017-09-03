using Xunit;

namespace CSharpTest.Net.Collections.Test.Compatibility
{
    public static class AssertExtensions
    {
        public static void Fail(this Assert assert, string message = null)
        {
            Assert.True(false, message);
        }
    }
}
