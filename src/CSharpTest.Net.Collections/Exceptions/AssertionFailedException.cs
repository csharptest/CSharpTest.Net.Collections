using System;

namespace CSharpTest.Net.Collections
{
    public class AssertionFailedException : Exception
    {
        public AssertionFailedException(Exception innerException, int hResult, string message) : base(message, innerException)
        {
            HResult = hResult;
        }

        public AssertionFailedException() : this(null, -1, Resources.AssertionFailedException())
        {
        }

        public AssertionFailedException(string message) : this(null, -1, Resources.AssertionFailedException(message))
        {
        }

        public static void Assert(bool condition)
        {
            if (!condition) throw new AssertionFailedException();
        }
    }
}