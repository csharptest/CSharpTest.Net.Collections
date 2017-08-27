using System;

namespace CSharpTest.Net.Collections
{
    public class InvalidNodeHandleException : Exception
    {
        public InvalidNodeHandleException(Exception innerException, int hResult, string message) : base(message, innerException)
        {
            HResult = hResult;
        }

        public InvalidNodeHandleException()
            : this(null, -1, Resources.InvalidNodeHandleException)
        {
        }

        public static void Assert(bool condition)
        {
            if (!condition) throw new InvalidNodeHandleException();
        }
    }
}