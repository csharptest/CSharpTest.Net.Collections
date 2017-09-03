using System;

namespace CSharpTest.Net.Collections.Exceptions
{
    public class DuplicateKeyException : Exception
    {
        public DuplicateKeyException(Exception innerException, int hResult, string message) : base(message, innerException)
        {
            HResult = hResult;
        }

        public DuplicateKeyException() : this(null, -1, Resources.DuplicateKeyException)
        {
        }

        public static void Assert(bool condition)
        {
            if (!condition) throw new DuplicateKeyException();
        }
    }
}