using System;

namespace CSharpTest.Net.Collections
{
    public class LurchTableCorruptionException : Exception
    {
        public LurchTableCorruptionException(Exception innerException, int hResult, string message) : base(message, innerException)
        {
            HResult = hResult;
        }

        public LurchTableCorruptionException() : this(null, -1, Resources.LurchTableCorruptionException)
        {
        }
    }
}