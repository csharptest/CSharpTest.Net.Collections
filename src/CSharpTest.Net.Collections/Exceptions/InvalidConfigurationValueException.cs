using System;

namespace CSharpTest.Net.Collections.Exceptions
{
    public class InvalidConfigurationValueException : ArgumentException
    {
        public InvalidConfigurationValueException(Exception innerException, int hResult, string message) : base(message, innerException)
        {
            HResult = hResult;
        }

        public InvalidConfigurationValueException(string property)
            : this(null, -1, Resources.InvalidConfigurationValueException(property))
        {
        }

        public InvalidConfigurationValueException(string property, string message)
            : this(null, -1, Resources.InvalidConfigurationValueException(property, message))
        {
        }

        public InvalidConfigurationValueException(string property, string message, Exception innerException)
            : this(innerException, -1, Resources.InvalidConfigurationValueException(property, message))
        {
        }

        public static void Assert(bool condition, string property, string message)
        {
            if (!condition) throw new InvalidConfigurationValueException(property, message);
        }
    }
}