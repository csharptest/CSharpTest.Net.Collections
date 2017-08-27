using System;

namespace CSharpTest.Net.Collections
{
    internal static class Resources
    {
        internal static string IOStreamFailedToRead => "Failed to read from input stream.";

        internal static string FailedToConstructSingleton(Type type)
        {
            return string.Format("The singleton for type {0} threw an excpetion.", type);
        }

        internal static string AssertionFailedException(string message = null)
        {
            if (message == null)
                return "A runtime assertion failed while performing the operation.";

            return string.Format("A runtime assertion failed: {0}", message);
        }

        internal static string DuplicateKeyException => "The specified key already exists in the collection.";

        internal static string InvalidConfigurationValueException(string property = null, string message = null)
        {
            if (message == null)
                return string.Format("The configuration value '{0}' is invalid.", property);

            return string.Format("The configuration value '{0}' is invalid: {1}", property, message);
        }

        internal static string InvalidNodeHandleException => "A storage handle was invalid or has been corrupted.";

        internal static string LurchTableCorruptionException => "The LurchTable internal datastructure appears to be corrupted.";
    }
}