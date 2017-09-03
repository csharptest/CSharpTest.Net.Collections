using System.IO;

namespace CSharpTest.Net.Collections.Compatibility
{
    public static class StreamExtensions
    {
        public static byte[] GetBuffer(this MemoryStream stream)
        {
            stream.TryGetBuffer(out var array);
            return array.Array;
        }
    }
}
