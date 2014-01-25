#region Copyright 2011-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System.IO;
using CSharpTest.Net.IO;

namespace CSharpTest.Net.Serialization
{
    /// <summary> Provides serialization for a type </summary>
    public interface ISerializer<T>
    {
        /// <summary> Writes the object to the stream </summary>
        void WriteTo(T value, Stream stream);
        /// <summary> Reads the object from a stream </summary>
        T ReadFrom(Stream stream);
    }

    /// <summary>
    /// Returns all bytes in the stream, or writes all bytes to the stream
    /// </summary>
    public class BytesSerializer : ISerializer<byte[]>
    {
        /// <summary> Gets a singleton of the BytesSerializer class </summary>
        public static readonly ISerializer<byte[]> RawBytes = new BytesSerializer();

        void ISerializer<byte[]>.WriteTo(byte[] value, Stream stream)
        { stream.Write(value, 0, value.Length); }

        byte[] ISerializer<byte[]>.ReadFrom(Stream stream)
        { return IOStream.ReadAllBytes(stream); }
    }

#if !NET20

    /// <summary>
    /// Extension methods used with ISerializer&lt;T> for byte[] transformations
    /// </summary>
    public static class SerializerExtensions
    {
        /// <summary>
        /// Enables creation of the type TKey from an array of bytes
        /// </summary>
        public static TKey FromByteArray<TKey>(this ISerializer<TKey> ser, byte[] value)
        {
            using (MemoryStream ms = new MemoryStream(value))
                return ser.ReadFrom(ms);
        }

        /// <summary>
        /// Enables creation of an array of bytes from type TKey
        /// </summary>
        public static byte[] ToByteArray<TKey>(this ISerializer<TKey> ser, TKey value)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                ser.WriteTo(value, ms);
                return ms.ToArray();
            }
        }
    }

#endif
}
