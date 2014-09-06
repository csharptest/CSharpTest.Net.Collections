#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.IO.Compression;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// A collection of Stream helpers
    /// </summary>
    public static class IOStream
    {
        /// <summary> Reads all of the bytes from the input stream, input stream will be disposed </summary>
        public static byte[] ReadAllBytes(Stream io)
        {
            using (io)
            using (MemoryStream ms = new MemoryStream())
            {
                CopyStream(io, ms);
                return ms.ToArray();
            }
        }
        /// <summary> Reads all of the bytes from the input stream, input stream will be disposed </summary>
        public static string ReadAllText(Stream io, Encoding encoding)
        {
            using (io)
                return encoding.GetString(ReadAllBytes(io));
        }
        /// <summary> Reads a the number of bytes specified or throws IOException </summary>
        public static void Read(Stream io, byte[] bytes) 
        { Read(io, bytes, bytes.Length); }
        /// <summary> Reads a the number of bytes specified or throws IOException </summary>
        public static byte[] Read(Stream io, int nBytes) 
        { 
            byte[] bytes = new byte[nBytes]; 
            Read(io, bytes, nBytes); 
            return bytes;
        }
        /// <summary> Reads a the number of bytes specified or throws IOException </summary>
        public static void Read(Stream io, byte[] bytes, int length)
        {
            if (length != ReadChunk(io, bytes, length))
                throw new IOException(Resources.IOStreamFailedToRead);
        }

        /// <summary> Attempts to read the number of bytes specified and returns the actual count </summary>
        public static int ReadChunk(Stream io, byte[] bytes, int length)
        { return ReadChunk(io, bytes, 0, length); }

        /// <summary> Attempts to read the number of bytes specified and returns the actual count </summary>
        public static int ReadChunk(Stream io, byte[] bytes, int offset, int length)
        {
            int bytesRead = 0;
            int len = 0;
            while (length > bytesRead && 0 != (len = io.Read(bytes, bytesRead, length - bytesRead)))
                bytesRead += len;
            return bytesRead;
        }

        /// <summary> Copy the entire input stream to the provided output stream, input stream will be disposed </summary>
        /// <returns> The number of bytes copied </returns>
        public static long CopyStream(Stream input, Stream output) 
        {
            using (input)
                return CopyStream(input, output, long.MaxValue);
        }
        /// <summary> Copy the specified number of bytes from the input stream to the provided output stream </summary>
        /// <returns> The number of bytes copied </returns>
        public static long CopyStream(Stream input, Stream output, long stopAfter)
        {
            byte[] bytes = new byte[ushort.MaxValue];
            long bytesRead = 0;
            int len = 0;
            while (0 != (len = input.Read(bytes, 0, Math.Min(bytes.Length, (int)Math.Min(int.MaxValue, stopAfter - bytesRead)))))
            {
                output.Write(bytes, 0, len);
                bytesRead = bytesRead + len;
            }
            output.Flush();
            return bytesRead;
        }
    }
}
