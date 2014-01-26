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

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Provides a base-class that aggregates another stream object
    /// </summary>
    public abstract class AggregateStream : Stream
    {
        Stream _stream;

        /// <summary> Creates the wrapper without an underlying stream </summary>
        protected AggregateStream() { _stream = null; }
        /// <summary> Creates the wrapper with the underlying stream </summary>
        protected AggregateStream(Stream io) { _stream = io; }

        /// <summary> Disposes of this.Stream </summary>
        protected override void Dispose(bool disposing)
        {
            if (disposing && _stream != null)
                _stream.Dispose();
            _stream = null;

            base.Dispose(disposing);
        }

        /// <summary> Disposes of this.Stream </summary>
        public override void Close()
        {
            if (_stream != null)
                _stream.Close();

            base.Close();
        }

        /// <summary> Allows modifying the underlying stream </summary>
        protected virtual Stream Stream { get { return _stream ?? Stream.Null; } set { _stream = value; } }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports reading.
        /// </summary>
        public override bool CanRead { get { return Stream.CanRead; } }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports seeking.
        /// </summary>
        public override bool CanSeek { get { return Stream.CanSeek; } }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports writing.
        /// </summary>
        public override bool CanWrite { get { return Stream.CanWrite; } }

        /// <summary>
        /// When overridden in a derived class, gets the length in bytes of the stream.
        /// </summary>
        public override long Length { get { return Stream.Length; } }

        /// <summary>
        /// When overridden in a derived class, gets or sets the position within the current stream.
        /// </summary>
        public override long Position
        {
            get { return Stream.Position; }
            set { Stream.Position = value; }
        }

        /// <summary>
        /// When overridden in a derived class, clears all buffers for this stream and causes any buffered data to be written to the underlying device.
        /// </summary>
        public override void Flush()
        {
            Stream.Flush();
        }

        /// <summary>
        /// When overridden in a derived class, reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count)
        {
            return Stream.Read(buffer, offset, count);
        }

        /// <summary>
        /// Reads a byte from the stream and advances the position within the stream by one byte, or returns -1 if at the end of the stream.
        /// </summary>
        public override int ReadByte()
        {
            return Stream.ReadByte();
        }

        /// <summary>
        /// When overridden in a derived class, sets the position within the current stream.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            return Stream.Seek(offset, origin);
        }

        /// <summary>
        /// When overridden in a derived class, sets the length of the current stream.
        /// </summary>
        public override void SetLength(long value)
        {
            Stream.SetLength(value);
        }

        /// <summary>
        /// When overridden in a derived class, writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count)
        {
            Stream.Write(buffer, offset, count);
        }

        /// <summary>
        /// Writes a byte to the current position in the stream and advances the position within the stream by one byte.
        /// </summary>
        public override void WriteByte(byte value)
        {
            Stream.WriteByte(value);
        }
    }
}
