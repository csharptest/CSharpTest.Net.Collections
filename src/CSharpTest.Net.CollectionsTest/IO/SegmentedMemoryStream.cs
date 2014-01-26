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
using System.IO;

namespace CSharpTest.Net.IO
{
    /// <summary>
	/// Creates a stream over an array of byte arrays in memory to reduce use of the LOH and array resizing operation.
	/// </summary>
	public class SegmentedMemoryStream : Stream
    {
        #region Contents
        class Contents
        {
            readonly int _segmentSize;
            byte[][] _segments;
            int _segmentCount;
            long _length;

            public Contents(int segmentSize)
            {
                _segmentSize = segmentSize;
                _segments = new byte[16][];
            }

            public int SegmentSize { get { return _segmentSize; } }
            public long Length { get { return _length; } set { _length = value; } }

            public byte[] this[int index] { get { return _segments[index]; } }

            public void GrowTo(int newSegmentCount)
            {
                lock (this)
                {
                    while (_segments.Length < newSegmentCount)
                        Array.Resize(ref _segments, _segments.Length << 1);
                    while (_segmentCount < newSegmentCount)
                    {
                        _segments[_segmentCount] = new byte[_segmentSize];
                        _segmentCount++;
                    }
                }
            }
        }
        #endregion
        readonly Contents _contents;
		long _position;

		/// <summary>
		/// Creates a memory stream that uses 32k segments for storage
		/// </summary>
		public SegmentedMemoryStream()
			: this(short.MaxValue)
		{ }
		/// <summary>
		/// Create a memory stream that uses the specified size of segments
		/// </summary>
		public SegmentedMemoryStream(int segmentSize)
		{
			_contents = new Contents(segmentSize);
			_position = 0;
		}
        /// <summary> Creates a 'clone' of the stream sharing the same contents </summary>
        protected SegmentedMemoryStream(SegmentedMemoryStream from)
        {
            _contents = from._contents;
            _position = 0;
        }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports reading.
        /// </summary>
        public override bool CanRead { get { return _position >= 0; } }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports seeking.
        /// </summary>
        public override bool CanSeek { get { return _position >= 0; } }

        /// <summary>
        /// When overridden in a derived class, gets a value indicating whether the current stream supports writing.
        /// </summary>
        public override bool CanWrite { get { return _position >= 0; } }

        /// <summary>
        /// When overridden in a derived class, clears all buffers for this stream and causes any buffered data to be written to the underlying device.
        /// </summary>
        public override void Flush()
		{ }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="T:System.IO.Stream"/> and optionally releases the managed resources.
        /// </summary>
        protected override void Dispose(bool disposing)
		{
			_position = -1;
			base.Dispose(disposing);
		}

        /// <summary>
        /// When overridden in a derived class, gets the length in bytes of the stream.
        /// </summary>
        public override long Length
		{
			get { AssertOpen(); return _contents.Length; }
		}

		private void AssertOpen()
		{
            if (_position < 0) throw new ObjectDisposedException(GetType().FullName);
		}

		private void OffsetToIndex(long offset, out int arrayIx, out int arrayOffset)
		{
			arrayIx = (int)(offset / _contents.SegmentSize);
            arrayOffset = (int)(offset % _contents.SegmentSize);
		}

        /// <summary>
        /// When overridden in a derived class, sets the length of the current stream.
        /// </summary>
        public override void SetLength(long value)
		{
			AssertOpen();
			Check.InRange(value, 0L, int.MaxValue);

			int arrayIx, arrayOffset;
			OffsetToIndex(value, out arrayIx, out arrayOffset);

            int chunksRequired = arrayIx + (arrayOffset > 0 ? 1 : 0);
            _contents.GrowTo(chunksRequired);
            _contents.Length = value;
		}

        /// <summary>
        /// When overridden in a derived class, gets or sets the position within the current stream.
        /// </summary>
        public override long Position
		{
			get { return _position; }
			set
			{
				AssertOpen();
				Check.InRange(value, 0L, int.MaxValue);
				if (value > _contents.Length)
					SetLength(value);
				_position = value;
			}
		}

        /// <summary>
        /// When overridden in a derived class, sets the position within the current stream.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
		{
			if (origin == SeekOrigin.End)
                offset = _contents.Length + offset;
			if (origin == SeekOrigin.Current)
				offset = _position + offset;
			return Position = offset;
		}

        /// <summary>
        /// When overridden in a derived class, reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count)
		{
			AssertOpen();
			int total = 0;
            if ((_contents.Length - _position) < count)
                count = (int)(_contents.Length - _position);

			while (count > 0)
			{
                int arrayIx, arrayOffset;
				OffsetToIndex(_position, out arrayIx, out arrayOffset);
                int amt = Math.Min(_contents.SegmentSize - arrayOffset, count);

				byte[] chunk = _contents[arrayIx];
				Array.Copy(chunk, arrayOffset, buffer, offset, amt);
				count -= amt;
				offset += amt;
				total += amt;
				_position += amt;
			}
			return total;
		}

        /// <summary>
        /// When overridden in a derived class, writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count)
		{
			AssertOpen();
            if ((_position + count) > _contents.Length)
				SetLength(_position + count);

			while (count > 0)
			{
			    int arrayIx, arrayOffset;
				OffsetToIndex(_position, out arrayIx, out arrayOffset);
                int amt = Math.Min(_contents.SegmentSize - arrayOffset, count);

				byte[] chunk = _contents[arrayIx];
				Array.Copy(buffer, offset, chunk, arrayOffset, amt);
				count -= amt;
				offset += amt;
				_position += amt;
			}
		}

        /// <summary>
        /// Reads a byte from the stream and advances the position within the stream by one byte, or returns -1 if at the end of the stream.
        /// </summary>
        public override int ReadByte()
        {
            byte[] bytes = new byte[1];
            return Read(bytes, 0, 1) == 1 ? bytes[0] : -1;
        }

        /// <summary>
        /// Writes a byte to the current position in the stream and advances the position within the stream by one byte.
        /// </summary>
        public override void WriteByte(byte value)
        {
            Write(new byte[] { value }, 0, 1);
        }
	}
}