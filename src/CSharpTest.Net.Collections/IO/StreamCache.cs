#region Copyright 2011-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Threading;
using CSharpTest.Net.Bases;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Provides a simple means of caching several streams on a single file and for a thread 
    /// to  quickly exclusive access to one of those streams.  This class provides the base
    /// implementation used by FileStreamCache and FragmentedFile.
    /// </summary>
    public class StreamCache : Disposable, IFactory<Stream>
    {
        readonly IFactory<Stream> _streamFactory;
        readonly Stream[] _streams;
        readonly Mutex[] _handles;

        /// <summary>
        /// Constructs the stream cache allowing one stream per thread
        /// </summary>
        public StreamCache(IFactory<Stream> streamFactory)
            : this(streamFactory, Environment.ProcessorCount)
        { }

        /// <summary>
        /// Constructs the stream cache with the maximum allowed stream items
        /// </summary>
        public StreamCache(IFactory<Stream> streamFactory, int maxItem)
        {
            _streamFactory = streamFactory;
            _streams = new Stream[maxItem];
            _handles = new Mutex[maxItem];

            for (int i = 0; i < maxItem; i++)
                _handles[i] = new Mutex();
        }

        /// <summary></summary>
        protected override void Dispose(bool disposing)
        {
            for (int i = 0; i < _handles.Length; i++)
                _handles[i].Close();
            for (int i = 0; i < _streams.Length; i++)
                if (_streams[i] != null)
                    _streams[i].Dispose();
        }


        private void InvalidHandle(Mutex ownerHandle)
        {
            for (int i = 0; i < _handles.Length; i++)
            {
                if(ReferenceEquals(_handles[i], ownerHandle))
                    _handles[i] = new Mutex();
            }
        }
        
        /// <summary>
        /// Waits for a stream to become available and returns a wrapper on that stream. Just dispose like
        /// any other stream to return the resource to the stream pool.
        /// </summary>
        public Stream Open() { return Open(FileAccess.ReadWrite); }
        
        Stream IFactory<Stream>.Create() { return Open(FileAccess.ReadWrite); }

        /// <summary>
        /// Waits for a stream to become available and returns a wrapper on that stream. Just dispose like
        /// any other stream to return the resource to the stream pool.
        /// </summary>
        public Stream Open(FileAccess access)
        {
            int handle;

            try { handle = WaitHandle.WaitAny(_handles); }
            catch (AbandonedMutexException ae)
            { handle = ae.MutexIndex; }

            Stream stream = _streams[handle];
            if (stream == null || !stream.CanRead)
            {
                try { } 
                finally 
                {
                    _streams[handle] = stream = _streamFactory.Create(); 
                }
            }

            if (stream.CanSeek)
                stream.Seek(0, SeekOrigin.Begin);

            return new CachedStream(this, stream, access, _handles[handle]);
        }

        private class CachedStream : AggregateStream
        {
            private const FileAccess NoAccess = 0;
            private readonly StreamCache _cache;
            private readonly Mutex _ownerHandle;
            private FileAccess _fileAccess;

            public CachedStream(StreamCache cache, Stream rawStream, FileAccess access, Mutex ownerHandle) : base(rawStream)
            {
                _cache = cache;
                _ownerHandle = ownerHandle;
                _fileAccess = access;
            }

            ~CachedStream() { GC.SuppressFinalize(this); Dispose(false); }

            public override void Close() { Dispose(true); }
            protected override void Dispose(bool disposing)
            {
                if (_fileAccess != NoAccess)
                {
                    if (disposing && Stream.CanWrite)
                        Stream.Flush();
                    _fileAccess = NoAccess;

                    if (disposing)
                        try { _ownerHandle.ReleaseMutex(); } catch (ObjectDisposedException) { }
                    else
                        _cache.InvalidHandle(_ownerHandle);

                    base.Stream = null;
                }
            }

            private void IsNotDisposed()
            {
                if (_fileAccess == NoAccess)
                    throw new ObjectDisposedException(GetType().FullName);
            }

            public override bool CanRead { get { return (_fileAccess & FileAccess.Read) == FileAccess.Read && Stream.CanRead; } }
            public override bool CanSeek { get { return _fileAccess != NoAccess && Stream.CanSeek; } }
            public override bool CanWrite { get { return (_fileAccess & FileAccess.Write) == FileAccess.Write && Stream.CanWrite; } }

            public override void SetLength(long value)
            {
                IsNotDisposed();
                Check.Assert<InvalidOperationException>(CanWrite);
                base.SetLength(value);
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                IsNotDisposed();
                Check.Assert<InvalidOperationException>(CanRead);
                return base.Read(buffer, offset, count);
            }

            public override int ReadByte()
            {
                IsNotDisposed();
                Check.Assert<InvalidOperationException>(CanRead);
                return base.ReadByte();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                IsNotDisposed();
                Check.Assert<InvalidOperationException>(CanWrite);
                base.Write(buffer, offset, count);
            }

            public override void WriteByte(byte value)
            {
                IsNotDisposed();
                Check.Assert<InvalidOperationException>(CanWrite);
                base.WriteByte(value);
            }
        }
    }
}
