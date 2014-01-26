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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Provides a means of storing multitudes of small files inside one big one.  I doubt this is a
    /// good name for it, but it works.  Anyway, the file is broken into fixed size blocks and each
    /// block can be chained to another to allow the sub-allocation to grow larger than the block size.
    /// This is the primary storage vehicle for the BPlusTree implementation.
    /// </summary>
    public class FragmentedFile : IDisposable
    {
        private const FileOptions NoBuffering = (FileOptions)0x20000000;
        /// <summary> Common operational values for 'normal' files </summary>
        public const FileOptions OptionsDefault = FileOptions.RandomAccess;
        /// <summary> Common operational values for using OS cache write-through (SLOW) </summary>
        public const FileOptions OptionsWriteThrough = FileOptions.RandomAccess | FileOptions.WriteThrough;
        /// <summary> Uses FILE_FLAG_NO_BUFFERING see http://msdn.microsoft.com/en-us/library/cc644950(v=vs.85).aspx (SLOWEST) </summary>
        public const FileOptions OptionsNoBuffering = OptionsWriteThrough | NoBuffering;

        private readonly StreamCache _streamCache;
        private readonly bool _useAlignedIo;
        private readonly long _maskVersion, _maskOffset;
        private readonly int _blockSize;
        private readonly int _reallocSize;
        private readonly object _syncFreeBlock;
        private readonly FileBlock _header;

        private bool _disposed;
        private long _nextFree;

        /// <summary>
        /// Opens an existing fragmented file store, to create a new one call the CreateNew() static
        /// </summary>
        /// <param name="filename">The file name that will store the data</param>
        /// <param name="blockSize">The block size that was specified when CreateNew() was called</param>
        public FragmentedFile(string filename, int blockSize)
            : this(filename, blockSize, 100, Environment.ProcessorCount, OptionsWriteThrough)
        { }

        /// <summary>
        /// Opens an existing fragmented file store, to create a new one call the CreateNew() static
        /// </summary>
        /// <param name="filename">The file name that will store the data</param>
        /// <param name="blockSize">The block size that was specified when CreateNew() was called</param>
        /// <param name="growthRate">The number of blocks to grow the file by when needed, or zero for on-demand </param>
        /// <param name="cacheLimit">The number of threads that can simultaneously access the file</param>
        /// <param name="options">The file options to use when opening the file</param>
        public FragmentedFile(string filename, int blockSize, int growthRate, int cacheLimit, FileOptions options)
            : this(new FileStreamFactory(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 8, options),
                    blockSize, growthRate, cacheLimit, options)
        { }

        /// <summary>
        /// Opens an existing fragmented file store, to create a new one call the CreateNew() static
        /// </summary>
        /// <param name="filename">The file name that will store the data</param>
        /// <param name="blockSize">The block size on disk to be used for allocations</param>
        /// <param name="growthRate">The number of blocks to grow the file by when needed, or zero for on-demand </param>
        /// <param name="cacheLimit">The number of threads that can simultaneously access the file</param>
        /// <param name="access">The file access requested</param>
        /// <param name="share">The file share permissions</param>
        /// <param name="options">The file options to use when opening the file</param>
        public FragmentedFile(string filename, int blockSize, int growthRate, int cacheLimit, FileAccess access, FileShare share, FileOptions options)
            : this(new FileStreamFactory(filename, FileMode.Open, access, share, 8, options),
                    blockSize, growthRate, cacheLimit, options)
        { }

        /// <summary>
        /// Opens an existing fragmented file store, to create a new one call the CreateNew() static
        /// </summary>
        /// <param name="streamFactory">An IFactory that produces streams for a storage</param>
        /// <param name="blockSize">The block size to be used for allocations</param>
        /// <param name="growthRate">The number of blocks to grow the file by when needed, or zero for on-demand </param>
        /// <param name="cacheLimit">The number of threads that can simultaneously access the file</param>
        public FragmentedFile(IFactory<Stream> streamFactory, int blockSize, int growthRate, int cacheLimit)
            : this(streamFactory, blockSize, growthRate, cacheLimit, FileOptions.None)
        { }

        /// <summary> Internal use to specify aligned IO when using NoBuffering file option </summary>
        protected FragmentedFile(IFactory<Stream> streamFactory, int blockSize, int growthRate, int cacheLimit, FileOptions options)
        {
            _useAlignedIo = (options & NoBuffering) == NoBuffering;
            _streamCache = new StreamCache(streamFactory, cacheLimit);
            _header = new FileBlock(blockSize, _useAlignedIo);
            _syncFreeBlock = new object();
            try
            {
                long fallocated;
                bool canWrite;

                using (Stream s = _streamCache.Open(FileAccess.ReadWrite))
                {
                    canWrite = s.CanWrite;
                    if (!s.CanRead)
                        throw new InvalidOperationException("The stream does not support Read access.");

                    _header.Read(s, blockSize);

                    if ((_header.Flags & ~BlockFlags.HeaderFilter) != BlockFlags.HeaderFlags)
                        throw new InvalidDataException();

                    _nextFree = _header.NextBlockId;
                    SetBlockSize(_header.Length, out _blockSize, out _maskVersion, out _maskOffset);
                    if (blockSize != _blockSize) throw new ArgumentOutOfRangeException("blockSize");
                    fallocated = LastAllocated(s);
                    _reallocSize = growthRate * _blockSize;

                    if (canWrite)
                    {
                        s.Position = 0;
                        _header.NextBlockId = long.MinValue;
                        _header.Write(s, FileBlock.HeaderSize);
                    }
                }

                if (canWrite)
                {
                    if ((_header.Flags & BlockFlags.ResizingFile) == BlockFlags.ResizingFile && _nextFree > 0)
                        ResizeFile(_nextFree, Math.Max(fallocated, _nextFree + _reallocSize));

                    if (_nextFree == long.MinValue)
                        _nextFree = RecoverFreeBlocks();
                }
            }
            catch
            {
                _streamCache.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Closes the storage, a must-do to save a costly recomputation of free block on open
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                try
                {
                    using (Stream s = _streamCache.Open(FileAccess.ReadWrite))
                    {
                        if (s.CanWrite)
                        {
                            _header.Read(s, FileBlock.HeaderSize);

                            if (_header.Flags != BlockFlags.HeaderFlags)
                                throw new InvalidDataException();

                            s.Position = 0;
                            _header.NextBlockId = _nextFree;
                            _header.Write(s, FileBlock.HeaderSize);
                        }
                    }

                    _header.Dispose();
                    _streamCache.Dispose();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        /// <summary>
        /// Creates a new file (or truncates an existing one) that stores multiple smaller files
        /// </summary>
        public static FragmentedFile CreateNew(string filename, int blockSize)
        { return CreateNew(filename, blockSize, 100, Environment.ProcessorCount, OptionsWriteThrough); }

        /// <summary>
        /// Creates a new file (or truncates an existing one) that stores multiple smaller files
        /// </summary>
        public static FragmentedFile CreateNew(string filename, int blockSize, int growthRate, int cacheLimit, FileOptions options)
        {
            WriteEmtpy(new FileStreamFactory(filename, FileMode.Create, FileAccess.Write, FileShare.None), blockSize);
            return new FragmentedFile(filename, blockSize, growthRate, cacheLimit, options);
        }
        /// <summary>
        /// Creates a new file (or truncates an existing one) that stores multiple smaller files
        /// </summary>
        public static FragmentedFile CreateNew(IFactory<Stream> streamFactory, int blockSize, int growthRate, int cacheLimit)
        {
            WriteEmtpy(streamFactory, blockSize);
            return new FragmentedFile(streamFactory, blockSize, growthRate, cacheLimit);
        }
        
        /// <summary>
        /// Creates a new file (or truncates an existing one) that stores multiple smaller files
        /// </summary>
        private static void WriteEmtpy(IFactory<Stream> streamFactory, int blockSize)
        {
            long mask;
            SetBlockSize(blockSize, out blockSize, out mask, out mask);
            using (FileBlock block = new FileBlock(Check.InRange(blockSize, 512, /*65,536*/0x10000), false))
            {
                block.Length = blockSize;
                block.Flags = BlockFlags.HeaderFlags;

                using (Stream f = streamFactory.Create())
                {
                    f.Position = 0;
                    block.Write(f, blockSize);
                    f.SetLength(f.Position);
                }
            }
        }

        /// <summary> Destroys all contents of the file and resets to an initial state </summary>
        public void Clear()
        {
            using (Stream f = _streamCache.Open(FileAccess.ReadWrite))
            {
                f.Position = 0;
                f.SetLength(0);
                _header.NextBlockId = 0;
                _header.Write(f, _blockSize);
            }
            _nextFree = 0;
        }

        /// <summary> Creates a new allocation block within the file </summary>
        /// <returns> A unique integer id for the block to be used with Open/Delete </returns>
        public long Create()
        {
            using (FileBlock block = new FileBlock(_blockSize, _useAlignedIo))
            {
                long identity = AllocBlock(block, BlockFlags.ExternalBlock);
                return identity;
            }
        }

        /// <summary> Creates a new allocation block within the file </summary>
        /// <param name="identity">A unique integer id for the block to be used with Open/Delete</param>
        /// <returns>The stream to write to the newly created block</returns>
        public Stream Create(out long identity)
        {
            using (FileBlock block = new FileBlock(_blockSize, _useAlignedIo))
            {
                identity = AllocBlock(block, BlockFlags.ExternalBlock);
                return new BlockStreamWriter(this, block);
            }
        }

        /// <summary>
        /// Opens the file with the identity previously obtained by Create() using the 
        /// access provided; however, Read+Write is not supported, use either Read or
        /// Write but not both.
        /// </summary>
        public Stream Open(long identity, FileAccess access)
        {
            if (identity < FirstIdentity)
                throw new ArgumentOutOfRangeException("identity");

            if (access == FileAccess.Read)
                return new BlockStreamReader(this, identity);
            
            if (access == FileAccess.Write)
                return new BlockStreamWriter(this, identity);
            
            throw new ArgumentOutOfRangeException("access");
        }

        /// <summary>
        /// Deletes the contents written to the identity provided and returns the
        /// identity to the available pool.
        /// </summary>
        public void Delete(long identity)
        {
            if (identity < FirstIdentity)
                throw new ArgumentOutOfRangeException("identity");

            if(!FreeBlock(identity, BlockFlags.ExternalBlock))
                throw new ArgumentOutOfRangeException("identity");
        }

        private Stream OpenBlock(FileAccess access, long identity)
        {
            Stream stream = _streamCache.Open(access);
            try
            {
                if ((identity & _maskOffset) > LastAllocated(stream))
                    throw new ArgumentOutOfRangeException("identity");

                long offset = (identity & _maskOffset);
                stream.Position = offset;
                Check.IsEqual(offset, stream.Position);
                return stream;
            }
            catch
            {
                if (stream != null)
                    stream.Dispose();
                throw;
            }
        }

        /// <summary> Returns the 'first' block identity that can be allocated </summary>
        public long FirstIdentity { get { return _blockSize | 1; } }
        private long LastAllocated(Stream s) { return ((s.Length - 1) / _blockSize) * _blockSize; }

        private static void SetBlockSize(int value, out int blockSize, out long maskVersion, out long maskOffset)
        {
            maskVersion = 0;
            int ix = 0;
            while (true)
            {
                long bit = 1 << ix;
                if (value == bit)
                    break;
                maskVersion |= bit;
                if (++ix >= 32)
                    throw new ArgumentOutOfRangeException("blockSize", "The block size must be a power of 2.");
            }

            maskOffset = ~maskVersion;
            blockSize = value;
        }

        private long RecoverFreeBlocks()
        {
            Trace.TraceWarning("The file store was not closed properly, recovering free blocks.");

            using (Stream s = _streamCache.Open(FileAccess.ReadWrite))
            using (FileBlock block = new FileBlock(_blockSize, _useAlignedIo))
            {
                long lastBlock = LastAllocated(s);
                long last = 0;
                for (long blk = lastBlock; blk > 0; blk -= _blockSize)
                {
                    s.Position = blk & _maskOffset;
                    block.Read(s, FileBlock.HeaderSize);
                    Check.Assert<InvalidDataException>((block.BlockId & _maskOffset) == blk);
                    if ((block.Flags & BlockFlags.BlockDeleted) == BlockFlags.BlockDeleted)
                    {
                        block.NextBlockId = last;
                        last = block.BlockId;
                        s.Position = blk & _maskOffset;
                        block.Write(s, FileBlock.HeaderSize);
                    }
                }
                return last;
            }
        }

        /// <summary> Used for enumeration of the storage blocks in the file. </summary>
        /// <param name="allocatedOnly"> Allows enumeration of all stream, or of just the externally allocated streams </param>
        /// <param name="verifyReads"> Determines if the checksum should be verified while reading the block bytes </param>
        /// <param name="ignoreException"> A method that returns true to ignore the exception and continue processing </param>
        /// <returns>Enumeration of the identity and data stream of each block in the file</returns>
        public IEnumerable<KeyValuePair<long, Stream>> ForeachBlock(bool allocatedOnly, bool verifyReads, Converter<Exception, bool> ignoreException)
        {
            using (Stream s = _streamCache.Open(FileAccess.ReadWrite))
            using (FileBlock block = new FileBlock(_blockSize, _useAlignedIo))
            {
                long lastBlock = LastAllocated(s);
                for (long blk = lastBlock; blk > 0; blk -= _blockSize)
                {
                    s.Position = blk & _maskOffset;
                    block.Read(s, FileBlock.HeaderSize);

                    byte[] bytes;
                    try
                    {
                        if ((block.BlockId & _maskOffset) != blk)
                            throw new InvalidDataException();
                        if (allocatedOnly && (block.Flags & BlockFlags.ExternalBlock) != BlockFlags.ExternalBlock)
                            continue;

                        using (Stream reader = new BlockStreamReader(this, block.BlockId, BlockFlags.None, verifyReads))
                            bytes = IOStream.ReadAllBytes(reader);
                    }
                    catch (Exception error)
                    {
                        if (ignoreException != null && ignoreException(error))
                            continue;
                        throw;
                    }

                    using (Stream ms = new MemoryStream(bytes, false))
                        yield return new KeyValuePair<long, Stream>(block.BlockId, ms);
                }
            }
        }

        private bool FreeBlock(long blockId, BlockFlags expected)
        {
            using (FileBlock first = new FileBlock(_blockSize, _useAlignedIo))
            {
                ReadBlock(blockId, first, FileBlock.HeaderSize, expected | BlockFlags.BlockDeleted);

                if ((first.Flags & expected) != expected)
                    return false;

                using (FileBlock last = first.Clone())
                {
                    while (last.NextBlockId != 0)
                    {
                        last.Flags = BlockFlags.BlockDeleted;

                        WriteBlock(last.BlockId, last, FileBlock.HeaderSize);
                        ReadBlock(last.NextBlockId, last, FileBlock.HeaderSize, BlockFlags.InternalBlock);
                    }

                    using (new SafeLock(_syncFreeBlock))
                    {
                        last.Flags = BlockFlags.BlockDeleted;
                        last.NextBlockId = _nextFree;
                        WriteBlock(last.BlockId, last, FileBlock.HeaderSize);
                        _nextFree = first.BlockId;
                    }
                    return true;
                }
            }
        }

        private void ResizeFile(long startBlock, long endBlock)
        {
            if ((startBlock & _maskVersion) != 0 || (endBlock & _maskVersion) != 0)
                throw new InvalidDataException();

            using (FileBlock block = new FileBlock(_blockSize, _useAlignedIo))
            using (Stream io = _streamCache.Open(FileAccess.Write))
            {
                _header.Flags |= BlockFlags.ResizingFile;
                _header.NextBlockId = startBlock;
                io.Position = 0;
                _header.Write(io, FileBlock.HeaderSize);
                try
                {
                    block.Clear();
                    block.Flags = BlockFlags.BlockDeleted;
                    _nextFree = 0;

                    for (long ix = endBlock; ix >= startBlock; ix -= _blockSize)
                    {
                        block.BlockId = ix;
                        block.NextBlockId = _nextFree;
                        _nextFree = block.BlockId;

                        io.Position = ix & _maskOffset;
                        block.Write(io, FileBlock.HeaderSize);
                    }
                }
                finally
                {
                    _header.Flags &= ~BlockFlags.ResizingFile;
                    _header.NextBlockId = long.MinValue;
                    io.Position = 0;
                    _header.Write(io, FileBlock.HeaderSize);
                }
            }
        }

        private long AllocBlock(FileBlock block, BlockFlags type)
        {
            using (new SafeLock(_syncFreeBlock))
            {
                long blockId = _nextFree;
                if (blockId == 0 && _reallocSize > 0)
                {
                    long fsize;
                    using (Stream s = _streamCache.Open(FileAccess.Read))
                        fsize = LastAllocated(s);
                    ResizeFile(fsize + _blockSize, fsize + _reallocSize);
                    blockId = _nextFree;
                }

                if (blockId <= 0)
                    throw new IOException();

                using (Stream io = OpenBlock(FileAccess.Read, blockId))
                    block.Read(io, FileBlock.HeaderSize);

                if ((block.BlockId & _maskOffset) != (blockId & _maskOffset) || (block.Flags & BlockFlags.BlockDeleted) == 0)
                    throw new InvalidDataException();

                _nextFree = block.NextBlockId;
            
                block.BlockId = blockId;
                block.IncrementId(_maskVersion);
                block.NextBlockId = 0;
                block.Flags = type == BlockFlags.ExternalBlock ? (type | BlockFlags.Temporary) : type;
                block.Length = 0;
                WriteBlock(block.BlockId, block, FileBlock.HeaderSize); 
                return block.BlockId;
            }
        }

        private void ReadBlock(long ordinal, FileBlock block, int length, BlockFlags type)
        { ReadBlock(ordinal, block, length, type, true); }
        private void ReadBlock(long ordinal, FileBlock block, int length, BlockFlags type, bool exactId)
        {
            using (Stream io = OpenBlock(FileAccess.Read, ordinal))
                block.Read(io, length);

            if (exactId && block.BlockId != ordinal)
                throw new InvalidDataException();

            if ((block.Flags & type) == 0 && type != 0)
                throw new InvalidDataException();

            if (block.Length < 0 || block.Length > (_blockSize - FileBlock.HeaderSize))
                throw new InvalidDataException();
        }

        private void WriteBlock(long ordinal, FileBlock block, int length)
        {
            if (block.BlockId != ordinal)
                throw new InvalidDataException();

            if (block.Length < 0 || block.Length > (_blockSize - FileBlock.HeaderSize))
                throw new InvalidDataException();

            try { } 
            finally 
            {
                using (Stream io = OpenBlock(FileAccess.Write, ordinal))
                    block.Write(io, length);
            }
        }

        #region BlockStreamReader
        class BlockStreamReader : Stream
        {
            readonly FragmentedFile _file;
            readonly FileBlock _block;
            readonly int _expectedSum;
            readonly bool _validated;
            int _blockPos;
            bool _disposed;
            Crc32 _checksum;

            public BlockStreamReader(FragmentedFile file, long ordinal)
                : this(file, ordinal, BlockFlags.ExternalBlock, true)
            { }

            public BlockStreamReader(FragmentedFile file, long ordinal, BlockFlags typeExpected, bool validated)
            {
                _file = file;
                _blockPos = 0;
                _validated = validated;
                _block = new FileBlock(file._blockSize, file._useAlignedIo);

                _file.ReadBlock(ordinal, _block, file._blockSize, typeExpected, _validated);
                if (_validated)
                {
                    _expectedSum = _block.CheckSum;
                    _checksum = new Crc32();
                    _checksum.Add(_block.BlockData, _block.DataOffset, _block.Length);
                    if (_block.NextBlockId == 0 && _checksum != _expectedSum)
                        throw new InvalidDataException();
                }
            }

            protected override void Dispose(bool disposing)
            {
                _disposed = true;
                _block.Dispose();
                base.Dispose(disposing);
            }

            public override bool CanRead { get { return !_disposed; } }
            public override bool CanSeek { get { return false; } }
            public override bool CanWrite { get { return false; } }

            public override void Flush()
            { }

            public override long Position { get { throw new NotSupportedException(); } set { throw new NotSupportedException(); } }
            public override long Length { get { throw new NotSupportedException(); } }
            public override void SetLength(long value) { throw new NotSupportedException(); }
            public override long Seek(long offset, SeekOrigin origin) { throw new NotSupportedException(); }

            private bool PrepareRead()
            {
                int remains = _block.Length - _blockPos;
                if (remains <= 0 && _block.NextBlockId != 0)
                {
                    _file.ReadBlock(_block.NextBlockId, _block, _file._blockSize, _validated ? BlockFlags.InternalBlock : 0, _validated);
                    remains = _block.Length;
                    _blockPos = 0;

                    if (_validated)
                    {
                        _checksum.Add(_block.BlockData, _block.DataOffset, _block.Length);
                        if (_block.NextBlockId == 0 && _checksum != _expectedSum)
                            throw new InvalidDataException();
                    }
                }

                return (remains > 0);
            }
            public override int Read(byte[] buffer, int offset, int count)
            {
                if (_disposed) throw new ObjectDisposedException(GetType().FullName);
                if (!PrepareRead())
                    return 0;

                int remains = _block.Length - _blockPos;
                int amt = Math.Min(remains, count);
                Array.Copy(_block.BlockData, _blockPos + _block.DataOffset, buffer, offset, amt);
                _blockPos += amt;
                return amt;
            }
            public override int ReadByte()
            {
                if (!PrepareRead())
                    return -1;
                byte response = _block.BlockData[_blockPos + _block.DataOffset];
                _blockPos += 1;
                return response;
            }

            public override void Write(byte[] buffer, int offset, int count)
            { throw new NotSupportedException(); }
        }
        #endregion
        #region BlockStreamWriter
        class BlockStreamWriter : Stream, ITransactable
        {
            readonly FragmentedFile _file;

            readonly FileBlock _first, _restore;
            FileBlock _current, _temp;

            Crc32 _checksum;
            readonly bool _isNew;
            bool _saved, _reverted;

            private BlockStreamWriter(FragmentedFile file)
            {
                _file = file;
                _checksum = new Crc32();
                _saved = _reverted = false;
            }

            public BlockStreamWriter(FragmentedFile file, FileBlock block)
                : this(file)
            {
                _current = _first = block;
                _current.Flags &= ~BlockFlags.Temporary;
                _restore = null;
                _temp = null;
                _isNew = true;
            }

            public BlockStreamWriter(FragmentedFile file, long ordinal)
                : this(file)
            {
                _current = _first = new FileBlock(file._blockSize, file._useAlignedIo);
                _file.ReadBlock(ordinal, _current, file._blockSize, BlockFlags.ExternalBlock);

                if ((_current.Flags & BlockFlags.ExternalBlock) != BlockFlags.ExternalBlock)
                    throw new InvalidOperationException();

                if ((_current.Flags & BlockFlags.Temporary) == BlockFlags.Temporary)
                {
                    _isNew = true;
                    _restore = null;
                    _current.Flags &= ~BlockFlags.Temporary;
                }
                else
                {
                    _isNew = false;
                    _restore = _current.Clone();
                    _current.NextBlockId = 0;
                }

                _current.Length = 0;
            }

            private int FileBlockDataSize { get { return _file._blockSize - FileBlock.HeaderSize; } }

            public override void Close()
            {
                Commit();
                base.Close();
            }

            protected override void Dispose(bool disposing)
            {
                if (!_reverted)
                {
                    if (_restore != null && _restore.NextBlockId != 0)
                    {
                        _file.FreeBlock(_restore.NextBlockId, BlockFlags.InternalBlock);
                        _restore.NextBlockId = 0;
                    }

                    _current.Dispose();
                    _first.Dispose();
                    if (_restore != null) _restore.Dispose();
                    if(_temp != null) _temp.Dispose();
                }
                base.Dispose(disposing);
            }

            public void Commit()
            {
                if (_saved || _reverted) return;
                _saved = true;
                try
                {
                    if(!ReferenceEquals(_current, _first))
                        _file.WriteBlock(_current.BlockId, _current, _current.Length + FileBlock.HeaderSize);

                    _first.CheckSum = _checksum.Value;
                    _file.WriteBlock(_first.BlockId, _first, _first.Length + FileBlock.HeaderSize);
                }
                catch
                {
                    try { Rollback(); } catch (Exception e) { GC.KeepAlive(e); }
                    throw;
                }
            }

            public void Rollback()
            {
                if (_reverted) return;
                _reverted = true;
                try
                {
                    if (_restore != null)
                        _file.WriteBlock(_restore.BlockId, _restore, _restore.Length + FileBlock.HeaderSize);

                    if (_isNew)
                        _file.FreeBlock(_first.BlockId, BlockFlags.ExternalBlock);
                    if (_first.NextBlockId > 0)
                        _file.FreeBlock(_first.NextBlockId, BlockFlags.InternalBlock);
                }
                finally
                {
                    _first.BlockId = -1;
                }
            }

            public override bool CanRead { get { return false; } }
            public override bool CanSeek { get { return false; } }
            public override bool CanWrite { get { return !_saved && !_reverted; } }

            public override void Flush()
            { }

            public override long Position { get { throw new NotSupportedException(); } set { throw new NotSupportedException(); } }
            public override long Length { get { throw new NotSupportedException(); } }
            public override void SetLength(long value) { throw new NotSupportedException(); }
            public override long Seek(long offset, SeekOrigin origin) { throw new NotSupportedException(); }

            public override int Read(byte[] buffer, int offset, int count)
            { throw new NotSupportedException(); }

            private void PrepareWrite()
            {
                if (_saved || _reverted) throw new ObjectDisposedException(GetType().FullName);
                
                if (_current.Length == FileBlockDataSize)//full
                {
                    FileBlock next = _temp ?? new FileBlock(_file._blockSize, _file._useAlignedIo);
                    _current.NextBlockId = _file.AllocBlock(next, BlockFlags.InternalBlock);

                    if (!ReferenceEquals(_current, _first))
                    {
                        _file.WriteBlock(_current.BlockId, _current, _current.Length + FileBlock.HeaderSize);
                        _temp = _current;
                    }
                    _current = next;
                }
            }
            public override void Write(byte[] buffer, int offset, int count)
            {
                _checksum.Add(buffer, offset, count);
                while (count > 0)
                {
                    PrepareWrite();

                    int amt = Math.Min(count, FileBlockDataSize - _current.Length);
                    Array.Copy(buffer, offset, _current.BlockData, _current.Length + _current.DataOffset, amt);
                    _current.Length += amt;
                    offset += amt;
                    count -= amt;
                }
            }
            public override void WriteByte(byte value)
            {
                _checksum.Add(value);
                PrepareWrite();
                int position = _current.Length + _current.DataOffset;
                _current.BlockData[position] = value;
                _current.Length += 1;
            }
        }
        #endregion

        #region FileBlock
        class FileBlock : IDisposable
        {
            public const int HeaderSize = 32;

            private readonly bool _alignedIo;
            private GCHandle _pin;
            private readonly int _baseOffset;
            private readonly int _minimumIo;
            public readonly byte[] BlockData;

            public FileBlock(int blockSize, bool alignIo) 
            {
                _alignedIo = alignIo;
                _baseOffset = 0;
                _minimumIo = HeaderSize;
                BlockData = new byte[alignIo ? blockSize * 2 : blockSize];

                if (_alignedIo)
                {
                    //FILE_FLAG_NO_BUFFERING required alignment see http://msdn.microsoft.com/en-us/library/cc644950(v=vs.85).aspx
                    _pin = GCHandle.Alloc(BlockData, GCHandleType.Pinned);
                    _baseOffset = (int)(_pin.AddrOfPinnedObject().ToInt64() & (blockSize - 1));
                    _minimumIo = blockSize;
                }
            }

            public int DataOffset { get { return _baseOffset + HeaderSize; } }
            private int BlockSize { get { return _alignedIo ? (BlockData.Length / 2) : BlockData.Length; } }

            public void Dispose()
            {
                if (_alignedIo && _pin.IsAllocated)
                    _pin.Free();
            }

            public void Clear()
            {
                long id = BlockId;
                Array.Clear(BlockData, 0, BlockData.Length);
                BlockId = id;
            }

            private long ReadLong(int offset)
            {
                int start = _baseOffset + offset;
                return (
                    ((long)BlockData[start + 0] << 56) |
                    ((long)BlockData[start + 1] << 48) |
                    ((long)BlockData[start + 2] << 40) |
                    ((long)BlockData[start + 3] << 32) |
                    ((long)BlockData[start + 4] << 24) |
                    ((long)BlockData[start + 5] << 16) |
                    ((long)BlockData[start + 6] << 8) |
                    ((long)BlockData[start + 7] << 0)
                    );
            }
            private void WriteLong(int offset, long value)
            {
                int start = _baseOffset + offset;
                BlockData[start + 0] = (byte)(value >> 56);
                BlockData[start + 1] = (byte)(value >> 48);
                BlockData[start + 2] = (byte)(value >> 40);
                BlockData[start + 3] = (byte)(value >> 32);
                BlockData[start + 4] = (byte)(value >> 24);
                BlockData[start + 5] = (byte)(value >> 16);
                BlockData[start + 6] = (byte)(value >> 8);
                BlockData[start + 7] = (byte)(value >> 0);
            }
            private int ReadInt(int offset)
            {
                int start = _baseOffset + offset;
                return (
                    (BlockData[start + 0] << 24) |
                    (BlockData[start + 1] << 16) |
                    (BlockData[start + 2] << 8) |
                    (BlockData[start + 3] << 0)
                    );
            }
            private void WriteInt(int offset, int value)
            {
                int start = _baseOffset + offset;
                BlockData[start + 0] = (byte)(value >> 24);
                BlockData[start + 1] = (byte)(value >> 16);
                BlockData[start + 2] = (byte)(value >> 8);
                BlockData[start + 3] = (byte)(value >> 0);
            }

            public long BlockId { get { return ReadLong(0); } set { WriteLong(0, value); } }
            public long NextBlockId { get { return ReadLong(8); } set { WriteLong(8, value); } }
            public int Length { get { return ReadInt(16); } set { WriteInt(16, value); } }
            public int CheckSum { get { return ReadInt(20); } set { WriteInt(20, value); } }
            //private int Reserved { get { return ReadInt(24); } set { WriteInt(24, value); } }
            public BlockFlags Flags { get { return (BlockFlags)ReadInt(28); } set { WriteInt(28, (int)value); } }

            public void IncrementId(long versionMask)
            {
                BlockId = (BlockId & ~versionMask) | ((BlockId + 1) & versionMask);
            }

            public void Read(Stream stream, int length)
            {
                int bytesRead = stream.Read(BlockData, _baseOffset, Math.Max(_minimumIo, length));
                if (bytesRead < HeaderSize)
                    throw new InvalidDataException();
            }

            public void Write(Stream stream, int length)
            {
                stream.Write(BlockData, _baseOffset, Math.Max(_minimumIo, length));
                if(!_alignedIo) stream.Flush();
            }

            public FileBlock Clone()
            {
                FileBlock copy = new FileBlock(BlockSize, _alignedIo);
                Array.Copy(BlockData, _baseOffset, copy.BlockData, copy._baseOffset, BlockSize);
                return copy;
            }
        }
        #endregion

        [Flags]
        enum BlockFlags : uint
        {
            None = 0,
            HeaderFilter    = 0x000F0000, // Allowable flags in header
            HeaderFlags     = 0x01000000, // A valid header, for breaking compatibility between versions.
            ResizingFile    = 0x00020000, // Currently resizing the file
            BlockDeleted    = 0x00000001, // A block that has been destroyed
            InternalBlock   = 0x00000002, // A block that chains with a public block index
            ExternalBlock   = 0x00000004, // A block that was allocated publicly
            Temporary       = 0x00000008, // Indicates a block that was allocated but not written to
        }
    }
}
