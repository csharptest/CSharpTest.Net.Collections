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
using System.IO;
using System.Reflection;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using Microsoft.Win32.SafeHandles;

// ReSharper disable InconsistentNaming
namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Similar behavior to the FragmentedFile; however, a much improved implementation.  Allows for
    /// file-level commit/rollback or write-by-write commits to disk.  By default provides data-protection
    /// against process-crashes but not OS crashes.  Use FileOptions.WriteThrough to protect against
    /// OS crashes and power outtages.
    /// </summary>
    public class TransactedCompoundFile : IDisposable, ITransactable
    {
        enum LoadFrom { FirstBlock, LastBlock, Either }
        #region Options and Delegates
        delegate void FPut(long position, byte[] bytes, int offset, int length);
        delegate int FGet(long position, byte[] bytes, int offset, int length);

        /// <summary>
        /// Advanced Options used to construct a TransactedCompoundFile
        /// </summary>
        public class Options : ICloneable
        {
            private string _filePath;
            private int _blockSize;
            private FileOptions _fileOptions;
            private bool _createNew;
            private bool _commitOnWrite;
            private bool _commitOnDispose;
            private LoadingRule _loadingRule;
            private bool _readOnly;

            /// <summary>
            /// Constructs an Options instance
            /// </summary>
            /// <param name="filePath">The file name to use</param>
            public Options(string filePath)
            {
                _filePath = Check.NotNull(filePath);
                BlockSize = 4096;
                _fileOptions = FileOptions.None;
                _readOnly = false;
                _createNew = false;
                _commitOnWrite = false;
                _loadingRule = LoadingRule.Default;
            }
            /// <summary>
            /// Retrieves the file name that was provided to the constructor
            /// </summary>
            public string FilePath
            {
                get { return _filePath; }
            }
            /// <summary>
            /// Defines the block-size used for storing data.  Data storred in a given handle must be less than ((16*BlockSize)-8)
            /// </summary>
            public int BlockSize
            {
                get { return _blockSize; }
                set
                {
                    int bit = 0;
                    for (int i = value; i != 1; i >>= 1)
                        bit++;
                    if (1 << bit != value)
                        throw new ArgumentException("BlockSize Must be a power of 2", "blockSize");
                    _blockSize = Check.InRange(value, 512, 65536);
                }
            }
            /// <summary>
            /// Returns the maximum number of bytes that can be written to a single handle base on the current BlockSize setting.
            /// </summary>
            public int MaxWriteSize
            {
                get { return (BlockSize*((BlockSize/4) - 2)) - BlockHeaderSize; }
            }
            /// <summary>
            /// The FileOptions used for writing to the file
            /// </summary>
            public FileOptions FileOptions
            {
                get { return _fileOptions; }
                set { _fileOptions = value; }
            }
            /// <summary>
            /// Gets or sets a flag that controls if the file is opened in read-only mode.  For ReadOnly
            /// files, another writer may exist; however, changes to the file will not be reflected until
            /// reload.
            /// </summary>
            public bool ReadOnly
            {
                get { return _readOnly; }
                set { _readOnly = value; }
            }
            /// <summary>
            /// True to create a new file, false to use the existing file.  If this value is false and the
            /// file does not exist an exception will be raised.
            /// </summary>
            public bool CreateNew
            {
                get { return _createNew; }
                set { _createNew = value; }
            }
            /// <summary>
            /// When true every write will rewrite the modified handle(s) back to disk, otherwise the
            /// handle state is kept in memory until a call to commit has been made.
            /// </summary>
            public bool CommitOnWrite
            {
                get { return _commitOnWrite; }
                set { _commitOnWrite = value; }
            }
            /// <summary>
            /// Automatically Commit the storage file when it's disposed.
            /// </summary>
            public bool CommitOnDispose
            {
                get { return _commitOnDispose; }
                set { _commitOnDispose = value; }
            }
            /// <summary>
            /// See comments on the LoadingRule enumerated type and Commit(Action,T)
            /// </summary>
            public LoadingRule LoadingRule
            {
                get { return _loadingRule; }
                set { _loadingRule = value; }
            }

            object ICloneable.Clone() { return Clone(); }
            /// <summary>
            /// Returns a copy of the options currently specified.
            /// </summary>
            public Options Clone()
            {
                return (Options)MemberwiseClone();
            }
        }

        /// <summary>
        /// Defines the loading rule to apply when using a transacted file that was interrupted
        /// durring the commit process.
        /// </summary>
        public enum LoadingRule
        {
            /// <summary>
            /// Load all from Primary if valid, else load all from Secondary.  If both fail,
            /// load either Primary or Secondary for each segment.  This is the normal option,
            /// use the other options only when recovering from a commit that was incomplete.
            /// </summary>
            Default,
            /// <summary>
            /// If you previously called Commit(Action,T) on a prior instance and the Action
            /// delegate *was* called, then setting this value will ensure that only the 
            /// primary state storage is loaded, thereby ensuring you load the 'previous'
            /// state.
            /// </summary>
            Primary,
            /// <summary>
            /// If you previously called Commit(Action,T) on a prior instance and the Action
            /// delegate was *not* called, then setting this value will ensure that only the 
            /// secondary state storage is loaded, thereby ensuring you load the 'previous'
            /// state.
            /// </summary>
            Secondary,
        }
        #endregion

        /// <summary>
        /// Returns the first block that *would* be allocated by a call to Create() on an empty file.
        /// </summary>
        public static uint FirstIdentity { get { return 1; } }

        const int BlockHeaderSize = 16; //length + CRC
        private const int OffsetOfHeaderSize = 0;
        private const int OffsetOfLength = 0;
        private const int OffsetOfCrc32 = 4;
        private const int OffsetOfBlockCount = 8;
        private const int OffsetOfBlockId = 12;

        readonly Options _options;
        readonly int BlockSize;
        readonly int BlocksPerSection;
        readonly long SectionSize;

        readonly object _sync;
        FileSection[] _sections;

        IFactory<Stream> _readers;
        Stream _stream;
        FPut _fcommit;
        FPut _fput;
        FGet _fget;

        int _firstFreeBlock, _prevFreeBlock, _prevFreeHandle;
        OrdinalList _freeHandles;
        OrdinalList _freeBlocks;
        OrdinalList _reservedBlocks;

        /// <summary>
        /// Creates or opens a TransactedCompoundFile using the filename specified.
        /// </summary>
        public TransactedCompoundFile(string filename)
            : this(new Options(filename) { CreateNew = !File.Exists(filename) })
        { }

        /// <summary>
        /// Creates or opens a TransactedCompoundFile using the filename specified.
        /// </summary>
        public TransactedCompoundFile(Options options)
        {
            _options = options.Clone();
            _sync = new object();
            BlockSize = _options.BlockSize;
            BlocksPerSection = BlockSize >> 2;
            SectionSize = BlockSize * BlocksPerSection;

            _freeHandles = new OrdinalList();
            _freeBlocks = new OrdinalList();
            _reservedBlocks = new OrdinalList();

            _stream =
                new FileStream(
                _options.FilePath,
                _options.CreateNew ? FileMode.Create : FileMode.Open,
                _options.ReadOnly ? FileAccess.Read : FileAccess.ReadWrite,
                _options.ReadOnly ? FileShare.ReadWrite : FileShare.Read,
                8,
                _options.FileOptions
                );
            _fcommit = _fput = fput;
            _fget = ReadBytes;

            try
            {
                LoadSections(_stream);
                if (_sections.Length == 0)
                    AddSection();
            }
            catch
            {
                _stream.Dispose();
                throw;
            }

            if (!_options.CommitOnWrite)
            {
                _fcommit = null;
            }

            _readers = new StreamCache(
                new FileStreamFactory(_options.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 8, FileOptions.None), 
                4);
        }

        private void fput(long position, byte[] bytes, int offset, int length)
        {
            try { }
            finally
            {
                _stream.Position = position;
                _stream.Write(bytes, offset, length);
            }
        }

        /// <summary>
        /// Closes all streams and clears all in-memory data.
        /// </summary>
        public void Dispose()
        {
            lock (_sync)
            {
                try
                {
                    if (_stream != null && _options.CommitOnDispose)
                        Commit();
                }
                finally
                {
                    _stream.Dispose();
                    if (_readers is IDisposable)
                        ((IDisposable) _readers).Dispose();
                    _stream = null;
                    _freeHandles.Clear();
                    _freeBlocks.Clear();
                    _reservedBlocks.Clear();
                    _sections = new FileSection[0];
                }
            }
        }

        #region void FlushStream(Stream stream)
#if !NET40
        [System.Runtime.InteropServices.DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
        private static extern bool FlushFileBuffers(IntPtr hFile);
        void FlushStream(Stream stream)
        {
            FileStream fs = stream as FileStream;
            if(fs == null || (_options.FileOptions & FileOptions.WriteThrough) != 0)
                stream.Flush();
            else
            {
                SafeFileHandle handle = (SafeFileHandle)fs.GetType().GetField("_handle", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(fs);
                if (!FlushFileBuffers(handle.DangerousGetHandle()))
                    throw new System.ComponentModel.Win32Exception();
            }
        }
#else
        void FlushStream(Stream stream)
        {
            FileStream fs = stream as FileStream;
            if (fs == null)
                stream.Flush();
            else
                fs.Flush(true);
        }
#endif
        #endregion
        /// <summary>
        /// Flushes any pending writes to the disk and returns.
        /// </summary>
        void Flush(bool forced)
        {
            if (_stream == null)
                throw new ObjectDisposedException(GetType().FullName);

            if (!forced)
            {
                lock (_sync)
                    _stream.Flush();
            }
            else
            {
                lock (_sync)
                    FlushStream(_stream);
            }
        }
        /// <summary>
        /// For file-level transactions, performs a two-stage commit of all changed handles.
        /// </summary>
        public void Commit()
        {
            try { }
            finally
            {
                Commit(null, 0);
            }
        }
        /// <summary>
        /// For file-level transactions, performs a two-stage commit of all changed handles.
        /// After the first stage has completed, the stageCommit() delegate is invoked.
        /// </summary>
        /// <remarks>
        /// 
        /// </remarks>
        public void Commit<T>(Action<T> stageCommit, T value)
        {
            if (_options.CommitOnWrite)
            {
                Flush(true);
                return;
            }

            lock(_sync)
            {
                if (_stream == null)
                    throw new ObjectDisposedException(GetType().FullName);

                //Phase 1 - commit block 0 for each section
                foreach (var section in _sections)
                    section.Commit(_fput, false);
                Flush(true);

                try { if (stageCommit != null) stageCommit(value); }
                finally
                {
                    //Phase 2 - commit block max for each section and set clean
                    foreach (var section in _sections)
                        section.Commit(_fput, true);
                    Flush(true);

                    foreach (int ifree in _reservedBlocks)
                    {
                        _firstFreeBlock = Math.Min(_firstFreeBlock, ifree);
                        break;
                    }
                    _reservedBlocks = _freeBlocks.Invert((_sections.Length*BlocksPerSection) - 1);
                }
            }
        }
        /// <summary>
        /// For file-level transactions, Reloads the file from it's original (or last committed) state.
        /// </summary>
        /// <exception cref="InvalidOperationException">When CommitOnWrite is true, there is no going back.</exception>
        public void Rollback()
        {
            if (_options.CommitOnWrite)
                throw new InvalidOperationException();

            lock (_sync)
            {
                if (_stream == null)
                    throw new ObjectDisposedException(GetType().FullName);

                LoadSections(_stream);
            }
        }

        #region Private Implementation
        private void LoadSections(Stream stream)
        {
            switch(_options.LoadingRule)
            {
                case LoadingRule.Primary:
                    if (!LoadSections(stream, LoadFrom.FirstBlock))
                        throw new InvalidDataException();
                    break;
                case LoadingRule.Secondary:
                    if (!LoadSections(stream, LoadFrom.LastBlock))
                        throw new InvalidDataException();
                    break;
                case LoadingRule.Default:
                default:
                    if (!LoadSections(stream, LoadFrom.FirstBlock))
                        if (!LoadSections(stream, LoadFrom.LastBlock))
                            if (!LoadSections(stream, LoadFrom.Either))
                                throw new InvalidDataException();
                    break;
            }
        }

        private bool LoadSections(Stream stream, LoadFrom from)
        {
            long fsize = stream.Length;
            long hsize = fsize / SectionSize;
            var sections = new FileSection[hsize];

            for (int i = 0; i < sections.Length; i++)
            {
                if (from == LoadFrom.Either && FileSection.TryLoadSection(stream, false, i, BlockSize, out sections[i]))
                    continue;

                if (!FileSection.TryLoadSection(stream, from != LoadFrom.FirstBlock, i, BlockSize, out sections[i]))
                    return false;
            }

            int lastIndex = (int)(hsize * BlocksPerSection) - 1;

            OrdinalList freeHandles = new OrdinalList();
            freeHandles.Ceiling = lastIndex;

            OrdinalList usedBlocks = new OrdinalList();
            usedBlocks.Ceiling = lastIndex;

            foreach (var section in sections)
                section.GetFree(freeHandles, usedBlocks, _fget);

            _sections = sections;
            _freeHandles = freeHandles;
            _freeBlocks = usedBlocks.Invert(lastIndex);
            if (!_options.CommitOnWrite)
            {
                _reservedBlocks = usedBlocks;
            }
            _firstFreeBlock = _prevFreeBlock = _prevFreeHandle = 0;
            return true;
        }

        private int AddSection()
        {
            FileSection n = new FileSection(_sections.Length, BlockSize);
            lock (_sync)
            {
                n.Commit(_fput, false);
                n.Commit(_fput, true);
            }

            FileSection[] grow = new FileSection[_sections.Length + 1];
            _sections.CopyTo(grow, 0);
            grow[_sections.Length] = n;

            OrdinalList freeblocks = _freeBlocks.Clone();
            freeblocks.Ceiling = (grow.Length * BlocksPerSection) - 1;

            OrdinalList freehandles = _freeHandles.Clone();
            freehandles.Ceiling = (grow.Length * BlocksPerSection) - 1;
            // First and last handles/blocks are reserved by the section
            int lastFree = grow.Length * BlocksPerSection - 1;
            int firstFree = lastFree - BlocksPerSection + 2;
            for (int i = firstFree; i < lastFree; i++)
            {
                freehandles.Add(i);
                freeblocks.Add(i);
            }

            _sections = grow;
            _freeHandles = freehandles;
            _freeBlocks = freeblocks;
            return firstFree;
        }

        private uint TakeBlocks(int blocksNeeded)
        {
            lock (_sync)
            {
                bool rescan = false;
                bool resized = false;
                int startingFrom = _prevFreeBlock;
                int endingBefore = int.MaxValue;
                while (true)
                {
                    int found = 0;
                    int last = int.MinValue;
                    int first = int.MaxValue;
                    foreach (int free in _freeBlocks.EnumerateRange(startingFrom, endingBefore))
                    {
                        if (_reservedBlocks.Contains(free))
                            continue;

                        if (found == 0)
                        {
                            _prevFreeBlock = free;
                            if (!resized && rescan)
                                _firstFreeBlock = free;
                        }

                        first = Math.Min(first, free);
                        found = (last + 1 != free) ? 1 : found + 1;
                        last = free;
                        if (found == blocksNeeded)
                        {
                            int start = free - (blocksNeeded - 1);
                            for (int i = start; i <= free; i++)
                                _freeBlocks.Remove(i);

                            uint blockId = (uint) start;
                            blockId |= ((uint) Math.Min(16, blocksNeeded) - 1 << 28) & 0xF0000000u;
                            return blockId;
                        }
                    }
                    if (resized)
                        throw new ArgumentOutOfRangeException("length");

                    if (!rescan && _firstFreeBlock < startingFrom)
                    {
                        rescan = true;
                        endingBefore = startingFrom + blocksNeeded - 1;
                        startingFrom = _firstFreeBlock;
                    }
                    else
                    {
                        resized = true;
                        startingFrom = AddSection();
                        endingBefore = int.MaxValue;
                    }
                }
            }
        }

        private void FreeBlocks(BlockRef block)
        {
            int free = (block.Section * BlocksPerSection) + block.Offset;
            if (free > 0)
            {
                _firstFreeBlock = Math.Min(_firstFreeBlock, free);

                if(block.ActualBlocks == 16)
                {
                    using (_sections[block.Section].Read(ref block, true, _fget))
                    { }
                    if (((block.Count < 16 && block.ActualBlocks != block.Count) ||
                         (block.Count == 16 && block.ActualBlocks < 16)))
                        throw new InvalidDataException();
                }

                for (int i = 0; i < block.ActualBlocks; i++)
                    _freeBlocks.Add(free + i);
            }
        }

        private int ReadBytes(long position, byte[] bytes, int offset, int length)
        {
            if (_readers != null)
            {
                using (Stream io = _readers.Create())
                {
                    io.Position = position;
                    return IOStream.ReadChunk(io, bytes, offset, length);
                }
            }

            lock (_sync)
            {
                _stream.Position = position;
                return IOStream.ReadChunk(_stream, bytes, offset, length);
            }
        }
        #endregion

        /// <summary>
        /// Allocates a handle for data, you MUST call Write to commit the handle, otherwise the handle
        /// may be reallocated after closing and re-opening this file.  If you do not intend to commit
        /// the handle by writing to it, you should still call Delete() so that it may be reused.
        /// </summary>
        public uint Create()
        {
            uint handle = 0;
            lock (_sync)
            {
                while (handle == 0)
                {
                    foreach (int i in _freeHandles.EnumerateFrom(_prevFreeHandle))
                    {
                        _freeHandles.Remove(i);
                        _prevFreeHandle = i + 1;
                        handle = (uint)i;
                        break;
                    }
                    if (handle == 0)
                        AddSection();
                }
            }

            HandleRef href = new HandleRef(handle, BlockSize);
            uint blockId = _sections[href.Section][href.Offset];
            if (blockId != 0)
                throw new InvalidDataException();

            return handle;
        }
        /// <summary>
        /// Writes the bytes provided to the handle that was previously obtained by a call to Create().
        /// The length must not be more than ((16*BlockSize)-32) bytes in length.  The exact header size
        /// (32 bytes) may change without notice in a future release.
        /// </summary>
        public void Write(uint handle, byte[] bytes, int offset, int length)
        {
            HandleRef href = new HandleRef(handle, BlockSize);
            if (handle == 0 || href.Section >= _sections.Length || _freeHandles.Contains((int)handle))
                throw new ArgumentOutOfRangeException("handle");

            uint oldblockId = _sections[href.Section][href.Offset];

            int blocksNeeded = Math.Max(1, (length + BlockHeaderSize + BlockSize - 1) / BlockSize);
            if (blocksNeeded > BlocksPerSection-2)
                throw new ArgumentOutOfRangeException("length");

            uint blockId = TakeBlocks(blocksNeeded);
            BlockRef block = new BlockRef(blockId, BlockSize, blocksNeeded);

            lock (_sync)
            {
                _sections[block.Section].Write(block, _fput, bytes, offset, length);
                _sections[href.Section].SetHandle(_fcommit, href.Offset, blockId);
                if (oldblockId != 0)
                    FreeBlocks(new BlockRef(oldblockId, BlockSize));
            }
        }
        /// <summary>
        /// Reads all bytes from the from the handle specified
        /// </summary>
        public Stream Read(uint handle)
        {
            HandleRef href = new HandleRef(handle, BlockSize);
            if (handle == 0 || href.Section >= _sections.Length || _freeHandles.Contains((int)handle))
                throw new ArgumentOutOfRangeException("handle");

            uint blockId = _sections[href.Section][href.Offset];
            if (blockId == 0) return new MemoryStream(new byte[0], false);

            if (_freeBlocks.Contains((int)blockId & 0x0FFFFFFF))
                throw new InvalidDataException();

            BlockRef block = new BlockRef(blockId, BlockSize);
            return _sections[block.Section].Read(ref block, false, _fget);
        }
        /// <summary>
        /// Deletes the handle and frees the associated block space for reuse.
        /// </summary>
        public void Delete(uint handle)
        {
            HandleRef href = new HandleRef(handle, BlockSize);
            if (handle == 0 || href.Section >= _sections.Length || _freeHandles.Contains((int)handle))
                throw new ArgumentOutOfRangeException("handle");

            uint oldblockId = _sections[href.Section][href.Offset];
            lock (_sync)
            {
                _sections[href.Section].SetHandle(_fcommit, href.Offset, 0);

                if (oldblockId != 0)
                    FreeBlocks(new BlockRef(oldblockId, BlockSize));
                _freeHandles.Add((int)handle);
                _prevFreeHandle = Math.Min(_prevFreeHandle, (int)handle);
            }
        }
        /// <summary>
        /// Immediatly truncates the file to zero-length and re-initializes an empty file
        /// </summary>
        public void Clear()
        {
            lock (_sync)
            {
                _stream.SetLength(0);
                _freeBlocks.Clear();
                _freeHandles.Clear();
                _reservedBlocks.Clear();
                _firstFreeBlock = _prevFreeBlock = _prevFreeHandle = 0;

                _sections = new FileSection[0];
                AddSection();
            }
        }

        struct HandleRef
        {
            public readonly int Section;
            public readonly int Offset;

            public HandleRef(uint handle, int blockSize)
            {
                int blocksPerSection = (blockSize >> 2);
                Section = (int)handle / blocksPerSection;
                Offset = (int)handle % blocksPerSection;

                if (Section < 0 || Section >= 0x10000000 || Offset <= 0 || Offset >= blocksPerSection - 1)
                    throw new ArgumentOutOfRangeException("handle");
            }
        }

        struct BlockRef
        {
            public readonly uint Identity;
            public readonly int Section;
            public readonly int Offset;
            public readonly int Count;
            public int ActualBlocks;

            public BlockRef(uint block, int blockSize)
            {
                Identity = block;
                ActualBlocks = Count = (int)(block >> 28 & 0x0F) + 1;
                block &= 0x0FFFFFFF;
                int blocksPerSection = (blockSize >> 2);
                Section = (int)block / blocksPerSection;
                Offset = (int)block % blocksPerSection;

                if (Section < 0 || Section >= 0x10000000 || Offset <= 0 || (Offset + Count - 1) >= blocksPerSection - 1)
                    throw new ArgumentOutOfRangeException("block");
            }

            public BlockRef(uint blockId, int blockSize, int actualBlocks)
                : this(blockId, blockSize)
            {
                ActualBlocks = actualBlocks;
            }
        }

        class FileSection
        {
            const int _baseOffset = 0;
            readonly int BlockSize;
            readonly int HandleCount;
            readonly int BlocksPerSection;
            readonly long SectionSize;

            readonly int _sectionIndex;
            readonly long _sectionPosition;
            readonly byte[] _blockData;

            private bool _isDirty;

            private FileSection(int sectionIndex, int blockSize, bool create)
            {
                _sectionIndex = sectionIndex;
                BlockSize = blockSize;
                HandleCount = BlockSize >> 2;
                BlocksPerSection = BlockSize >> 2;
                SectionSize = BlockSize * BlocksPerSection;

                _sectionPosition = SectionSize * sectionIndex;
                _blockData = new byte[BlockSize];
                if (create)
                {
                    MakeValid();
                    _isDirty = true;
                }
            }

            public FileSection(int sectionIndex, int blockSize)
                : this(sectionIndex, blockSize, true)
            { }

            public static bool TryLoadSection(Stream stream, bool alt, int sectionIndex, int blockSize, out FileSection section)
            {
                section = new FileSection(sectionIndex, blockSize, false);
                byte[] part1 = alt ? new byte[blockSize] : section._blockData;
                stream.Position = section._sectionPosition;
                IOStream.Read(stream, part1, blockSize);

                byte[] part2 = !alt ? new byte[blockSize] : section._blockData;
                stream.Position = section._sectionPosition + (section.SectionSize - blockSize);
                IOStream.Read(stream, part2, blockSize);

                section._isDirty = BinaryComparer.Compare(part1, part2) != 0;

                if (!section.CheckValid())
                {
                    section = null;
                    return false;
                }
                return true;
            }

            public void SetHandle(FPut fcommit, int index, uint blockId)
            {
                if (index <= 0 || index >= BlocksPerSection - 1)
                    throw new InvalidDataException();
                WriteUInt32(index, blockId);
                _isDirty = true;

                if (fcommit != null)
                {
                    Commit(fcommit, false);
                    Commit(fcommit, true);
                }
            }

            public void Commit(FPut put, bool phase2)
            {
                if (!_isDirty)
                    return;

                if (phase2 && ReadUInt32(0) != CalcCrc32())
                    throw new InvalidDataException();
                else 
                    MakeValid();

                long phaseShift = phase2 ? (SectionSize - BlockSize) : 0;
                put(_sectionPosition + phaseShift, _blockData, 0, BlockSize);

                if (phase2)
                    _isDirty = false;
            }

            public void Write(BlockRef block, FPut fput, byte[] bytes, int offset, int length)
            {
                byte[] blockdata = new byte[BlockSize * block.ActualBlocks];
                PutUInt32(blockdata, OffsetOfLength, (uint)length);
                blockdata[OffsetOfHeaderSize] = BlockHeaderSize;
                Crc32 crc = new Crc32();
                crc.Add(bytes, offset, length);
                PutUInt32(blockdata, OffsetOfCrc32, (uint)crc.Value);
                PutUInt32(blockdata, OffsetOfBlockCount, (uint)block.ActualBlocks);
                PutUInt32(blockdata, OffsetOfBlockId, block.Identity);
                Buffer.BlockCopy(bytes, offset, blockdata, BlockHeaderSize, length);

                long position = _sectionPosition + (BlockSize * block.Offset);
                fput(position, blockdata, 0, blockdata.Length);
            }

            public Stream Read(ref BlockRef block, bool headerOnly, FGet fget)
            {
                bool retry;
                byte[] bytes;
                int readBytes, headerSize, length;
                do
                {
                    retry = false;
                    long position = _sectionPosition + (BlockSize*block.Offset);
                    bytes = new byte[headerOnly ? BlockHeaderSize : block.ActualBlocks * BlockSize];

                    readBytes = fget(position, bytes, 0, bytes.Length);
                    if (readBytes < BlockHeaderSize)
                        throw new InvalidDataException();

                    headerSize = bytes[OffsetOfHeaderSize];
                    length = (int) GetUInt32(bytes, OffsetOfLength) & 0x00FFFFFF;

                    block.ActualBlocks = (int) GetUInt32(bytes, OffsetOfBlockCount);
                    uint blockId = GetUInt32(bytes, OffsetOfBlockId);

                    if (headerSize < BlockHeaderSize || blockId != block.Identity ||
                        ((block.Count < 16 && block.ActualBlocks != block.Count) ||
                         (block.Count == 16 && block.ActualBlocks < 16)))
                        throw new InvalidDataException();

                    if (block.ActualBlocks != Math.Max(1, (length + headerSize + BlockSize - 1)/BlockSize))
                        throw new InvalidDataException();

                    if (headerOnly)
                        return null;
                    if (readBytes < length + headerSize)
                    {
                        retry = bytes.Length != block.ActualBlocks*BlockSize;
                    }
                } while (retry);

                if (readBytes < length + headerSize)
                    throw new InvalidDataException();

                Crc32 crc = new Crc32();
                crc.Add(bytes, headerSize, length);
                if ((uint)crc.Value != GetUInt32(bytes, OffsetOfCrc32))
                    throw new InvalidDataException();

                return new MemoryStream(bytes, headerSize, length, false, false);
            }

            public void GetFree(ICollection<int> freeHandles, ICollection<int> usedBlocks, FGet fget)
            {
                int baseHandle = unchecked(BlocksPerSection * _sectionIndex);
                //reserved: first and last block
                usedBlocks.Add(baseHandle);
                usedBlocks.Add(baseHandle + BlocksPerSection - 1);

                for (int handle = 1; handle < BlocksPerSection - 1; handle++)
                {
                    uint data = ReadUInt32(handle);
                    if (data == 0)
                        freeHandles.Add(baseHandle + handle);
                    else
                    {
                        BlockRef block = new BlockRef(data, BlockSize);
                        int blockId = (int)block.Identity & 0x0FFFFFFF;

                        if (block.Count == 16)
                        {
                            long position = (long)BlocksPerSection*BlockSize*block.Section;
                            position += BlockSize*block.Offset;
                            byte[] header = new byte[BlockHeaderSize];
                            if (BlockHeaderSize != fget(position, header, 0, header.Length))
                                throw new InvalidDataException();
                            block.ActualBlocks = (int)GetUInt32(header, OffsetOfBlockCount);
                        }

                        for (uint i = 0; i < block.ActualBlocks; i++)
                            usedBlocks.Add(blockId++);
                    }
                }
            }

            public uint this[int index]
            {
                get 
                {
                    if (index < 1 || index >= BlocksPerSection - 1)
                        throw new ArgumentOutOfRangeException();
                    return ReadUInt32(index);
                }
            }

            private uint ReadUInt32(int ordinal)
            {
                int offset = ordinal << 2;
                int start = _baseOffset + offset;
                lock (_blockData)
                    return GetUInt32(_blockData, start);
            }

            private void WriteUInt32(int ordinal, uint value)
            {
                int offset = ordinal << 2;
                int start = _baseOffset + offset;
                lock(_blockData)
                    PutUInt32(_blockData, start, value);
            }

            private void MakeValid()
            {
                uint crc = CalcCrc32();
                PutUInt32(_blockData, 0, crc);
                PutUInt32(_blockData, _blockData.Length - 4, crc);
            }

            private uint CalcCrc32()
            {
                Crc32 crc = new Crc32();
                crc.Add(_blockData, 4, BlockSize - 8);
                return unchecked((uint) crc.Value);
            }

            private bool CheckValid()
            {
                uint crc1 = GetUInt32(_blockData, 0);
                uint crc2 = GetUInt32(_blockData, _blockData.Length - 4);
                return crc1 == crc2 && crc1 == CalcCrc32();
            }
        }

        private static uint GetUInt32(byte[] bytes, int start)
        {
            return unchecked((uint)
                (
                (bytes[start + 0] << 24) |
                (bytes[start + 1] << 16) |
                (bytes[start + 2] << 8) |
                (bytes[start + 3] << 0)
                ));
        }
        private static void PutUInt32(byte[] bytes, int start, uint value)
        {
            bytes[start + 0] = (byte)(value >> 24);
            bytes[start + 1] = (byte)(value >> 16);
            bytes[start + 2] = (byte)(value >> 8);
            bytes[start + 3] = (byte)(value >> 0);
        }
    }
}
