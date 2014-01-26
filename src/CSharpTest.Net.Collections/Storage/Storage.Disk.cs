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
using System.Diagnostics;
using System.IO;
using System.Text;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Storage
{
    /// <summary>
    /// Provides a file-based storage for the BPlusTree dictionary
    /// </summary>
    class BTreeFileStore : INodeStorage
    {
        static readonly ISerializer<IStorageHandle> StorageHandleSerializer = new HandleSerializer();
        private readonly byte[] _fileId;
        private readonly FragmentedFile _file;
        private readonly FileId _rootId;

        /// <summary>
        /// Opens an existing BPlusTree file at the path specified, for a new file use CreateNew()
        /// </summary>
        public BTreeFileStore(string filePath, int blockSize, int growthRate, int concurrentWriters, FileOptions options, bool readOnly)
            : this(new FragmentedFile(filePath, blockSize, growthRate, concurrentWriters,
                        readOnly ? FileAccess.Read : FileAccess.ReadWrite, 
                        readOnly ? FileShare.Read : FileShare.ReadWrite, 
                        options))
        {
           _fileId = Encoding.UTF8.GetBytes(Path.GetFullPath(filePath).ToLower());
        }

        private BTreeFileStore(FragmentedFile filestore)
        {
            _file = filestore;
            _rootId = new FileId(_file.FirstIdentity);
        }

        /// <summary>
        /// Closes the file in it's current state.
        /// </summary>
        public void Dispose()
        {
            _file.Dispose();
        }
        
        /// <summary>
        /// Creates an empty file store in the path specified
        /// </summary>
        public static BTreeFileStore CreateNew(string filepath, int blockSize, int growthRate, int concurrentWriters, FileOptions options)
        {
            using (FragmentedFile file = FragmentedFile.CreateNew(filepath, blockSize))
                CreateRoot(file);

            return new BTreeFileStore(filepath, blockSize, growthRate, concurrentWriters, options, false);
        }

        private static void CreateRoot(FragmentedFile file)
        {
            long rootId;
            using (file.Create(out rootId)) { }
            if (rootId != file.FirstIdentity)
                throw new InvalidNodeHandleException();
        }

        public void Reset()
        {
            _file.Clear();
            CreateRoot(_file);
        }

        public IStorageHandle OpenRoot(out bool isNew)
        {
            using (Stream s = _file.Open(_rootId.Id, FileAccess.Read))
                isNew = s.ReadByte() == -1;
            return _rootId;
        }

        public bool TryGetNode<TNode>(IStorageHandle handleIn, out TNode node, ISerializer<TNode> serializer)
        {
            Check.Assert<InvalidNodeHandleException>(handleIn is FileId);
            FileId handle = (FileId)handleIn;
            using (Stream s = _file.Open(handle.Id, FileAccess.Read))
            {
                node = serializer.ReadFrom(s);
                return true;
            }
        }

        public IStorageHandle Create()
        {
            FileId hnew = new FileId(_file.Create());
            return hnew;
        }

        public void Destroy(IStorageHandle handleIn)
        {
            Check.Assert<InvalidNodeHandleException>(handleIn is FileId);
            FileId handle = (FileId)handleIn;
            _file.Delete(handle.Id);
        }

        public void Update<T>(IStorageHandle handleIn, ISerializer<T> serializer, T node)
        {
            Check.Assert<InvalidNodeHandleException>(handleIn is FileId);
            FileId handle = (FileId)handleIn;
            using (Stream s = _file.Open(handle.Id, FileAccess.Write))
            {
                try { } finally { serializer.WriteTo(node, s); }
            }
        }

        void ISerializer<IStorageHandle>.WriteTo(IStorageHandle handleIn, Stream stream)
        { StorageHandleSerializer.WriteTo(handleIn, stream); }

        IStorageHandle ISerializer<IStorageHandle>.ReadFrom(Stream stream)
        { return StorageHandleSerializer.ReadFrom(stream); }

        [DebuggerDisplay("{Id}[{Unique:x8}]")]
        class FileId : IStorageHandle
        {
            public readonly long Id;
            public readonly long Unique;

            public FileId(long id) 
            {
                Id = id;

                byte[] unique = Guid.NewGuid().ToByteArray();
                for (int i = 0; i < 8; i++) unique[i] ^= unique[i + 8];
                Unique = BitConverter.ToInt64(unique, 0);
            }

            public FileId(long id, long unique)
            {
                Id = id;
                Unique = unique;
            }

            bool IEquatable<IStorageHandle>.Equals(IStorageHandle other)
            {
                return Equals(other);
            }

            public override bool Equals(object other)
            {
                if (!(other is FileId)) return false;
                return Id.Equals(((FileId)other).Id)
                    && Unique.Equals(((FileId)other).Unique);
            }

            public override int GetHashCode()
            { return Id.GetHashCode(); }
        }


        public class HandleSerializer : ISerializer<IStorageHandle>
        {
            void ISerializer<IStorageHandle>.WriteTo(IStorageHandle handleIn, Stream stream)
            {
                Check.Assert<InvalidNodeHandleException>(handleIn is FileId);
                FileId handle = (FileId)handleIn;
                PrimitiveSerializer.Int64.WriteTo(handle.Id, stream);
                PrimitiveSerializer.Int64.WriteTo(handle.Unique, stream);
            }

            IStorageHandle ISerializer<IStorageHandle>.ReadFrom(Stream stream)
            {
                return new FileId(
                    PrimitiveSerializer.Int64.ReadFrom(stream),
                    PrimitiveSerializer.Int64.ReadFrom(stream)
                );
            }
        }
    }
}
