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
using System.Threading;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Collections.Exceptions;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Storage
{
    /// <summary>
    ///     Provides an in-memory implementation of the storage services for BPlusTree, useful when testing :)
    /// </summary>
    internal class BTreeMemoryStore : INodeStorage
    {
        private readonly ISerializer<string> _stringSerializer;
        private MyStorageHandle _root;

        /// <summary> Default in-memory storage </summary>
        public BTreeMemoryStore()
        {
            _stringSerializer = PrimitiveSerializer.Instance;
        }

        public void Dispose()
        {
            _root = null;
        }

        public IStorageHandle OpenRoot(out bool isNew)
        {
            isNew = _root == null;
            _root = _root ?? new MyStorageHandle("ROOT");
            return _root;
        }

        public void Reset()
        {
            _root = null;
        }

        public bool TryGetNode<TNode>(IStorageHandle handleIn, out TNode node, ISerializer<TNode> serializer)
        {
            InvalidNodeHandleException.Assert(handleIn is MyStorageHandle);
            MyStorageHandle handle = (MyStorageHandle) handleIn;
            if (handle.Node != null)
            {
                node = (TNode) handle.Node;
                return true;
            }
            node = default(TNode);
            return false;
        }

        [Obsolete("Not supported", true)]
        void ISerializer<IStorageHandle>.WriteTo(IStorageHandle value, Stream stream)
        {
            throw new NotSupportedException();
        }

        [Obsolete("Not supported", true)]
        IStorageHandle ISerializer<IStorageHandle>.ReadFrom(Stream stream)
        {
            throw new NotSupportedException();
        }

        public IStorageHandle Create()
        {
            MyStorageHandle handle = new MyStorageHandle();
            return handle;
        }

        public void Destroy(IStorageHandle handleIn)
        {
            InvalidNodeHandleException.Assert(handleIn is MyStorageHandle);
            MyStorageHandle handle = (MyStorageHandle) handleIn;

            handle.Clear();
        }

        public void Update<T>(IStorageHandle handleIn, ISerializer<T> serializer, T node)
        {
            InvalidNodeHandleException.Assert(handleIn is MyStorageHandle);
            MyStorageHandle handle = (MyStorageHandle) handleIn;
            handle.Node = node;
        }

        [DebuggerDisplay("{" + nameof(_id) + "}")]
        private class MyStorageHandle : IStorageHandle
        {
            private static int _counter;
            private readonly string _id;

            internal object Node;

            public MyStorageHandle()
                : this(Interlocked.Increment(ref _counter).ToString())
            {
            }

            public MyStorageHandle(string id)
            {
                _id = id;
            }

            public bool Equals(IStorageHandle other)
            {
                return base.Equals(other);
            }

            public void Clear()
            {
                Node = null;
            }
        }
    }
}