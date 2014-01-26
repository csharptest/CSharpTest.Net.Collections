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
using System.Diagnostics;
using CSharpTest.Net.Bases;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        [DebuggerDisplay("Handle({_storeHandle})")]
        class NodeHandle : Equatable<NodeHandle>
        {
            private readonly IStorageHandle _storeHandle;
            private object _cacheEntry;

            public NodeHandle(IStorageHandle storeHandle)
            {
                _storeHandle = storeHandle;
            }

            public IStorageHandle StoreHandle { get { return _storeHandle; } }

            public bool TryGetCache<T>(out T cacheEntry) where T : class
            {
                cacheEntry = _cacheEntry as T;
                return cacheEntry != null;
            }

            public void SetCacheEntry(object cacheEntry)
            { _cacheEntry = cacheEntry; }

            protected override int HashCode { get { return _storeHandle.GetHashCode(); } }
            public override bool Equals(NodeHandle other) { return _storeHandle.Equals(other._storeHandle); }
        }

        class NodeHandleSerializer : ISerializer<NodeHandle>, ISerializer<IStorageHandle>
        {
            private readonly ISerializer<IStorageHandle> _handleSerializer;

            public NodeHandleSerializer(ISerializer<IStorageHandle> handleSerializer)
            {
                _handleSerializer = handleSerializer;
            }

            public void WriteTo(NodeHandle value, System.IO.Stream stream)
            { _handleSerializer.WriteTo(value.StoreHandle, stream); }

            public NodeHandle ReadFrom(System.IO.Stream stream)
            { return new NodeHandle(_handleSerializer.ReadFrom(stream)); }

            public void WriteTo(IStorageHandle value, System.IO.Stream stream)
            { _handleSerializer.WriteTo(value, stream); }

            IStorageHandle ISerializer<IStorageHandle>.ReadFrom(System.IO.Stream stream)
            { return _handleSerializer.ReadFrom(stream); }
        }
    }
}
