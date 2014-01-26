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
using System.Collections.Generic;
using System.Diagnostics;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        class InvalidSerializer<T> : ISerializer<T>
        {
            private InvalidSerializer() { }
            internal static readonly ISerializer<T> Instance = new InvalidSerializer<T>();

            [Obsolete] T ISerializer<T>.ReadFrom(Stream stream) { throw new NotSupportedException(); }
            [Obsolete] void ISerializer<T>.WriteTo(T value, Stream stream) { throw new NotSupportedException(); }
        }

        class NodeSerializer : ISerializer<Node>
        {
            readonly ISerializer<int> _intSerializer = VariantNumberSerializer.Instance;
            readonly ISerializer<bool> _boolSerializer = PrimitiveSerializer.Instance;
            private readonly ISerializer<IStorageHandle> _storageHandleSerializer;
            private readonly NodeHandleSerializer _handleSerializer;
            private readonly BPlusTreeOptions<TKey, TValue> _options;
            private readonly ISerializer<TKey> _keySerializer;
            private readonly ISerializer<TValue> _valueSerializer;

            public NodeSerializer(BPlusTreeOptions<TKey, TValue> options, NodeHandleSerializer handleSerializer)
            {
                _options = options;
                _keySerializer = options.KeySerializer;
                _valueSerializer = options.ValueSerializer;
                _handleSerializer = handleSerializer;
                _storageHandleSerializer = handleSerializer;
            }

            void ISerializer<Node>.WriteTo(Node value, Stream stream)
            {
                _handleSerializer.WriteTo(value.StorageHandle, stream);

                bool isLeaf = value.IsLeaf;
                int maximumKeys = value.IsRoot ? 1 : (isLeaf ? _options.MaximumValueNodes : _options.MaximumChildNodes);
                Assert(value.Size == maximumKeys);

                _boolSerializer.WriteTo(isLeaf, stream);
                _boolSerializer.WriteTo(value.IsRoot, stream);
                _intSerializer.WriteTo(value.Count, stream);

                for (int i = 0; i < value.Count; i++)
                {
                    Element item = value[i];

                    if (i > 0 || isLeaf)
                    {
                        _keySerializer.WriteTo(item.Key, stream);
                    }
                    if (isLeaf)
                    {
                        _valueSerializer.WriteTo(item.Payload, stream);
                    }
                    else
                    {
                        _handleSerializer.WriteTo(item.ChildNode, stream);
                    }
                }
            }

            public IEnumerable<KeyValuePair<TKey, TValue>> RecoverLeaf(Stream stream)
            {
                _storageHandleSerializer.ReadFrom(stream);
                bool isLeaf = _boolSerializer.ReadFrom(stream);
                if (isLeaf)
                {
                    /* isRoot */_boolSerializer.ReadFrom(stream);
                    int count = _intSerializer.ReadFrom(stream);

                    for (int i = 0; i < count; i++)
                    {
                        TKey key = _keySerializer.ReadFrom(stream);
                        TValue value = _valueSerializer.ReadFrom(stream);
                        yield return new KeyValuePair<TKey, TValue>(key, value);
                    }
                }
            }

            Node ISerializer<Node>.ReadFrom(Stream stream)
            {
                IStorageHandle handle = _storageHandleSerializer.ReadFrom(stream);

                bool isLeaf = _boolSerializer.ReadFrom(stream);
                bool isRoot = _boolSerializer.ReadFrom(stream);
                int count = _intSerializer.ReadFrom(stream);

                Element[] items = new Element[count];

                for (int i = 0; i < count; i++)
                {
                    TKey key = default(TKey);

                    if (i > 0 || isLeaf)
                        key = _keySerializer.ReadFrom(stream);
                    if (isLeaf)
                        items[i] = new Element(key, _valueSerializer.ReadFrom(stream));
                    else
                        items[i] = new Element(key, _handleSerializer.ReadFrom(stream));
                }

                int nodeSize = isLeaf ? _options.MaximumValueNodes : _options.MaximumChildNodes;
                Assert(nodeSize >= count);
                Node resurrected = Node.FromElements(handle, isRoot, nodeSize, items);
                return resurrected;
            }
        }
    }
}
