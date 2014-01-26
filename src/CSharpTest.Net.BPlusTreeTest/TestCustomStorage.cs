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
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class TestCustomStorage : BasicTests
    {
        protected override BPlusTreeOptions<int, string> Options
        {
            get
            {
                return new BPlusTree<int, string>.Options(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    BTreeOrder = 4,
                    StorageSystem = new MyNodeStore()
                };
            }
        }

        [Test]
        public void TestStorageSystemOption()
        {
            BPlusTreeOptions<int, string> options = Options;
            Assert.AreEqual(StorageType.Custom, options.StorageType);
        }

        [Test, ExpectedException(typeof(InvalidOperationException))]
        public void TestFailedWrite()
        {
            BPlusTreeOptions<int, string> options = Options;
            using (BPlusTree<int, string> tree = new BPlusTree<int, string>(options))
            {
                for(int i= 0; i < 10; i++ )
                    tree[i] = i.ToString();

                ((MyNodeStore)options.StorageSystem).ReadOnly = true;
                tree.Add(50, string.Empty);
            }
        }

        /// <summary>
        /// This is the most primitive example of a serializing node store...
        /// </summary>
        class MyNodeStore : INodeStorage
        {
            int _nextId;
            readonly Dictionary<int, byte[]> _data;
            public bool ReadOnly;

            public MyNodeStore()
            {
                _nextId = 1;
                _data = new Dictionary<int, byte[]>();
            }

            void IDisposable.Dispose()
            { _data.Clear(); }

            IStorageHandle INodeStorage.OpenRoot(out bool isNew)
            {
                isNew = _data.Count == 0;
                return new Handle(0);
            }

            IStorageHandle INodeStorage.Create()
            {
                return new Handle(_nextId++);
            }

            void INodeStorage.Destroy(IStorageHandle handle)
            {
                _data.Remove(((Handle)handle).Id);
            }

            void INodeStorage.Reset()
            {
                _data.Clear();
            }

            bool INodeStorage.TryGetNode<TNode>(IStorageHandle handle, out TNode node, ISerializer<TNode> serializer)
            {
                byte[] bytes;
                if (_data.TryGetValue(((Handle)handle).Id, out bytes))
                {
                    using (MemoryStream ms = new MemoryStream(bytes, false))
                        node = serializer.ReadFrom(ms);
                    return true;
                }

                node = default(TNode);
                return false;
            }

            void INodeStorage.Update<TNode>(IStorageHandle handle, ISerializer<TNode> serializer, TNode node)
            {
                Check.Assert<InvalidOperationException>(ReadOnly == false);
                using (MemoryStream ms = new MemoryStream())
                {
                    serializer.WriteTo(node, ms);
                    _data[((Handle)handle).Id] = ms.ToArray();
                }
            }

            IStorageHandle ISerializer<IStorageHandle>.ReadFrom(System.IO.Stream stream)
            {
                return new Handle(PrimitiveSerializer.Int32.ReadFrom(stream));
            }

            void ISerializer<IStorageHandle>.WriteTo(IStorageHandle value, System.IO.Stream stream)
            {
                PrimitiveSerializer.Int32.WriteTo(((Handle)value).Id, stream);
            }

            class Handle : IStorageHandle
            {
                public readonly Int32 Id;
                public Handle(Int32 id)
                {
                    Id = id;
                }

                public bool Equals(IStorageHandle other)
                {
                    return ((Handle)other).Id == Id;
                }
            }
        }
    }
}
