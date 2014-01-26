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
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary> performs a perfect cache of the entire tree </summary>
        sealed class NodeCacheNone : NodeCacheBase
        {
            readonly Dictionary<IStorageHandle, ILockStrategy> _list;
            readonly ILockStrategy _lock;
            NodeHandle _root;

            public NodeCacheNone(BPlusTreeOptions<TKey, TValue> options)
                : base(Fix(options))
            {
                _lock = new ReaderWriterLocking();
                _list = new Dictionary<IStorageHandle, ILockStrategy>();
            }

            static BPlusTreeOptions<TKey, TValue> Fix(BPlusTreeOptions<TKey, TValue> options)
            {
                return options;
            }

            protected override void LoadStorage()
            {
                bool isNew;
                _root = new NodeHandle(Storage.OpenRoot(out isNew));
                _root.SetCacheEntry(LockFactory.Create());
                if (isNew)
                    CreateRoot(_root);

                Node rootNode;
                if (Storage.TryGetNode(_root.StoreHandle, out rootNode, NodeSerializer))
                    _root.SetCacheEntry(LockFactory.Create());

                Assert(rootNode != null, "Unable to load storage root.");
            }

            protected override NodeHandle RootHandle
            {
                get { return _root; }
            }

            public override void ResetCache()
            {
                _list.Clear();
            }

            public override void UpdateNode(NodePin node)
            {
                if (node.IsDeleted)
                    using (_lock.Write(base.Options.LockTimeout))
                        _list.Remove(node.Handle.StoreHandle);
            }

            public override ILockStrategy CreateLock(NodeHandle handle, out object refobj)
            {
                ILockStrategy lck;
                using (_lock.Write(base.Options.LockTimeout))
                {
                    if (!_list.TryGetValue(handle.StoreHandle, out lck))
                    {
                        _list.Add(handle.StoreHandle, lck = LockFactory.Create());
                        handle.SetCacheEntry(lck);
                    }
                }

                refobj = null;
                bool acquired = lck.TryWrite(base.Options.LockTimeout);
                DeadlockException.Assert(acquired);
                return lck;
            }

            protected override NodePin Lock(NodePin parent, LockType ltype, NodeHandle child)
            {
                ILockStrategy lck;
                if (!child.TryGetCache(out lck))
                {
                    using (_lock.Write(base.Options.LockTimeout))
                    {
                        if (!_list.TryGetValue(child.StoreHandle, out lck))
                        {
                            _list.Add(child.StoreHandle, lck = LockFactory.Create());
                            child.SetCacheEntry(lck);
                        }
                    }
                }

                bool success;
                if (ltype == LockType.Read)
                    success = lck.TryRead(base.Options.LockTimeout);
                else
                    success = lck.TryWrite(base.Options.LockTimeout);
                DeadlockException.Assert(success);
                try
                {
                    Node node;
                    success = Storage.TryGetNode(child.StoreHandle, out node, NodeSerializer);
                    Assert(success && node != null);

                    return new NodePin(child, lck, ltype, ltype, lck, node, null);
                }
                catch
                {
                    if (ltype == LockType.Read)
                        lck.ReleaseRead();
                    else
                        lck.ReleaseWrite();
                    throw;
                }
            }
        }
    }
}
