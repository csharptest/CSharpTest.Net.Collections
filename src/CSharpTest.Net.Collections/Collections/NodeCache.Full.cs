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
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary> performs a perfect cache of the entire tree </summary>
        sealed class NodeCacheFull : NodeCacheBase
        {
            NodeHandle _root;
            public NodeCacheFull(BPlusTreeOptions<TKey, TValue> options)
                : base(options)
            {}

            protected override void LoadStorage()
            {
                bool isNew;
                _root = new NodeHandle(Storage.OpenRoot(out isNew));
                _root.SetCacheEntry(new NodeWithLock(null, LockFactory.Create()));
                if (isNew)
                    CreateRoot(_root);

                Node rootNode;
                if (Storage.TryGetNode(_root.StoreHandle, out rootNode, NodeSerializer))
                    _root.SetCacheEntry(new NodeWithLock(rootNode, LockFactory.Create()));

                Assert(rootNode != null, "Unable to load storage root.");
            }

            protected override NodeHandle RootHandle
            {
                get { return _root; }
            }

            public override void ResetCache()
            {
                NodeWithLock nlck;
                if (_root.TryGetCache(out nlck))
                    nlck.Node = null;
            }

            public override void UpdateNode(NodePin node)
            {
                if (!node.IsDeleted)
                {
                    NodeWithLock nlck;
                    if (!node.Handle.TryGetCache(out nlck))
                        throw new AssertionFailedException("Unable to retrieve handle cache.");
                    nlck.Node = node.Ptr;
                }
            }

            public override ILockStrategy CreateLock(NodeHandle handle, out object refobj)
            {
                NodeWithLock nlck = new NodeWithLock(null, LockFactory.Create());
                handle.SetCacheEntry(nlck);
                refobj = nlck;
                bool acquired = nlck.Lock.TryWrite(base.Options.LockTimeout);
                DeadlockException.Assert(acquired);
                return nlck.Lock;
            }

            protected override NodePin Lock(NodePin parent, LockType ltype, NodeHandle child)
            {
                NodeWithLock nlck;
                if (!child.TryGetCache(out nlck))
                    child.SetCacheEntry(nlck = new NodeWithLock(null, LockFactory.Create()));

                bool acquired;
                if(ltype == LockType.Read)
                    acquired = nlck.Lock.TryRead(base.Options.LockTimeout);
                else
                    acquired = nlck.Lock.TryWrite(base.Options.LockTimeout);
                DeadlockException.Assert(acquired);
                try
                {
                    if (nlck.Node == null)
                    {
                        using (new SafeLock<DeadlockException>(nlck, base.Options.LockTimeout))
                            Storage.TryGetNode(child.StoreHandle, out nlck.Node, NodeSerializer);
                    }

                    Assert(nlck.Node != null);
                    return new NodePin(child, nlck.Lock, ltype, ltype, nlck, nlck.Node, null);
                }
                catch
                {
                    if (ltype == LockType.Read)
                        nlck.Lock.ReleaseRead();
                    else
                        nlck.Lock.ReleaseWrite();
                    throw;
                }
            }

            class NodeWithLock
            {
                public readonly ILockStrategy Lock;
                public Node Node;

                public NodeWithLock(Node node, ILockStrategy lck)
                {
                    Node = node;
                    Lock = lck;
                }
            }
        }
    }
}
