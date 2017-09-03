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


using CSharpTest.Net.Collections.Exceptions;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary> performs a perfect cache of the entire tree </summary>
        private sealed class NodeCacheFull : NodeCacheBase
        {
            private NodeHandle _root;

            public NodeCacheFull(BPlusTreeOptions<TKey, TValue> options)
                : base(options)
            {
            }

            protected override NodeHandle RootHandle => _root;

            protected override void LoadStorage()
            {
                _root = new NodeHandle(Storage.OpenRoot(out var isNew));
                _root.SetCacheEntry(new CacheWrapper());

                if (isNew)
                    CreateRoot(_root);

                if (Storage.TryGetNode(_root.StoreHandle, out var rootNode, NodeSerializer))
                    _root.SetCacheEntry(new CacheWrapper(rootNode));

                AssertionFailedException.Assert(rootNode != null, "Unable to load storage root.");
            }

            public override void ResetCache()
            {
                CacheWrapper entry;
                if (_root.TryGetCache(out entry))
                    entry.Node = null;
            }

            public override void UpdateNode(NodePin node)
            {
                if (!node.IsDeleted)
                {
                    CacheWrapper entry;
                    if (!node.Handle.TryGetCache(out entry))
                        throw new AssertionFailedException("Unable to retrieve handle cache.");
                    entry.Node = node.Ptr;
                }
            }

            public override void CreateLock(NodeHandle handle, out object refobj)
            {
                CacheWrapper entry = new CacheWrapper();
                handle.SetCacheEntry(entry);
                refobj = entry;
            }

            protected override NodePin Lock(LockType ltype, NodeHandle child)
            {
                CacheWrapper entry;
                if (!child.TryGetCache(out entry))
                    child.SetCacheEntry(entry = new CacheWrapper());

                if (entry.Node == null)
                    Storage.TryGetNode(child.StoreHandle, out entry.Node, NodeSerializer);

                AssertionFailedException.Assert(entry.Node != null);
                return new NodePin(child, ltype, entry, entry.Node, null);
            }

            private class CacheWrapper
            {
                public Node Node;

                public CacheWrapper(Node node = null)
                {
                    Node = node;
                }
            }
        }
    }
}