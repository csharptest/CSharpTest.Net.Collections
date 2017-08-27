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
                bool isNew;
                _root = new NodeHandle(Storage.OpenRoot(out isNew));
                _root.SetCacheEntry(new NodeWithLock(null));
                if (isNew)
                    CreateRoot(_root);

                Node rootNode;
                if (Storage.TryGetNode(_root.StoreHandle, out rootNode, NodeSerializer))
                    _root.SetCacheEntry(new NodeWithLock(rootNode));

                AssertionFailedException.Assert(rootNode != null, "Unable to load storage root.");
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

            public override void CreateLock(NodeHandle handle, out object refobj)
            {
                NodeWithLock nlck = new NodeWithLock(null);
                handle.SetCacheEntry(nlck);
                refobj = nlck;
            }

            protected override NodePin Lock(NodePin parent, LockType ltype, NodeHandle child)
            {
                NodeWithLock nlck;
                if (!child.TryGetCache(out nlck))
                    child.SetCacheEntry(nlck = new NodeWithLock(null));

                if (nlck.Node == null)
                {
                    Storage.TryGetNode(child.StoreHandle, out nlck.Node, NodeSerializer);
                }

                AssertionFailedException.Assert(nlck.Node != null);
                return new NodePin(child, ltype, nlck, nlck.Node, null);
            }

            private class NodeWithLock
            {
                public Node Node;

                public NodeWithLock(Node node)
                {
                    Node = node;
                }
            }
        }
    }
}