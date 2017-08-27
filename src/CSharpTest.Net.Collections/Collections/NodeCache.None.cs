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
        private sealed class NodeCacheNone : NodeCacheBase
        {
            private NodeHandle _root;

            public NodeCacheNone(BPlusTreeOptions<TKey, TValue> options)
                : base(Fix(options))
            {
            }

            protected override NodeHandle RootHandle => _root;

            private static BPlusTreeOptions<TKey, TValue> Fix(BPlusTreeOptions<TKey, TValue> options)
            {
                return options;
            }

            protected override void LoadStorage()
            {
                _root = new NodeHandle(Storage.OpenRoot(out var isNew));
                if (isNew)
                    CreateRoot(_root);

                Storage.TryGetNode(_root.StoreHandle, out var rootNode, NodeSerializer);

                AssertionFailedException.Assert(rootNode != null, "Unable to load storage root.");
            }

            public override void ResetCache()
            {
            }

            public override void UpdateNode(NodePin node)
            {
            }

            public override void CreateLock(NodeHandle handle, out object refobj)
            {
                refobj = null;
            }

            protected override NodePin Lock(LockType ltype, NodeHandle child)
            {
                var success = Storage.TryGetNode(child.StoreHandle, out var node, NodeSerializer);
                AssertionFailedException.Assert(success && node != null);

                return new NodePin(child, ltype, ltype, node, null);
            }
        }
    }
}