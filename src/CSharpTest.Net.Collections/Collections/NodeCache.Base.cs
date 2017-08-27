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
using CSharpTest.Net.Serialization;


namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary> Provides base functionality of a node cache, not much exciting here </summary>
        private abstract class NodeCacheBase : IDisposable
        {
            public readonly ISerializer<Node> NodeSerializer;
            public readonly BPlusTreeOptions<TKey, TValue> Options;
            public readonly INodeStorage Storage;
            private NodeVersion _version;

            public NodeCacheBase(BPlusTreeOptions<TKey, TValue> options)
            {
                Options = options;
                Storage = Options.CreateStorage();
                if (Options.UseStorageCache)
                    Storage = new StorageCache(Storage, Options.CacheKeepAliveMaximumHistory);

                NodeSerializer = new NodeSerializer(Options, new NodeHandleSerializer(Storage));
                _version = new NodeVersion();
            }

            public NodeVersion CurrentVersion => _version;

            protected abstract NodeHandle RootHandle { get; }

            public void Load()
            {
                LoadStorage();
            }

            public void DeleteAll()
            {
                Storage.Reset();
                LoadStorage();
            }

            public void AddVersion(NodeVersion version)
            {
                version.ChainTo(ref _version);
            }

            public void ReturnVersion(ref NodeVersion ver)
            {
                /*if (_version is NodePin) new NodeVersion().ChainTo(ref _version);*/
                GC.KeepAlive(ver);
            }

            protected abstract void LoadStorage();
            public abstract void ResetCache();

            public abstract void UpdateNode(NodePin node);
            public abstract void CreateLock(NodeHandle handle, out object refobj);
            protected abstract NodePin Lock(LockType ltype, NodeHandle child);

            public NodePin LockRoot(LockType ltype)
            {
                return Lock(ltype, RootHandle);
            }

            public NodePin Lock(NodePin parent, NodeHandle child)
            {
                return Lock(parent.LockType, child);
            }


            protected Node CreateRoot(NodeHandle rootHandle)
            {
                NodeHandle hChild;
                using (NodeTransaction t = BeginTransaction())
                {
                    using (NodePin child = t.Create(LockType.Insert, true))
                    {
                        hChild = child.Handle;
                        t.Commit();
                    }
                }

                object refobj;
                RootNode rootNode = new RootNode(rootHandle.StoreHandle);

                 CreateLock(rootHandle, out refobj);
              
                    using (NodePin rootPin = new NodePin(rootHandle, LockType.Insert, refobj,
                        rootNode, null))
                    using (NodeTransaction t = BeginTransaction())
                    {
                        rootNode = (RootNode) t.BeginUpdate(rootPin);
                        rootNode.ReplaceChild(0, null, hChild);
                        t.Commit();
                    }

                return rootNode;
            }

            public NodeTransaction BeginTransaction()
            {
                return new NodeTransaction(this);
            }

            public void SaveChanges(NodePin pin)
            {
                if (pin.IsDeleted)
                    Storage.Destroy(pin.Handle.StoreHandle);
                else
                    Storage.Update(pin.Handle.StoreHandle, NodeSerializer, pin.Ptr);

                UpdateNode(pin);
            }

            #region IDisposable

            ~NodeCacheBase()
            {
                try
                {
                    Dispose(false);
                }
                catch (Exception ex)
                {
                    Trace.TraceError(ex.ToString());
                }
            }

            public void Dispose()
            {
                GC.SuppressFinalize(this);
                Dispose(true);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (Storage != null)
                    Storage.Dispose();
            }

            #endregion
        }
    }
}