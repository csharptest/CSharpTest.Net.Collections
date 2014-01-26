#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;
using System.Threading;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        class StorageCache : INodeStorage, ITransactable, INodeStoreWithCount
        {
            private readonly INodeStorage _store;
            private readonly LurchTable<IStorageHandle, object> _cache, _dirty;

            private readonly ThreadStart _writeBehindFunc;
            private IAsyncResult _asyncWriteBehind;
            private readonly int _asyncThreshold;

            ISerializer<Node> _serializer;

            public StorageCache(INodeStorage store, int sizeLimit)
            {
                _asyncThreshold = 50;
                _writeBehindFunc = Flush;
                _asyncWriteBehind = null;

                _store = store;
                _cache = new LurchTable<IStorageHandle, object>(  LurchTableOrder.Access, sizeLimit, 1000000, sizeLimit >> 4, 1000, EqualityComparer<IStorageHandle>.Default);
                _dirty = new LurchTable<IStorageHandle, object>(LurchTableOrder.Modified, sizeLimit, 1000000, sizeLimit >> 4, 1000, EqualityComparer<IStorageHandle>.Default);
                _dirty.ItemRemoved += OnItemRemoved;
            }

            public void Dispose()
            {
                using(_cache)
                {
                    _dirty.ItemRemoved -= OnItemRemoved;
                    ClearCache();
                    _store.Dispose();
                }
            }

            private void ClearCache()
            {
                lock (_writeBehindFunc)
                {
                    CompleteAsync();
                    _cache.Clear();
                    _dirty.Clear();
                }
            }

            public int Count
            {
                get
                {
                    INodeStoreWithCount tstore = _store as INodeStoreWithCount;
                    return (tstore != null) ? tstore.Count : -1;
                }
                set
                {
                    INodeStoreWithCount tstore = _store as INodeStoreWithCount;
                    if (tstore != null)
                        tstore.Count = value;
                }
            }

            public void Commit()
            {
                lock (_writeBehindFunc)
                {
                    CompleteAsync();

                    Flush();

                    ITransactable tstore = _store as ITransactable;
                    if (tstore != null)
                        tstore.Commit();
                }
            }

            private void CompleteAsync()
            {
                var completion = _asyncWriteBehind;
                if (completion != null && !completion.IsCompleted)
                {
                    _asyncWriteBehind = null;
                    _writeBehindFunc.EndInvoke(completion);
                }
            }

            public void Rollback()
            {
                ITransactable tstore = _store as ITransactable;
                if (tstore != null)
                {
                    _serializer = null;
                    ClearCache();
                    tstore.Rollback();
                }
            }

            public void Reset()
            {
                _serializer = null;
                ClearCache();
                _store.Reset();
            }

            public IStorageHandle OpenRoot(out bool isNew)
            {
                return _store.OpenRoot(out isNew);
            }

            public IStorageHandle Create()
            {
                return _store.Create();
            }

            struct FetchFromStore<TNode> : ICreateOrUpdateValue<IStorageHandle, object>
            {
                public INodeStorage Storage;
                public ISerializer<TNode> Serializer;
                public LurchTable<IStorageHandle, object> DirtyCache;
                public TNode Value;
                public bool Success;

                public bool CreateValue(IStorageHandle key, out object value)
                {
                    if (DirtyCache.TryGetValue(key, out value) && value != null)
                    {
                        Success = true;
                        Value = (TNode)value;
                        return true;
                    }

                    Success = Storage.TryGetNode(key, out Value, Serializer);
                    if (Success)
                    {
                        value = Value;
                        return true;
                    }

                    value = null;
                    return false;
                }
                public bool UpdateValue(IStorageHandle key, ref object value)
                {
                    Success = value != null;
                    Value = (TNode)value;
                    return false;
                }
            }

            public bool TryGetNode<TNode>(IStorageHandle handle, out TNode tnode, ISerializer<TNode> serializer)
            {
                if (_serializer == null) _serializer = (ISerializer<Node>) serializer;

                var fetch = new FetchFromStore<TNode>
                                {
                                    DirtyCache = _dirty,
                                    Storage = _store, 
                                    Serializer = serializer,
                                };

                _cache.AddOrUpdate(handle, ref fetch);
                if (fetch.Success)
                {
                    tnode = fetch.Value;
                    return true;
                }
                tnode = default(TNode);
                return false;
            }

            public void Update<TNode>(IStorageHandle handle, ISerializer<TNode> serializer, TNode tnode)
            {
                if (_serializer == null) _serializer = (ISerializer<Node>)serializer;

                _cache[handle] = tnode;
                _dirty[handle] = tnode;

                var completion = _asyncWriteBehind;
                if (_dirty.Count > _asyncThreshold && (completion == null || completion.IsCompleted))
                {
                    lock (_writeBehindFunc)
                    {
                        if (completion == null || completion.IsCompleted)
                            _asyncWriteBehind = _writeBehindFunc.BeginInvoke(null, null);
                    }
                }
            }

            public void Destroy(IStorageHandle handle)
            {
                _dirty[handle] = null;
                _dirty.Remove(handle);
                _cache.Remove(handle);
                _store.Destroy(handle);
            }

            void OnItemRemoved(KeyValuePair<IStorageHandle, object> item)
            {
                var ser = _serializer;
                if (ser != null && item.Value != null)
                    _store.Update(item.Key, ser, (Node)item.Value);
            }

            private void Flush()
            {
                try
                {
                    KeyValuePair<IStorageHandle, object> value;
                    while (_dirty.TryDequeue(out value))
                    { }
                }
                finally
                {
                    _asyncWriteBehind = null;
                }
            }

            IStorageHandle ISerializer<IStorageHandle>.ReadFrom(System.IO.Stream stream)
            {
                return _store.ReadFrom(stream);
            }

            void ISerializer<IStorageHandle>.WriteTo(IStorageHandle value, System.IO.Stream stream)
            {
                _store.WriteTo(value, stream);
            }
        }
    }
}
