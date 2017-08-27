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
using System.IO;
using System.Threading;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        private class StorageCache : INodeStorage, ITransactable, INodeStoreWithCount
        {
            private readonly int _asyncThreshold;
            private readonly LurchTable<IStorageHandle, object> _cache, _dirty;

            private readonly object _flushSync;
            private readonly INodeStorage _store;

            private ISerializer<Node> _serializer;

            public StorageCache(INodeStorage store, int sizeLimit)
            {
                _flushSync = new object();
                _asyncThreshold = 50;

                _store = store;
                _cache = new LurchTable<IStorageHandle, object>(LurchTableOrder.Access, sizeLimit, 1000000,
                    sizeLimit >> 4, 1000, EqualityComparer<IStorageHandle>.Default);
                _dirty = new LurchTable<IStorageHandle, object>(LurchTableOrder.Modified, sizeLimit, 1000000,
                    sizeLimit >> 4, 1000, EqualityComparer<IStorageHandle>.Default);
                _dirty.ItemRemoved += OnItemRemoved;
            }

            public void Dispose()
            {
                using (_cache)
                {
                    lock (_flushSync) // disallow concurrent async flush
                    {
                        _dirty.ItemRemoved -= OnItemRemoved;
                        ClearCache();
                        _store.Dispose();
                    }
                }
            }

            public void Reset()
            {
                lock (_flushSync) // disallow concurrent async flush
                {
                    _serializer = null;
                    ClearCache();
                    _store.Reset();
                }
            }

            public IStorageHandle OpenRoot(out bool isNew)
            {
                return _store.OpenRoot(out isNew);
            }

            public IStorageHandle Create()
            {
                return _store.Create();
            }

            public bool TryGetNode<TNode>(IStorageHandle handle, out TNode tnode, ISerializer<TNode> serializer)
            {
                if (_serializer == null) _serializer = (ISerializer<Node>)serializer;

                FetchFromStore<TNode> fetch = new FetchFromStore<TNode>
                {
                    DirtyCache = _dirty,
                    Storage = _store,
                    Serializer = serializer
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

                if (_dirty.Count > _asyncThreshold )//&& (completion == null || completion.IsCompleted))
                {
                    try
                    {
                    }
                    finally
                    {
                        bool locked = Monitor.TryEnter(_flushSync);
                        try
                        {
                            if (locked)
                            {
                                //completion = _asyncWriteBehind;
                                //if (completion == null || completion.IsCompleted)
                                //    _asyncWriteBehind = _writeBehindFunc.BeginInvoke(null, null);
                                Flush();
                            }
                        }
                        finally
                        {
                            if (locked)
                                Monitor.Exit(_flushSync);
                        }
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

            IStorageHandle ISerializer<IStorageHandle>.ReadFrom(Stream stream)
            {
                return _store.ReadFrom(stream);
            }

            void ISerializer<IStorageHandle>.WriteTo(IStorageHandle value, Stream stream)
            {
                _store.WriteTo(value, stream);
            }

            public int Count
            {
                get
                {
                    INodeStoreWithCount tstore = _store as INodeStoreWithCount;
                    return tstore != null ? tstore.Count : -1;
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
                lock (_flushSync) // disallow concurrent async flush
                {
                    Flush();

                    ITransactable tstore = _store as ITransactable;
                    if (tstore != null)
                        tstore.Commit();
                }
            }

            public void Rollback()
            {
                ITransactable tstore = _store as ITransactable;
                if (tstore != null)
                    lock (_flushSync) // disallow concurrent async flush
                    {
                        _serializer = null;
                        ClearCache();
                        tstore.Rollback();
                    }
            }

            // Must SYNC on lock (_flushSync)
            private void ClearCache()
            {
                _cache.Clear();
                _dirty.Clear();
            }

            private void OnItemRemoved(KeyValuePair<IStorageHandle, object> item)
            {
                ISerializer<Node> ser = _serializer;
                if (ser != null && item.Value != null)
                    _store.Update(item.Key, ser, (Node)item.Value);
            }

            private void Flush()
            {
                lock (_flushSync) // disallow concurrent async flush
                {
                    try
                    {
                        KeyValuePair<IStorageHandle, object> value;
                        while (_dirty.TryDequeue(out value))
                        {
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            }

            private struct FetchFromStore<TNode> : ICreateOrUpdateValue<IStorageHandle, object>
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
        }
    }
}