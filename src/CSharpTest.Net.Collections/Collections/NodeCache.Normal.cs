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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

using CSharpTest.Net.Utils;

// ReSharper disable MemberHidesStaticFromOuterClass

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary>
        ///     This is the default cache type, uses weakreferences and the GC to collect unused nodes after they exit
        ///     the ObjectKeepAlive containment policy.
        /// </summary>
        private sealed class NodeCacheNormal : NodeCacheBase
        {
            private readonly ConcurrentDictionary<NodeHandle, Utils.WeakReference<CacheEntry>> _cache;
            private readonly IObjectKeepAlive _keepAlive;
            private bool _disposed;
            private CacheEntry _root;

            public NodeCacheNormal(BPlusTreeOptions<TKey, TValue> options) : base(options)
            {
                _keepAlive = options.CreateCacheKeepAlive();
                _cache = new ConcurrentDictionary<NodeHandle, Utils.WeakReference<CacheEntry>>();
            }

            protected override NodeHandle RootHandle => _root.Handle;

            protected override void Dispose(bool disposing)
            {
                _disposed = true;
                if (disposing)
                {
                    _keepAlive.Clear();
                    _cache.Clear();
                }
                base.Dispose(disposing);
            }

            protected override void LoadStorage()
            {
                _keepAlive.Clear();
                _cache.Clear();

                bool isNew;
                NodeHandle rootHandle = new NodeHandle(Storage.OpenRoot(out isNew));
                _root = GetCache(rootHandle, true);
                if (isNew)
                    _root.Node = CreateRoot(_root.Handle);

                Storage.TryGetNode(rootHandle.StoreHandle, out _root.Node, NodeSerializer);
                AssertionFailedException.Assert(_root.Node != null, "Unable to load storage root.");
            }

            public override void ResetCache()
            {
                _keepAlive.Clear();
                _cache.Clear();
                _root = GetCache(_root.Handle, true);

                bool isnew;
                AssertionFailedException.Assert(_root.Handle.StoreHandle.Equals(Storage.OpenRoot(out isnew)));
                if (isnew)
                    _root.Node = CreateRoot(_root.Handle);
            }

            private CacheEntry GetCache(NodeHandle handle, bool isNew)
            {
                Utils.WeakReference<CacheEntry> weakRef;
                CacheEntry entry = null;

                if (!isNew)
                {
                    if (handle.TryGetCache(out weakRef) && weakRef != null && weakRef.TryGetTarget(out entry))
                        return entry;


                    if (_cache.TryGetValue(handle, out weakRef))
                        if (!weakRef.TryGetTarget(out entry))
                        {
                            if (!weakRef.TryGetTarget(out entry))
                                weakRef.Target = entry = new CacheEntry(this, handle);
                            handle.SetCacheEntry(weakRef);
                        }
                }
                if (entry == null)
                {
                    if (!_cache.TryGetValue(handle, out weakRef))
                    {
                        _cache.TryAdd(handle, weakRef = new Utils.WeakReference<CacheEntry>(entry = new CacheEntry(this, handle)));
                        handle.SetCacheEntry(weakRef);
                    }
                    else
                    {
                        if (!weakRef.TryGetTarget(out entry))
                        {
                            if (!weakRef.TryGetTarget(out entry))
                                weakRef.Target = entry = new CacheEntry(this, handle);
                            handle.SetCacheEntry(weakRef);
                        }
                    }
                }
                AssertionFailedException.Assert(entry != null, "Cache entry is null");
                _keepAlive.Add(entry);
                return entry;
            }

            public override void CreateLock(NodeHandle handle, out object refobj)
            {
                CacheEntry entry = GetCache(handle, true);
                refobj = entry;
            }

            protected override NodePin Lock(LockType ltype, NodeHandle child)
            {
                CacheEntry entry = GetCache(child, false);

                Node node = entry.Node;
                if (node == null)
                {
                    node = entry.Node;
                    if (node == null)
                    {
                        InvalidNodeHandleException.Assert(
                            Storage.TryGetNode(child.StoreHandle, out node, NodeSerializer)
                            && node != null
                            && node.StorageHandle.Equals(entry.Handle.StoreHandle)
                        );
                        Node old = Interlocked.CompareExchange(ref entry.Node, node, null);
                        AssertionFailedException.Assert(null == old, "Collision on cache load.");
                    }
                }
                return new NodePin(child, ltype, entry, node, null);
            }

            public override void UpdateNode(NodePin node)
            {
                if (ReferenceEquals(node.Original, node.Ptr))
                    return;

                CacheEntry entry = node.Reference as CacheEntry;
                if (entry == null)
                    throw new AssertionFailedException("Invalid node pin in update.");

                if (node.IsDeleted)
                {
                    AssertionFailedException.Assert(node.LockType != LockType.Read);
                    //With lockless-reads we leave instances in cache until GC collects, otherwise we could remove them.
                    //using (node.Lock.Write(Options.LockTimeout))
                    //    node.Original.Invalidate();
                    //entry.Node = null;
                }
                else if (node.Ptr.IsRoot && _root.Node == null)
                {
                    _root.Node = node.Ptr;
                }
                else
                {
                    Node old = Interlocked.CompareExchange(ref entry.Node, node.Ptr, node.Original);
                    AssertionFailedException.Assert(ReferenceEquals(old, node.Original), "Node was modified without lock");
                }
            }

            [DebuggerDisplay("{Handle} = {Node}")]
            private class CacheEntry
            {
                public readonly NodeHandle Handle;

                private NodeCacheNormal _owner;
                public Node Node;

                public CacheEntry(NodeCacheNormal owner, NodeHandle handle)
                {
                    Handle = handle;
                    _owner = owner;
                }

                ~CacheEntry()
                {
                    if (!_owner._disposed)
                    {
                        try
                        {
                            if (_owner._cache.TryGetValue(Handle, out var me) && me.IsAlive == false)
                                _owner._cache.TryRemove(Handle, out _);
                        }
                        catch (ObjectDisposedException)
                        {
                        }
                    }
                    Node = null;
                    _owner = null;
                }
            }
        }
    }
}