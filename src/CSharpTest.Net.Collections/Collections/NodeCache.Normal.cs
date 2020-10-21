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
using System.Threading;
using CSharpTest.Net.Synchronization;
using CSharpTest.Net.Utils;
// ReSharper disable MemberHidesStaticFromOuterClass

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary>
        /// This is the default cache type, uses weakreferences and the GC to collect unused nodes after they exit
        /// the ObjectKeepAlive containment policy.
        /// </summary>
        sealed class NodeCacheNormal : NodeCacheBase
        {
            [System.Diagnostics.DebuggerDisplay("{Handle} = {Node}")]
            class CacheEntry
            {
                private NodeCacheNormal _owner;
                public CacheEntry(NodeCacheNormal owner, NodeHandle handle)
                {
                    Lock = owner.LockFactory.Create();
                    Handle = handle;
                    _owner = owner;
                }
                ~CacheEntry()
                {
                    Lock.Dispose();
                    if (!_owner._disposed)
                    {
                        try
                        {
                            using (_owner._cacheLock.Write(_owner.Options.LockTimeout))
                            {
                                Utils.WeakReference<CacheEntry> me;
                                if (_owner._cache.TryGetValue(Handle, out me) && me.IsAlive == false)
                                    _owner._cache.Remove(Handle);
                            }
                        }
                        catch (ObjectDisposedException)
                        { }
                    }
                    Node = null;
                    _owner = null;
                }

                public readonly ILockStrategy Lock;
                public readonly NodeHandle Handle;
                public Node Node;
            }
            private readonly IObjectKeepAlive _keepAlive;

            private readonly Dictionary<NodeHandle, Utils.WeakReference<CacheEntry>> _cache;
            private bool _disposed;
            private CacheEntry _root;
            private ILockStrategy _cacheLock;

            public NodeCacheNormal(BPlusTreeOptions<TKey, TValue> options) : base(options)
            {
                _keepAlive = options.CreateCacheKeepAlive();
                _cache = new Dictionary<NodeHandle, Utils.WeakReference<CacheEntry>>();
                _cacheLock = new ReaderWriterLocking();
            }

            protected override NodeHandle RootHandle { get { return _root.Handle; } }

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
                Assert(_root.Node != null, "Unable to load storage root.");
            }

            public override void ResetCache()
            {
                _keepAlive.Clear();
                _cache.Clear();
                _cacheLock = new ReaderWriterLocking();
                _root = GetCache(_root.Handle, true);

                bool isnew;
                Assert(_root.Handle.StoreHandle.Equals(Storage.OpenRoot(out isnew)));
                if (isnew)
                    _root.Node = CreateRoot(_root.Handle);
            }

            CacheEntry GetCache(NodeHandle handle, bool isNew)
            {
                Utils.WeakReference<CacheEntry> weakRef;
                CacheEntry entry = null;

                if (!isNew)
                {
                    if (handle.TryGetCache(out weakRef) && weakRef != null && weakRef.TryGetTarget(out entry))
                        return entry;

                    using (_cacheLock.Read(base.Options.LockTimeout))
                    {
                        if (_cache.TryGetValue(handle, out weakRef))
                        {
                            if (!weakRef.TryGetTarget(out entry))
                                using (new SafeLock<DeadlockException>(weakRef))
                                {
                                    if (!weakRef.TryGetTarget(out entry))
                                        weakRef.Target = entry = new CacheEntry(this, handle);
                                    handle.SetCacheEntry(weakRef);
                                }
                        }
                    }
                }
                if (entry == null)
                {
                    using (_cacheLock.Write(base.Options.LockTimeout))
                    {
                        if (!_cache.TryGetValue(handle, out weakRef))
                        {
                            _cache.Add(handle, weakRef = new Utils.WeakReference<CacheEntry>(entry = new CacheEntry(this, handle)));
                            handle.SetCacheEntry(weakRef);
                        }
                        else
                        {
                            if (!weakRef.TryGetTarget(out entry))
                                using (new SafeLock<DeadlockException>(weakRef))
                                {
                                    if (!weakRef.TryGetTarget(out entry))
                                        weakRef.Target = entry = new CacheEntry(this, handle);
                                    handle.SetCacheEntry(weakRef);
                                }
                        }
                    }
                }
                Assert(entry != null, "Cache entry is null");
                _keepAlive.Add(entry);
                return entry;
            }

            public override ILockStrategy CreateLock(NodeHandle handle, out object refobj)
            {
                CacheEntry entry = GetCache(handle, true);
                bool locked = entry.Lock.TryWrite(base.Options.LockTimeout);
                Assert(locked);
                refobj = entry;
                return entry.Lock;
            }

            protected override NodePin Lock(NodePin parent, LockType ltype, NodeHandle child)
            {
                return LockInternal(parent, ltype, child, false);
            }

            private NodePin LockInternal(NodePin parent, LockType ltype, NodeHandle child, bool ignoreHandleComparison)
            {
                CacheEntry entry = GetCache(child, false);

                LockType locked = NoLock;
                if (ltype == LockType.Read && entry.Lock.TryRead(base.Options.LockTimeout))
                    locked = LockType.Read;
                if (ltype != LockType.Read && entry.Lock.TryWrite(base.Options.LockTimeout))
                    locked = ltype;

                DeadlockException.Assert(locked != NoLock);
                try
                {
                    Node node = entry.Node;
                    if (node == null)
                    {
                        using (new SafeLock<DeadlockException>(entry))
                        {
                            node = entry.Node;
                            if (node == null)
                            {
                                InvalidNodeHandleException.Assert(
                                    Storage.TryGetNode(child.StoreHandle, out node, NodeSerializer) && node != null &&
                                    ignoreHandleComparison
                                        ? true
                                        : node.StorageHandle.Equals(entry.Handle.StoreHandle));
                               


                                Node old = Interlocked.CompareExchange(ref entry.Node, node, null);
                                Assert(null == old, "Collision on cache load.");
                            }
                        }
                    }
                    return new NodePin(child, entry.Lock, ltype, locked, entry, node, null);
                }
                catch
                {
                    if (locked == LockType.Read)
                        entry.Lock.ReleaseRead();
                    else if (locked != NoLock)
                        entry.Lock.ReleaseWrite();
                    throw;
                }
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
                    Assert(node.LockType != LockType.Read);
                    //With lockless-reads we leave instances in cache until GC collects, otherwise we could remove them.
                    //using (node.Lock.Write(Options.LockTimeout))
                    //    node.Original.Invalidate();
                    //entry.Node = null;
                }
                else if (node.Ptr.IsRoot && _root.Node == null)
                    _root.Node = node.Ptr;
                else
                {
                    Node old = Interlocked.CompareExchange(ref entry.Node, node.Ptr, node.Original);
                    Assert(ReferenceEquals(old, node.Original), "Node was modified without lock");
                }
            }

            public override NodePin LockRoot(LockType ltype)
            {
                return LockInternal(null, ltype, RootHandle, true);
            }
        }
    }
}
