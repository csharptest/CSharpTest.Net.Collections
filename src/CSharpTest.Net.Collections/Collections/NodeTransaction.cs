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
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        class NodeTransaction : IDisposable, ITransactable
        {
            readonly NodeCacheBase _cache;
            private NodePin _created, _deleted;
            private NodePin _parentItem;
            bool _disposed, _committed, _reverted;

            private bool _hasLogToken;
            private TransactionToken _logToken;

            public NodeTransaction(NodeCacheBase cache)
            {
                _cache = cache;
                _hasLogToken = false;
                _logToken = new TransactionToken();
            }

            public void AddValue(TKey key, TValue value)
            {
                if(_cache.Options.LogFile != null)
                {
                    if (!_hasLogToken)
                        _logToken = _cache.Options.LogFile.BeginTransaction();
                    _hasLogToken = true;
                    _cache.Options.LogFile.AddValue(ref _logToken, key, value);
                }
            }

            public void UpdateValue(TKey key, TValue value)
            {
                if (_cache.Options.LogFile != null)
                {
                    if (!_hasLogToken)
                        _logToken = _cache.Options.LogFile.BeginTransaction();
                    _hasLogToken = true;
                    _cache.Options.LogFile.UpdateValue(ref _logToken, key, value);
                }
            }

            public void RemoveValue(TKey key)
            {
                if (_cache.Options.LogFile != null)
                {
                    if (!_hasLogToken)
                        _logToken = _cache.Options.LogFile.BeginTransaction();
                    _hasLogToken = true;
                    _cache.Options.LogFile.RemoveValue(ref _logToken, key);
                }
            }


            public NodePin Create(NodePin parent, bool isLeaf)
            { return Create(parent.LockType, isLeaf); }
            public NodePin Create(LockType ltype, bool isLeaf)
            {
                IStorageHandle storeHandle = _cache.Storage.Create();
                NodeHandle handle = new NodeHandle(storeHandle);
                object refobj;
                ILockStrategy lck = _cache.CreateLock(handle, out refobj);

                int size = isLeaf ? _cache.Options.MaximumValueNodes : _cache.Options.MaximumChildNodes;
                NodePin pin = new NodePin(handle, lck, ltype, LockType.Insert, refobj, null, new Node(handle.StoreHandle, size));
                NodePin.Append(ref _created, pin);
                return pin;
            }

            public void Destroy(NodePin pin)
            {
                Assert(pin.LockType != LockType.Read, "Node is not locked for update");
                NodePin.Append(ref _deleted, pin);
                pin.MarkDeleted();
            }

            public Node BeginUpdate(NodePin pin)
            {
                Assert(pin.LockType != LockType.Read, "Node is not locked for update");
                Assert(_parentItem == null, "An update is already in this operation");
                _parentItem = pin;
                pin.BeginUpdate();
                return pin.Ptr;
            }

            public void Commit()
            {
                Assert(_committed == false, "Transaction has already been committed.");
                //Assert(_parentItem != null || (_created != null && _created.Ptr.IsRoot), "The parent was not updated.");
                PerformCommit();
            }

            void PerformCommit()
            {
                if (_disposed) throw new ObjectDisposedException(GetType().FullName);

                try
                {
                    NodePin pin = _created;
                    while (pin != null)
                    {
                        pin.Ptr.ToReadOnly();
                        _cache.SaveChanges(pin);
                        pin = (NodePin)pin.Next;
                    }

                    if (_parentItem != null)
                    {
                        using (_parentItem.Lock.Write(_cache.Options.LockTimeout))
                        {
                            _parentItem.Ptr.ToReadOnly();
                            _cache.SaveChanges(_parentItem);
                        }
                    }

                    pin = _deleted;
                    while (pin != null)
                    {
                        _cache.SaveChanges(pin);
                        pin = (NodePin)pin.Next;
                    }

                    _committed = true;
                    FinalizeCommit();
                }
                catch
                {
                    Rollback();
                    throw;
                }
            }

            private void FinalizeCommit()
            {
                NodePin pin = _created;
                while (pin != null)
                {
                    pin.CommitChanges();
                    pin = (NodePin)pin.Next;
                }

                if (_parentItem != null)
                    _parentItem.CommitChanges();

                pin = _deleted;
                while (pin != null)
                {
                    pin.CommitChanges();
                    pin = (NodePin)pin.Next;
                }

                if (_deleted != null)
                    _cache.AddVersion(_deleted);

                if (_hasLogToken)
                {
                    _hasLogToken = false;
                    _cache.Options.LogFile.CommitTransaction(ref _logToken);
                }
            }

            public void Rollback()
            {
                if (_disposed) throw new ObjectDisposedException(GetType().FullName);
                if (_reverted) return;
                _reverted = true;

                if (_parentItem != null)
                {
                    _parentItem.CancelChanges();
                    _cache.UpdateNode(_parentItem);
                    _parentItem.Dispose();
                }

                NodePin pin = _created;
                while (pin != null)
                {
                    pin.CancelChanges();
                    _cache.UpdateNode(pin);
                    _cache.Storage.Destroy(pin.Handle.StoreHandle);
                    pin.Dispose();
                    pin = (NodePin)pin.Next;
                }

                pin = _deleted;
                while (pin != null)
                {
                    pin.CancelChanges();
                    _cache.UpdateNode(pin);
                    pin.Dispose();
                    pin = (NodePin)pin.Next;
                }

                if (_hasLogToken)
                {
                    _hasLogToken = false;
                    _cache.Options.LogFile.RollbackTransaction(ref _logToken);
                }
            }

            public void Dispose()
            {
                try
                {
                    if (!_committed)
                        Rollback();
                }
                finally
                {
                    _disposed = true;
                }
            }
        }
    }
}
