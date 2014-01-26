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
using System.Threading;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        const LockType NoLock = 0;
        enum LockType { Read = 1, Insert = 2, Update = 3, Delete = 4 }

        //[System.Diagnostics.DebuggerNonUserCode]
        [System.Diagnostics.DebuggerDisplay("{Ptr._list}")]
        class NodePin : NodeVersion, IDisposable
        {
            private readonly NodeHandle _handle;
            private readonly object _reference;
            private readonly LockType _ltype;
            private readonly ILockStrategy _lock;
            private Node _ptr, _temp;
            private LockType _lockHeld;
            private bool _deleted;

            public NodeHandle Handle { get { return _handle; } }
            public Node Original { get { return _ptr; } }
            public Node Ptr { get { return _deleted ? null : (_temp ?? _ptr); } }
            public ILockStrategy Lock { get { return _lock; } }
            public LockType LockType { get { return _ltype; } }
            public bool IsDeleted { get { return _deleted; } }
            public Object Reference { get { return _reference; } }

            public NodePin(NodeHandle handle, ILockStrategy lck, LockType ltype, LockType lockHeld, object refobj, Node original, Node updated)
            {
                Assert(original == null || original.IsReadOnly);
                _handle = handle;
                _lock = lck;
                _ltype = ltype;
                _lockHeld = lockHeld;
                _ptr = original;
                _temp = updated;
                _reference = refobj;
            }

            public void Dispose()
            {
                if (_lockHeld == LockType.Read)
                    _lock.ReleaseRead();
                else if(_lockHeld != NoLock)
                    _lock.ReleaseWrite();
                    
                _lockHeld = NoLock;
                //if (_temp != null)
                //    _temp.Invalidate();
                _temp = null;
            }

            public void BeginUpdate()
            {
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                Assert(_temp == null, "Node is already in a transaction");
                Assert(_deleted == false, "Node is already marked for deletion");
                _temp = _ptr.CloneForWrite(_ltype);
            }

            public void MarkDeleted()
            {
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                _deleted = true;
                //if (_temp != null)
                //  _temp.Invalidate();
                _temp = null;
            }

            public void CommitChanges()
            {
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                if (_deleted)
                { Dispose(); }
                else
                {
                    if (_temp == null) throw new InvalidOperationException("Node is not in a transaction");
                    _ptr = _temp.IsReadOnly ? _temp : _temp.ToReadOnly();
                    _temp = null;
                }
            }

            public void CancelChanges()
            {
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                //if (_temp == null) throw new InvalidOperationException("Node is not in a transaction");
                //_temp.Invalidate();
                _temp = null;
            }
        }

        class NodeVersion
        {
            public NodeVersion Next;

            public void ChainTo(ref NodeVersion current)
            {
                NodeVersion last = this;
                while (last.Next != null)
                    last = last.Next;
                Interlocked.Exchange(ref current, last).Next = this;
            }

            public static void Append<T>(ref T first, T version) where T : NodeVersion
            {
                if (first == null)
                    first = version;
                else
                {
                    NodeVersion last = first;
                    while (last.Next != null)
                        last = last.Next;
                    last.Next = version;
                }
            }
        }
    }
}
