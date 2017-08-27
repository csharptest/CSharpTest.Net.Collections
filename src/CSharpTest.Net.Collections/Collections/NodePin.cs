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
using System.Threading;


namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        private const LockType NoLock = 0;

        private enum LockType
        {
            Read = 1,
            Insert = 2,
            Update = 3,
            Delete = 4
        }

        //[System.Diagnostics.DebuggerNonUserCode]
        [DebuggerDisplay("{Ptr._list}")]
        private class NodePin : NodeVersion, IDisposable
        {
            private Node _temp;

            public NodePin(NodeHandle handle, LockType ltype, object refobj,
                Node original, Node updated)
            {
                AssertionFailedException.Assert(original == null || original.IsReadOnly);
                Handle = handle;
                LockType = ltype;
                Original = original;
                _temp = updated;
                Reference = refobj;
            }

            public NodeHandle Handle { get; }

            public Node Original { get; private set; }

            public Node Ptr => IsDeleted ? null : (_temp ?? Original);

            public LockType LockType { get; }

            public bool IsDeleted { get; private set; }

            public object Reference { get; }

            public void Dispose()
            {
                _temp = null;
            }

            public void BeginUpdate()
            {
                AssertionFailedException.Assert(LockType != LockType.Read, "Node is currently read-only");
                AssertionFailedException.Assert(_temp == null, "Node is already in a transaction");
                AssertionFailedException.Assert(IsDeleted == false, "Node is already marked for deletion");
                _temp = Original.CloneForWrite(LockType);
            }

            public void MarkDeleted()
            {
                AssertionFailedException.Assert(LockType != LockType.Read, "Node is currently read-only");
                IsDeleted = true;
                _temp = null;
            }

            public void CommitChanges()
            {
                AssertionFailedException.Assert(LockType != LockType.Read, "Node is currently read-only");
                if (IsDeleted)
                {
                    Dispose();
                }
                else
                {
                    if (_temp == null) throw new InvalidOperationException("Node is not in a transaction");
                    Original = _temp.IsReadOnly ? _temp : _temp.ToReadOnly();
                    _temp = null;
                }
            }

            public void CancelChanges()
            {
                AssertionFailedException.Assert(LockType != LockType.Read, "Node is currently read-only");
                //if (_temp == null) throw new InvalidOperationException("Node is not in a transaction");
                //_temp.Invalidate();
                _temp = null;
            }
        }

        private class NodeVersion
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
                {
                    first = version;
                }
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