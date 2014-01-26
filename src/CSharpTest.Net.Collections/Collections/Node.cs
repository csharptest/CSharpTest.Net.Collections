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

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        [System.Diagnostics.DebuggerDisplay("RootNode, Handle = {_handle}")]
        class RootNode : Node
        {
            public RootNode(IStorageHandle handle)
                : base(handle, 1)
            { 
                _count = 1; /*invariant for root*/
                _ltype = LockType.Read; /*will be a transacted update, not a create*/
            }
            private RootNode(Node copyFrom, LockType type) : base(copyFrom, type)
            { }

            public override bool IsRoot { get { return true; } }

            public override bool BinarySearch(IComparer<Element> comparer, Element find, out int ordinal)
            { ordinal = 0; return true; }

            public override Node CloneForWrite(LockType ltype)
            {
                if (_ltype == ltype) return this;
                return new RootNode(this, ltype);
            }
        }

        [System.Diagnostics.DebuggerDisplay("Handle = {_handle}, Count = {_count}")]
        class Node
        {
            private readonly IStorageHandle _handle;
            protected readonly Element[] _list;
            protected LockType _ltype;
            protected int _count;
            protected int _version;

            public Node(IStorageHandle handle, int elementCount)
            {
                _handle = handle;
                _list = new Element[elementCount];
                _ltype = LockType.Insert;
                _count = 0;
            }

            protected Node(Node copyFrom, LockType type)
            {
                _handle = copyFrom._handle;
                _list = (Element[])copyFrom._list.Clone();
                _count = copyFrom._count;
                _ltype = type;
                if (_ltype == LockType.Update && !IsLeaf)
                    _ltype = LockType.Read;

                _version = copyFrom._version + 1;
            }

            public static Node FromElements(IStorageHandle handle, bool isRoot, int nodeSize, Element[] items)
            {
                if(isRoot)
                {
                    RootNode root = new RootNode(handle);
                    Assert(items.Length == 1);
                    root._list[0] = items[0];
                    return root;
                }

                Node node = new Node(handle, nodeSize);
                Array.Copy(items, 0, node._list, 0, items.Length);
                node._count = items.Length;
                node._ltype = LockType.Read;
                return node;
            }

            public IStorageHandle StorageHandle { get { return _handle; } }
            public bool IsReadOnly { get { return _ltype == LockType.Read; } }

            //public void Invalidate()
            //{
            //    _count = int.MinValue;
            //    Array.Clear(_list, 0, _list.Length);
            //    _ltype = LockType.Read;
            //}

            public Node ToReadOnly()
            {
                Assert(_ltype != LockType.Read, "Node is already read-only.");
                _ltype = LockType.Read;
                return this;
            }

            public virtual Node CloneForWrite(LockType ltype)
            {
                if (_ltype == ltype) return this;
                Assert(ltype != LockType.Read, "Read lock can not clone for write");
                return new Node(this, ltype);
            }

            [System.Diagnostics.DebuggerNonUserCode]
            public Element this[int ordinal] { get { return _list[ordinal]; } }

            [System.Diagnostics.DebuggerNonUserCode]
            public int Count { get { return _count; } }

            [System.Diagnostics.DebuggerNonUserCode]
            public int Size { get { return _list.Length; } }

            [System.Diagnostics.DebuggerNonUserCode]
            public bool IsLeaf { get { return _count == 0 || _list[0].IsNode == false; } }
            
            [System.Diagnostics.DebuggerNonUserCode]
            public virtual bool IsRoot { get { return false; } }

            public virtual bool BinarySearch(IComparer<Element> comparer, Element find, out int ordinal)
            {
                int start = _count == 0 || _list[0].IsValue ? 0 : 1;
                ordinal = Array.BinarySearch(_list, start, _count - start, find, comparer);
                if (ordinal < 0)
                {
                    ordinal = ~ordinal;
                    if (IsLeaf)
                        return false;

                    if (ordinal > 0)
                        ordinal--;
                    return false;
                }
                return true;
            }

            public void ReplaceKey(int ordinal, TKey minKey)
            { ReplaceKey(ordinal, minKey, null); }
            
            public void ReplaceKey(int ordinal, TKey minKey, IComparer<TKey> comparer)
            {
                Assert(!IsRoot, "Invalid operation on root.");
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                Assert(ordinal >= 0 && ordinal < _count, "Index out of range.");
                if (comparer == null || comparer.Compare(minKey, _list[ordinal].Key) != 0)
                    _list[ordinal] = new Element(minKey, _list[ordinal]);
            }

            public void ReplaceChild(int ordinal, NodeHandle original, NodeHandle value)
            {
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                Assert(ordinal >= 0 && ordinal < _count, "Index out of range.");
                Element replacing = _list[ordinal];
                Assert(
                    (original == null && replacing.ChildNode == null) ||
                    (original != null && original.Equals(replacing.ChildNode))
                    , "Incorrect child being replaced.");
                _list[ordinal] = new Element(replacing.Key, value);
            }

            public void SetValue(int ordinal, TKey key, TValue value, IComparer<TKey> comparer)
            {
                Assert(!IsRoot, "Invalid operation on root.");
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                Assert(ordinal >= 0 && ordinal < _count, "Index out of range.");
                Assert(comparer.Compare(_list[ordinal].Key, key) == 0, "Incorrect key for value replacement.");
                _list[ordinal] = new Element(key, value);
            }

            public void Insert(int ordinal, Element item)
            {
                Assert(!IsRoot, "Invalid operation on root.");
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                if (ordinal < 0 || ordinal > _count || ordinal >= _list.Length)
                    throw new AssertionFailedException();

                if (ordinal < _count)
                    Array.Copy(_list, ordinal, _list, ordinal + 1, _count - ordinal);

                _list[ordinal] = item;

                _count++;
            }

            public void Remove(int ordinal, Element item, IComparer<TKey> comparer)
            {
                Assert(!IsRoot, "Invalid operation on root.");
                Assert(_ltype != LockType.Read, "Node is currently read-only");
                if (ordinal < 0 || ordinal >= _count)
                    throw new AssertionFailedException();

                Assert(comparer.Compare(_list[ordinal].Key, item.Key) == 0);

                if (ordinal < _count - 1)
                    Array.Copy(_list, ordinal + 1, _list, ordinal, _count - ordinal - 1);

                _count--;
                _list[_count] = new Element();
            }

            /// <summary> For enumeration </summary>
            public void CopyTo(Element[] elements, out int currentLimit)
            {
                _list.CopyTo(elements, 0);
                currentLimit = _count;
            }
        }
    }
}
