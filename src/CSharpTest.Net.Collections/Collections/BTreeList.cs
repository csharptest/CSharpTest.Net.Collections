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

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Implements an IList interface for an in-memory B+Tree of unique values
    /// </summary>
    [System.Diagnostics.DebuggerDisplay("Count = {_count}")]
    public class BTreeList<T> : IList<T>, ICloneable, IDisposable
    {
        /// <summary>The default `order` of the B+Tree structure.</summary>
        public const int DefaultOrder = 64;
        private Node _root;
        private readonly int _order;
        private readonly IComparer<T> _comparer;
        private readonly KvComparer _kvcomparer;
        private int _count;
        private bool _isReadOnly;
        private Node _first, _last;

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList() : this(DefaultOrder, Comparer<T>.Default) { }
        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList(IComparer<T> comparer) : this(DefaultOrder, comparer) { }
        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList(int order, IComparer<T> comparer)
        {
            _isReadOnly = false;
            _order = order;
            _comparer = comparer;
            _kvcomparer = new KvComparer(_comparer);
            Clear();
        }
        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList(IEnumerable<T> copyFrom) : this(DefaultOrder, Comparer<T>.Default, copyFrom) { }
        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList(IComparer<T> comparer, IEnumerable<T> copyFrom) : this(DefaultOrder, comparer, copyFrom) { }
        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeList(int order, IComparer<T> comparer, IEnumerable<T> copyFrom)
            : this(order, comparer)
        {
            AddRange(copyFrom);
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        public int Count { get { return _count; } }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        /// </summary>
        public bool IsReadOnly { get { return _isReadOnly; } }
        /// <summary>
        /// Gets the Comparer provided to the constructor or Comparer&lt;TKey>.Default if it was not provided.
        /// </summary>
        public IComparer<T> Comparer { get { return _comparer; } }

        void IDisposable.Dispose() { Clear(); }

        #region IList<TKey> Members

        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The Collection is read-only.</exception>
        public void Clear()
        {
            Modify();
            _count = 0;
            _root = new Node(_order);
            _first = _last = _root;
        }

        /// <summary>
        /// Warning O(n) operation: This method works; however, it is not intended for use on sufficiently large lists. 
        /// </summary>
        T IList<T>.this[int index]
        {
            get 
            {
                if (index >= 0 && index < _count)
                {
                    Node parent = _first;
                    while (parent != null)
                    {
                        if (index < parent.Count)
                            return parent.Values[index];
                        index -= parent.Count;
                        parent = parent.Next;
                    }
                }
                throw new ArgumentOutOfRangeException("index");
            }
            set { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Adds an element with the provided value to the <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </summary>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.ArgumentException">An element with the same value already exists in the <see cref="T:System.Collections.Generic.IList`1"/>.</exception>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public void Add(T value)
        {
            Modify();
            if (!Add(null, -1, _root, value, true))
                throw new ArgumentException("An item with the same value has already been added.");
        }

        /// <summary>
        /// Adds a set of items to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <param name="items">The items to add to the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param>
        /// <exception cref="T:System.ArgumentException">An element with the same value already exists in the <see cref="T:System.Collections.Generic.IList`1"/>.</exception>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public void AddRange(IEnumerable<T> items)
        {
            Modify();
            foreach (T pair in items)
                Add(pair);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value pair.
        /// </summary>
        /// <returns>true if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.</returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param>
        public bool Contains(T item)
        {
            SeekResult result;
            return Seek(_root, item, out result);
        }

        /// <summary>
        /// Warning O(n) operation: This method works; however, it is not intended for use on sufficiently large lists. 
        /// Determines the index of a specific item in the <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </summary>
        /// <returns>The index of <paramref name="item"/> if found in the list; otherwise, -1.</returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.IList`1"/>.</param>
        int IList<T>.IndexOf(T item)
        {
            SeekResult result;
            if(Seek(_root, item, out result))
            {
                int ordinal = result.ParentIx;
                while((result.Parent = result.Parent.Prev) != null)
                    ordinal += result.Parent.Count;
                return ordinal;
            }
            return -1;
        }

        void IList<T>.Insert(int index, T item)
        { throw new NotSupportedException(); }

        /// <summary>
        /// Warning O(n) operation: This method works; however, it is not intended for use on sufficiently large lists. 
        /// </summary>
        void IList<T>.RemoveAt(int index)
        {
            Modify();
            IList<T> l = this;
            Remove(l[index]);
        }
        
        /// <summary>
        /// Removes the element with the specified value from the <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </summary>
        /// <returns>
        /// true if the element is successfully removed; otherwise, false.  This method also returns false if <paramref name="value"/> was not found in the original <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </returns>
        /// <param name="value">The value of the element to remove.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public bool Remove(T value)
        {
            Modify();
            return Remove(null, -1, _root, value);
        }

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        public void CopyTo(T[] array, int arrayIndex)
        {
            if(array == null)
                throw new ArgumentNullException();
            if(arrayIndex < 0 || (array.Length - arrayIndex) < _count)
                throw new ArgumentOutOfRangeException();

            Node node = _first;
            while(node != null)
            {
                Array.Copy(node.Values, 0, array, arrayIndex, node.Count);
                arrayIndex += node.Count;
                node = node.Next;
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<T> GetEnumerator() { return new Enumerator(_first, 0); }
        [Obsolete] System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }

        #endregion
        #region Public Members

        /// <summary>
        /// Adds an element with the provided value to the <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </summary>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public bool TryAddItem(T value)
        {
            Modify();
            return Add(null, -1, _root, value, true);
        }

        /// <summary>
        /// Returns all the items of this collection as an array of  <see cref="T:System.Collections.Generic.KeyValuePair`2"/>.
        /// </summary>
        public T[] ToArray()
        {
            T[] array = new T[_count];
            CopyTo(array, 0);
            return array;
        }

        /// <summary>
        /// Inclusivly enumerates from start value to the end of the collection
        /// </summary>
        public IEnumerable<T> EnumerateFrom(T start)
        {
            SeekResult result;
            Seek(_root, start, out result);
            return new Enumerator(result.Parent, result.ParentIx);
        }

        /// <summary>
        /// Inclusivly enumerates from start value to stop value
        /// </summary>
        public IEnumerable<T> EnumerateRange(T start, T end)
        {
            SeekResult result;
            Seek(_root, start, out result);
            return new Enumerator(result.Parent, result.ParentIx, x => (_comparer.Compare(x, end) <= 0));
        }

        object ICloneable.Clone() { return Clone(); }
        /// <summary>
        /// Returns a writable clone of this collection.
        /// </summary>
        public BTreeList<T> Clone()
        {
            BTreeList<T> copy = (BTreeList<T>)MemberwiseClone();
            copy._first = copy._last = null;
            copy._root = copy._root.Clone(ref copy._first, ref copy._last);
            copy._isReadOnly = false;
            return copy;
        }
        /// <summary>
        /// Returns a read-only clone of this collection.  If this instance is already read-only the method will return this.
        /// </summary>
        public BTreeList<T> MakeReadOnly()
        {
            if (_isReadOnly) return this;
            BTreeList<T> copy = Clone();
            copy._isReadOnly = true;
            return copy;
        }

        #endregion
        #region Implementation Details

        void Modify() { if (_isReadOnly) throw new NotSupportedException("Collection is read-only."); }

        struct SeekResult
        {
            public Node Parent;
            public int ParentIx;
        }

        bool Seek(Node from, T item, out SeekResult found)
        {
            found = new SeekResult();
            found.Parent = from;
            found.ParentIx = -1;
            
            KeyValuePair<T, Node> seek = new KeyValuePair<T, Node>(item, null);
            while (!found.Parent.IsLeaf)
            {
                int ix = Array.BinarySearch(found.Parent.Children, 1, found.Parent.Count - 1, seek, _kvcomparer);
                if (ix < 0 && (ix = ~ix) > 0)
                    ix--;
                found.Parent = found.Parent.Children[ix].Value;
            }

            found.ParentIx = Array.BinarySearch(found.Parent.Values, 0, found.Parent.Count, item, _comparer);
            if (found.ParentIx < 0)
            {
                found.ParentIx = ~found.ParentIx;
                return false;
            }

            return true;
        }

        bool Add(Node parent, int myIx, Node me, T item, bool adding)
        {
            if (me.Count == _order)
            {
                if (parent == null)
                {
                    _root = parent = new Node(_order, default(T), me);
                    myIx = 0;
                }
                T split;
                Node next = new Node(_order, me, out split, ref _first, ref _last);
                parent.Add(myIx + 1, split, next);
                if (_comparer.Compare(split, item) <= 0)
                    me = next;
            }
            int ix;
            if (me.IsLeaf)
            {
                ix = Array.BinarySearch(me.Values, 0, me.Count, item, _comparer);
                if (ix < 0) ix = ~ix;
                else
                {
                    if (adding) return false;
                    me.Values[ix] = item;
                    return true;
                }
                me.Add(ix, item);
                _count++;
                return true;
            }

            KeyValuePair<T, Node> seek = new KeyValuePair<T, Node>(item, null);
            ix = Array.BinarySearch(me.Children, 1, me.Count - 1, seek, _kvcomparer);
            if (ix < 0 && (ix = ~ix) > 0)
                ix--;
            return Add(me, ix, me.Children[ix].Value, item, adding);
        }

        bool Remove(Node parent, int myIx, Node me, T item)
        {
            if (parent == null && ReferenceEquals(me, _root) && me.Count == 1 && me.IsLeaf == false)
                _root = me.Children[0].Value;
            if (parent != null && me.Count <= (_order >> 2))
            {
                if (myIx == 0)
                    parent.Join(myIx, myIx + 1, ref _first, ref _last);
                else
                    parent.Join(myIx - 1, myIx, ref _first, ref _last);
                return Remove(null, -1, parent, item);
            }

            int ix;
            if (me.IsLeaf)
            {
                ix = Array.BinarySearch(me.Values, 0, me.Count, item, _comparer);
                if (ix < 0)
                    return false;

                me.RemoveValue(ix);
                _count--;
                return true;
            }

            KeyValuePair<T, Node> seek = new KeyValuePair<T, Node>(item, null);
            ix = Array.BinarySearch(me.Children, 1, me.Count - 1, seek, _kvcomparer);
            if (ix < 0 && (ix = ~ix) > 0)
                ix--;

            Node child = me.Children[ix].Value;
            if (!Remove(me, ix, child, item))
                return false;

            return true;
        }

        #endregion
        #region class Enumerator
        class Enumerator : IEnumerator<T>, IEnumerable<T>
        {
            private readonly Node _start;
            private readonly int _startIx;
            private readonly Predicate<T> _fnContinue;
            private Node _current;
            private int _index;
            private bool _valid;

            public Enumerator(Node start, int index) : this(start, index, null) { }
            public Enumerator(Node start, int index, Predicate<T> fnContinue)
            {
                _start = start;
                _startIx = index;
                _fnContinue = fnContinue;
                Reset();
            }
            public void Reset()
            {
                _valid = false;
                _current = _start;
                _index = _startIx - 1;
            }
            public bool MoveNext()
            {
                if (_current == null) return false;
                if (++_index >= _current.Count)
                {
                    _current = _current.Next;
                    _index = 0;
                }
                _valid = _current != null && _index >= 0 && _index < _current.Count;
                if (_valid && _fnContinue != null && !_fnContinue(Current))
                    _valid = false;
                return _valid;
            }
            [Obsolete] object System.Collections.IEnumerator.Current { get { return Current; } }
            public T Current
            {
                get 
                {
                    if (_valid) 
                        return _current.Values[_index];
                    throw new InvalidOperationException();
                }
            }
            public void Dispose()
            { }

            [Obsolete]
            IEnumerator<T> IEnumerable<T>.GetEnumerator()
            { return this; }
            [Obsolete]
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() 
            { return this; }
        }
        #endregion
        #region class KvComparer

        class KvComparer : IComparer<KeyValuePair<T, Node>>
        {
            private readonly IComparer<T> _comparer;
            public KvComparer(IComparer<T> comparer)
            { _comparer = comparer; }
            int IComparer<KeyValuePair<T, Node>>.Compare(KeyValuePair<T, Node> x, KeyValuePair<T, Node> y)
            {
                return _comparer.Compare(x.Key, y.Key);
            }
        }

        #endregion
        #region class Node

        [System.Diagnostics.DebuggerDisplay("Count = {_count}")]
        private class Node
        {
            int _count;
            KeyValuePair<T, Node>[] _children;
            T[] _values;
            public Node Next, Prev;

            public int Count { get { return _count; } }
            public bool IsLeaf { get { return _values != null; } }
            public KeyValuePair<T, Node>[] Children { get { return _children; } }
            public T[] Values { get { return _values; } }

            public Node(int order)
            {
                _count = 0;
                _values = new T[order];
            }
            public Node(int order, T value, Node node)
            {
                _count = 1;
                _children = new KeyValuePair<T, Node>[order];
                _children[0] = new KeyValuePair<T, Node>(value, node);
            }
            public Node(int order, Node split, out T splitKey, ref Node first, ref Node last)
            {
                if (split._values != null)
                {
                    _values = new T[order];
                    int half = split._count >> 1;
                    Array.Copy(split._values, half, _values, 0, _count = (split._count - half));
                    Array.Clear(split._values, half, _count);
                    split._count = half;
                    splitKey = _values[0];

                    Prev = split;
                    Node oldNext = split.Next;
                    split.Next = this;
                    if ((Next = oldNext) == null)
                        last = split;
                }
                else //if (split.Children != null)
                {
                    _children = new KeyValuePair<T, Node>[order];
                    int half = split._count >> 1;
                    Array.Copy(split._children, half, _children, 0, _count = (split._count - half));
                    Array.Clear(split._children, half, _count);
                    split._count = half;
                    splitKey = _children[0].Key;
                }
            }
            public void Add(int ix, T kv)
            {
                if (ix < _count)
                    Array.Copy(_values, ix, _values, ix + 1, _count - ix);
                _values[ix] = kv;
                _count++;
            }
            public void Add(int ix, T value, Node child)
            {
                if (ix < _count)
                    Array.Copy(_children, ix, _children, ix + 1, _count - ix);
                _children[ix] = new KeyValuePair<T, Node>(value, child);
                _count++;
            }
            public void RemoveValue(int ix)
            {
                _values[ix] = default(T);
                _count--;
                if (ix < _count)
                    Array.Copy(_values, ix + 1, _values, ix, _count - ix);
            }
            void RemoveChild(int ix, ref Node first, ref Node last)
            {
                Node child = _children[ix].Value;
                if(child != null)
                {
                    if (child.Next != null)
                        child.Next.Prev = child.Prev;
                    else if (ReferenceEquals(child, last))
                        last = child.Prev;
                    if (child.Prev != null)
                        child.Prev.Next = child.Next;
                    else if (ReferenceEquals(child, first))
                        first = child.Next;
                }
                _children[ix] = new KeyValuePair<T, Node>();
                _count--;
                if (ix < _count)
                    Array.Copy(_children, ix + 1, _children, ix, _count - ix);
            }
            public void Join(int firstIx, int secondIx, ref Node nodeFirst, ref Node nodeLast)
            {
                if (IsLeaf || firstIx < 0 || secondIx >= Count) return;
                Node first = Children[firstIx].Value;
                Node second = Children[secondIx].Value;
                int order = first.IsLeaf ? first.Values.Length : first.Children.Length;
                int total = first.Count + second.Count;

                if (total < order)
                {
                    if (first.IsLeaf)
                        Array.Copy(second.Values, 0, first.Values, first.Count, second.Count);
                    else
                        Array.Copy(second.Children, 0, first.Children, first.Count, second.Count);
                    first._count += second.Count;
                    RemoveChild(secondIx, ref nodeFirst, ref nodeLast);
                }
                else if (first.IsLeaf)
                {
                    T[] list = new T[total];
                    Array.Copy(first.Values, 0, list, 0, first.Count);
                    Array.Copy(second.Values, 0, list, first.Count, second.Count);
                    Array.Clear(first.Values, 0, first.Count);
                    Array.Clear(second.Values, 0, second.Count);

                    int half = total >> 1;
                    Array.Copy(list, 0, first.Values, 0, first._count = half);
                    Array.Copy(list, half, second.Values, 0, second._count = (total - half));
                    Children[secondIx] = new KeyValuePair<T, Node>(second.Values[0], second);
                }
                else //if (first.IsLeaf == false)
                {
                    KeyValuePair<T, Node>[] list = new KeyValuePair<T, Node>[total];
                    Array.Copy(first.Children, 0, list, 0, first.Count);
                    Array.Copy(second.Children, 0, list, first.Count, second.Count);
                    Array.Clear(first.Children, 0, first.Count);
                    Array.Clear(second.Children, 0, second.Count);
                    list[first.Count] = new KeyValuePair<T, Node>(Children[secondIx].Key, list[first.Count].Value);

                    int half = total >> 1;
                    Array.Copy(list, 0, first.Children, 0, first._count = half);
                    Array.Copy(list, half, second.Children, 0, second._count = (total - half));
                    Children[secondIx] = new KeyValuePair<T, Node>(second.Children[0].Key, second);
                }
            }
            public Node Clone(ref Node first, ref Node last)
            {
                Node copy = (Node)MemberwiseClone();
                if (copy.IsLeaf)
                {
                    copy._values = (T[])copy._values.Clone();

                    copy.Next = null;
                    copy.Prev = last;
                    if (first == null)
                        first = copy;
                    if (last != null)
                        last.Next = copy;
                    last = copy;
                }
                else
                {
                    copy._children = new KeyValuePair<T, Node>[copy._children.Length];
                    for( int i=0; i < copy._count; i++)
                        copy._children[i] = new KeyValuePair<T, Node>(
                            _children[i].Key,
                            _children[i].Value.Clone(ref first, ref last)
                            );
                }
                return copy;
            }
        }
        #endregion
    }
}
