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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace CSharpTest.Net.Collections
{
    /// <summary>
    ///     Implements an IDictionary interface for an in-memory B+Tree
    /// </summary>
    [DebuggerDisplay("Count = {Count}")]
    public class BTreeDictionary<TKey, TValue> : IDictionaryEx<TKey, TValue>, ICloneable
    {
        /// <summary>The default `order` of the B+Tree structure.</summary>
        public const int DefaultOrder = 64;

        private readonly KvComparer _kvcomparer;
        private readonly int _order;
        private Node _first, _last;

        private KeyCollection _keys;
        private Node _root;
        private ValueCollection _values;

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary() : this(DefaultOrder, Comparer<TKey>.Default)
        {
        }

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary(IComparer<TKey> comparer) : this(DefaultOrder, comparer)
        {
        }

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary(int order, IComparer<TKey> comparer)
        {
            if (order < 4)
                throw new ArgumentOutOfRangeException("order");
            if (comparer == null)
                throw new ArgumentNullException("comparer");

            IsReadOnly = false;
            _order = order;
            Comparer = comparer;
            _kvcomparer = new KvComparer(Comparer);
            Clear();
        }

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary(IEnumerable<KeyValuePair<TKey, TValue>> copyFrom) : this(DefaultOrder,
            Comparer<TKey>.Default, copyFrom)
        {
        }

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> copyFrom) : this(
            DefaultOrder, comparer, copyFrom)
        {
        }

        /// <summary>Constructs a BTreeList instance.</summary>
        public BTreeDictionary(int order, IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> copyFrom)
            : this(order, comparer)
        {
            if (copyFrom == null)
                throw new ArgumentNullException("copyFrom");
            AddRange(copyFrom);
        }

        /// <summary>
        ///     Gets the Comparer provided to the constructor or Comparer&lt;TKey>.Default if it was not provided.
        /// </summary>
        public IComparer<TKey> Comparer { get; }

        /// <summary>
        ///     Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </summary>
        public int Count { get; private set; }

        /// <summary>
        ///     Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1" /> is read-only.
        /// </summary>
        public bool IsReadOnly { get; private set; }

        void IDisposable.Dispose()
        {
            Clear();
        }

        /// <summary>
        ///     Ensures data integrity or raises exception
        /// </summary>
        [Conditional("DEBUG")]
        public void DebugAssert()
        {
#if DEBUG
            Node foundFirst = _root;
            while (!foundFirst.IsLeaf)
                foundFirst = foundFirst.Children[0].Value;

            if (!ReferenceEquals(_first, foundFirst))
                throw new ApplicationException("The _first node reference is incorrect.");

            Node foundLast = _root;
            while (!foundLast.IsLeaf)
                foundLast = foundLast.Children[foundLast.Count - 1].Value;

            if (!ReferenceEquals(_last, foundLast))
                throw new ApplicationException("The _last node reference is incorrect.");

            int counter = 0;
            Node tmp = _first;
            while (tmp != null)
            {
                if (tmp.Next == null && !ReferenceEquals(_last, tmp))
                    throw new ApplicationException("Forward links corrupted.");
                if (!tmp.IsLeaf)
                    throw new ApplicationException("Non Leaf in linked list.");
                counter += tmp.Count;
                tmp = tmp.Next;
            }
            if (counter != Count)
                throw new ApplicationException(string.Format("Found {0} items in links, {1} expected.", counter,
                    Count));
            counter = 0;
            tmp = _last;
            while (tmp != null)
            {
                if (tmp.Prev == null && !ReferenceEquals(_first, tmp))
                    throw new ApplicationException("Forward links corrupted.");
                if (!tmp.IsLeaf)
                    throw new ApplicationException("Non Leaf in linked list.");
                counter += tmp.Count;
                tmp = tmp.Prev;
            }
            if (counter != Count)
                throw new ApplicationException(string.Format("Found {0} items in links, {1} expected.", counter,
                    Count));

            counter = DescendAssert(_root);
            if (counter != Count)
                throw new ApplicationException(string.Format("Tree crawl found {0} items, {1} expected.", counter,
                    Count));
#endif
        }

#if DEBUG
        private int DescendAssert(Node node)
        {
            EqualityComparer<TValue> eq = EqualityComparer<TValue>.Default;
            int count = 0;
            if (node.IsLeaf)
                for (int i = 0; i < node.Values.Length; i++)
                    if (i < node.Count)
                    {
                        count++;
                    }
                    else
                    {
                        if (!eq.Equals(default(TValue), node.Values[i].Value))
                            throw new ApplicationException("Unexpected non-default value after count.");
                    }
            else
                for (int i = 0; i < node.Children.Length; i++)
                    if (i < node.Count)
                    {
                        count += DescendAssert(node.Children[i].Value);
                    }
                    else
                    {
                        if (node.Children[i].Value != null)
                            throw new ApplicationException("Unexpected non-null child after count.");
                    }

            if (count == 0 && Count != 0)
                throw new ApplicationException("Unexpected empty count.");
            return count;
        }
#endif

        #region class KeyCollection

        private class KeyCollection : ICollection<TKey>
        {
            private readonly BTreeDictionary<TKey, TValue> _owner;

            public KeyCollection(BTreeDictionary<TKey, TValue> owner)
            {
                _owner = owner;
            }

            #region ICollection<TKey> Members

            public int Count => _owner.Count;
            public bool IsReadOnly => true;

            public bool Contains(TKey item)
            {
                return _owner.ContainsKey(item);
            }

            public void CopyTo(TKey[] array, int arrayIndex)
            {
                foreach (TKey key in this)
                    array[arrayIndex++] = key;
            }

            public IEnumerator<TKey> GetEnumerator()
            {
                return new KeyEnumerator(_owner.GetEnumerator());
            }

            [Obsolete]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class KeyEnumerator : IEnumerator<TKey>
            {
                private readonly IEnumerator<KeyValuePair<TKey, TValue>> _e;

                public KeyEnumerator(IEnumerator<KeyValuePair<TKey, TValue>> e)
                {
                    _e = e;
                }

                public TKey Current => _e.Current.Key;

                [Obsolete]
                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _e.Dispose();
                }

                public bool MoveNext()
                {
                    return _e.MoveNext();
                }

                public void Reset()
                {
                    _e.Reset();
                }
            }

            [Obsolete]
            void ICollection<TKey>.Add(TKey item)
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            void ICollection<TKey>.Clear()
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            bool ICollection<TKey>.Remove(TKey item)
            {
                throw new NotSupportedException();
            }

            #endregion
        }

        #endregion

        #region class ValueCollection

        private class ValueCollection : ICollection<TValue>
        {
            private readonly BTreeDictionary<TKey, TValue> _owner;

            public ValueCollection(BTreeDictionary<TKey, TValue> owner)
            {
                _owner = owner;
            }

            #region ICollection<TKey> Members

            public int Count => _owner.Count;
            public bool IsReadOnly => true;

            public bool Contains(TValue item)
            {
                IEqualityComparer<TValue> c = EqualityComparer<TValue>.Default;
                foreach (TValue value in this)
                    if (c.Equals(item, value))
                        return true;
                return false;
            }

            public void CopyTo(TValue[] array, int arrayIndex)
            {
                foreach (TValue value in this)
                    array[arrayIndex++] = value;
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                return new ValueEnumerator(_owner.GetEnumerator());
            }

            [Obsolete]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class ValueEnumerator : IEnumerator<TValue>
            {
                private readonly IEnumerator<KeyValuePair<TKey, TValue>> _e;

                public ValueEnumerator(IEnumerator<KeyValuePair<TKey, TValue>> e)
                {
                    _e = e;
                }

                public TValue Current => _e.Current.Value;

                [Obsolete]
                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _e.Dispose();
                }

                public bool MoveNext()
                {
                    return _e.MoveNext();
                }

                public void Reset()
                {
                    _e.Reset();
                }
            }

            [Obsolete]
            void ICollection<TValue>.Add(TValue item)
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            void ICollection<TValue>.Clear()
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            bool ICollection<TValue>.Remove(TValue item)
            {
                throw new NotSupportedException();
            }

            #endregion
        }

        #endregion

        #region class Enumerator

        private class Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>, IEnumerable<KeyValuePair<TKey, TValue>>
        {
            private readonly Predicate<KeyValuePair<TKey, TValue>> _fnContinue;
            private readonly Node _start;
            private readonly int _startIx;
            private Node _current;
            private int _index;
            private bool _valid;

            public Enumerator(Node start, int index) : this(start, index, null)
            {
            }

            public Enumerator(Node start, int index, Predicate<KeyValuePair<TKey, TValue>> fnContinue)
            {
                _start = start;
                _startIx = index;
                _fnContinue = fnContinue;
                Reset();
            }

            [Obsolete]
            IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            {
                return this;
            }

            [Obsolete]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
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

            [Obsolete]
            object IEnumerator.Current => Current;

            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if (_valid)
                        return _current.Values[_index];
                    throw new InvalidOperationException();
                }
            }

            public void Dispose()
            {
            }
        }

        #endregion

        #region class KvComparer

        private class KvComparer : IComparer<KeyValuePair<TKey, TValue>>, IComparer<KeyValuePair<TKey, Node>>
        {
            private readonly IComparer<TKey> _comparer;

            public KvComparer(IComparer<TKey> comparer)
            {
                _comparer = comparer;
            }

            int IComparer<KeyValuePair<TKey, Node>>.Compare(KeyValuePair<TKey, Node> x, KeyValuePair<TKey, Node> y)
            {
                return _comparer.Compare(x.Key, y.Key);
            }

            int IComparer<KeyValuePair<TKey, TValue>>.Compare(KeyValuePair<TKey, TValue> x,
                KeyValuePair<TKey, TValue> y)
            {
                return _comparer.Compare(x.Key, y.Key);
            }
        }

        #endregion

        #region class Node

        [DebuggerDisplay("Count = {Count}")]
        private class Node
        {
            public Node Next, Prev;

            public Node(int order)
            {
                Count = 0;
                Values = new KeyValuePair<TKey, TValue>[order];
            }

            public Node(int order, TKey key, Node node)
            {
                Count = 1;
                Children = new KeyValuePair<TKey, Node>[order];
                Children[0] = new KeyValuePair<TKey, Node>(key, node);
            }

            public Node(int order, Node split, out TKey splitKey, ref Node first, ref Node last)
            {
                if (split.Values != null)
                {
                    Values = new KeyValuePair<TKey, TValue>[order];
                    int half = split.Count >> 1;
                    Array.Copy(split.Values, half, Values, 0, Count = split.Count - half);
                    Array.Clear(split.Values, half, Count);
                    split.Count = half;
                    splitKey = Values[0].Key;

                    Prev = split;
                    Node oldNext = split.Next;
                    split.Next = this;
                    if ((Next = oldNext) == null)
                        last = this;
                    else
                        Next.Prev = this;
                }
                else //if (split.Children != null)
                {
                    Children = new KeyValuePair<TKey, Node>[order];
                    int half = split.Count >> 1;
                    Array.Copy(split.Children, half, Children, 0, Count = split.Count - half);
                    Array.Clear(split.Children, half, Count);
                    split.Count = half;
                    splitKey = Children[0].Key;
                }
            }

            public int Count { get; private set; }

            public bool IsLeaf => Values != null;
            public KeyValuePair<TKey, Node>[] Children { get; private set; }

            public KeyValuePair<TKey, TValue>[] Values { get; private set; }

            public void Add(int ix, KeyValuePair<TKey, TValue> kv)
            {
                if (ix < Count)
                    Array.Copy(Values, ix, Values, ix + 1, Count - ix);
                Values[ix] = kv;
                Count++;
            }

            public void Add(int ix, TKey key, Node child)
            {
                if (ix < Count)
                    Array.Copy(Children, ix, Children, ix + 1, Count - ix);
                Children[ix] = new KeyValuePair<TKey, Node>(key, child);
                Count++;
            }

            public void RemoveValue(int ix)
            {
                Values[ix] = new KeyValuePair<TKey, TValue>();
                Count--;
                if (ix < Count)
                {
                    Array.Copy(Values, ix + 1, Values, ix, Count - ix);
                    Values[Count] = new KeyValuePair<TKey, TValue>();
                }
            }

            private void RemoveChild(int ix, ref Node first, ref Node last)
            {
                Node child = Children[ix].Value;
                if (child != null)
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
                Children[ix] = new KeyValuePair<TKey, Node>();
                Count--;
                if (ix < Count)
                {
                    Array.Copy(Children, ix + 1, Children, ix, Count - ix);
                    Children[Count] = new KeyValuePair<TKey, Node>();
                }
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
                    first.Count += second.Count;
                    RemoveChild(secondIx, ref nodeFirst, ref nodeLast);
                }
                else if (first.IsLeaf)
                {
                    KeyValuePair<TKey, TValue>[] list = new KeyValuePair<TKey, TValue>[total];
                    Array.Copy(first.Values, 0, list, 0, first.Count);
                    Array.Copy(second.Values, 0, list, first.Count, second.Count);
                    Array.Clear(first.Values, 0, first.Count);
                    Array.Clear(second.Values, 0, second.Count);

                    int half = total >> 1;
                    Array.Copy(list, 0, first.Values, 0, first.Count = half);
                    Array.Copy(list, half, second.Values, 0, second.Count = total - half);
                    Children[secondIx] = new KeyValuePair<TKey, Node>(second.Values[0].Key, second);
                }
                else //if (first.IsLeaf == false)
                {
                    KeyValuePair<TKey, Node>[] list = new KeyValuePair<TKey, Node>[total];
                    Array.Copy(first.Children, 0, list, 0, first.Count);
                    Array.Copy(second.Children, 0, list, first.Count, second.Count);
                    Array.Clear(first.Children, 0, first.Count);
                    Array.Clear(second.Children, 0, second.Count);
                    list[first.Count] = new KeyValuePair<TKey, Node>(Children[secondIx].Key, list[first.Count].Value);

                    int half = total >> 1;
                    Array.Copy(list, 0, first.Children, 0, first.Count = half);
                    Array.Copy(list, half, second.Children, 0, second.Count = total - half);
                    Children[secondIx] = new KeyValuePair<TKey, Node>(second.Children[0].Key, second);
                }
            }

            public Node Clone(ref Node first, ref Node last)
            {
                Node copy = (Node) MemberwiseClone();
                if (copy.IsLeaf)
                {
                    copy.Values = (KeyValuePair<TKey, TValue>[]) copy.Values.Clone();

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
                    copy.Children = new KeyValuePair<TKey, Node>[copy.Children.Length];
                    for (int i = 0; i < copy.Count; i++)
                        copy.Children[i] = new KeyValuePair<TKey, Node>(
                            Children[i].Key,
                            Children[i].Value.Clone(ref first, ref last)
                        );
                }
                return copy;
            }
        }

        #endregion

        #region IDictionary<TKey,TValue> Members

        /// <summary>
        ///     Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The Collection is read-only.</exception>
        public void Clear()
        {
            Modify();
            Count = 0;
            _root = new Node(_order);
            _first = _last = _root;
        }

        /// <summary>
        ///     Gets or sets the element with the specified key.
        /// </summary>
        /// <returns>
        ///     The element with the specified key.
        /// </returns>
        /// <param name="key">The key of the element to get or set.</param>
        /// <exception cref="T:System.Collections.Generic.KeyNotFoundException">
        ///     The property is retrieved and
        ///     <paramref name="key" /> is not found.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        ///     The property is set and the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" /> is read-only.
        /// </exception>
        public TValue this[TKey key]
        {
            get
            {
                SeekResult result;
                if (Seek(_root, key, out result))
                    return result.Value;
                throw new KeyNotFoundException();
            }
            set
            {
                Modify();
                if (!Add(null, -1, _root, new KeyValuePair<TKey, TValue>(key, value), false))
                    throw new KeyNotFoundException();
            }
        }

        /// <summary>
        ///     Gets the value associated with the specified key.
        /// </summary>
        /// <returns>
        ///     true if the object that implements <see cref="T:System.Collections.Generic.IDictionary`2" /> contains an element
        ///     with the specified key; otherwise, false.
        /// </returns>
        public bool TryGetValue(TKey key, out TValue value)
        {
            SeekResult result;
            if (Seek(_root, key, out result))
            {
                value = result.Value;
                return true;
            }
            value = default(TValue);
            return false;
        }

        /// <summary>
        ///     Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        /// <param name="key">The object to use as the key of the element to add.</param>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.ArgumentException">
        ///     An element with the same key already exists in the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public void Add(TKey key, TValue value)
        {
            Modify();
            Add(new KeyValuePair<TKey, TValue>(key, value));
        }

        /// <summary>
        ///     Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </summary>
        /// <param name="item">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
        /// <exception cref="T:System.ArgumentException">
        ///     An element with the same key already exists in the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Modify();
            if (!Add(null, -1, _root, item, true))
                throw new ArgumentException("An item with the same key has already been added.");
        }

        /// <summary>
        ///     Adds a set of items to the <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </summary>
        /// <param name="items">The items to add to the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
        /// <exception cref="T:System.ArgumentException">
        ///     An element with the same key already exists in the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </exception>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public void AddRange(IEnumerable<KeyValuePair<TKey, TValue>> items)
        {
            Modify();
            foreach (KeyValuePair<TKey, TValue> pair in items)
                Add(pair);
        }

        /// <summary>
        ///     Determines whether the <see cref="T:System.Collections.Generic.IDictionary`2" /> contains an element with the
        ///     specified key.
        /// </summary>
        /// <returns>
        ///     true if the <see cref="T:System.Collections.Generic.IDictionary`2" /> contains an element with the key; otherwise,
        ///     false.
        /// </returns>
        public bool ContainsKey(TKey key)
        {
            SeekResult result;
            return Seek(_root, key, out result);
        }

        /// <summary>
        ///     Determines whether the <see cref="T:System.Collections.Generic.ICollection`1" /> contains a specific key and value
        ///     pair.
        /// </summary>
        /// <returns>
        ///     true if <paramref name="item" /> is found in the <see cref="T:System.Collections.Generic.ICollection`1" />;
        ///     otherwise, false.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            SeekResult result;
            return Seek(_root, item.Key, out result) &&
                   EqualityComparer<TValue>.Default.Equals(item.Value, result.Value);
        }

        /// <summary>
        ///     Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        /// <returns>
        ///     true if the element is successfully removed; otherwise, false.  This method also returns false if
        ///     <paramref name="key" /> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </returns>
        /// <param name="key">The key of the element to remove.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool Remove(TKey key)
        {
            Modify();
            KeyValuePair<TKey, TValue> item = new KeyValuePair<TKey, TValue>(key, default(TValue));
            return Remove(null, -1, _root, ref item);
        }

        /// <summary>
        ///     Removes the first occurrence of a specific object from the
        ///     <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </summary>
        /// <returns>
        ///     true if <paramref name="item" /> was successfully removed from the
        ///     <see cref="T:System.Collections.Generic.ICollection`1" />; otherwise, false.
        ///     This method also returns false if <paramref name="item" /> is not found in the original
        ///     <see cref="T:System.Collections.Generic.ICollection`1" />.
        /// </returns>
        /// <param name="item">The object to remove from the <see cref="T:System.Collections.Generic.ICollection`1" />.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.ICollection`1" /> is
        ///     read-only.
        /// </exception>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            Modify();
            SeekResult result;
            if (Seek(_root, item.Key, out result) &&
                EqualityComparer<TValue>.Default.Equals(item.Value, result.Value))
                return Remove(null, -1, _root, ref item);
            return false;
        }

        /// <summary>
        ///     Gets an <see cref="T:System.Collections.Generic.ICollection`1" /> containing the keys of the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public ICollection<TKey> Keys => _keys != null ? _keys : (_keys = new KeyCollection(this));

        /// <summary>
        ///     Gets an <see cref="T:System.Collections.Generic.ICollection`1" /> containing the values in the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public ICollection<TValue> Values => _values != null ? _values : (_values = new ValueCollection(this));

        /// <summary>
        ///     Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1" /> to an
        ///     <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
        /// </summary>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException();
            if (arrayIndex < 0 || array.Length - arrayIndex < Count)
                throw new ArgumentOutOfRangeException();

            Node node = _first;
            while (node != null)
            {
                Array.Copy(node.Values, 0, array, arrayIndex, node.Count);
                arrayIndex += node.Count;
                node = node.Next;
            }
        }

        /// <summary>
        ///     Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new Enumerator(_first, 0);
        }

        [Obsolete]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Public Members

        /// <summary>
        ///     Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2" /> if the key does not already
        ///     exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value to be added, if the key does not already exist.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public TValue GetOrAdd(TKey key, TValue value)
        {
            Modify();
            SeekResult result;
            if (Seek(_root, key, out result))
                return result.Value;
            Add(key, value);
            return value;
        }

        /// <summary>
        ///     Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        /// <param name="key">The object to use as the key of the element to add.</param>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool TryAdd(TKey key, TValue value)
        {
            Modify();
            return Add(null, -1, _root, new KeyValuePair<TKey, TValue>(key, value), true);
        }

        /// <summary>
        ///     Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool TryUpdate(TKey key, TValue value)
        {
            Modify();
            SeekResult result;
            if (Seek(_root, key, out result))
            {
                result.Parent.Values[result.ParentIx] = new KeyValuePair<TKey, TValue>(key, value);
                return true;
            }
            return false;
        }

        /// <summary>
        ///     Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <param name="comparisonValue">The value that is compared to the value of the element with key.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool TryUpdate(TKey key, TValue value, TValue comparisonValue)
        {
            Modify();
            SeekResult result;
            if (Seek(_root, key, out result) &&
                EqualityComparer<TValue>.Default.Equals(result.Parent.Values[result.ParentIx].Value, comparisonValue))
            {
                result.Parent.Values[result.ParentIx] = new KeyValuePair<TKey, TValue>(key, value);
                return true;
            }
            return false;
        }

        /// <summary>
        ///     Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        /// <returns>
        ///     true if the element is successfully removed; otherwise, false.  This method also returns false if
        ///     <paramref name="key" /> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </returns>
        /// <param name="key">The key of the element to remove.</param>
        /// <param name="value">The value that was removed.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool TryRemove(TKey key, out TValue value)
        {
            Modify();
            KeyValuePair<TKey, TValue> item = new KeyValuePair<TKey, TValue>(key, default(TValue));
            if (Remove(null, -1, _root, ref item))
            {
                value = item.Value;
                return true;
            }
            value = default(TValue);
            return false;
        }

        /// <summary>
        ///     Returns all the items of this collection as an array of  <see cref="T:System.Collections.Generic.KeyValuePair`2" />
        ///     .
        /// </summary>
        public KeyValuePair<TKey, TValue>[] ToArray()
        {
            KeyValuePair<TKey, TValue>[] array = new KeyValuePair<TKey, TValue>[Count];
            CopyTo(array, 0);
            return array;
        }

        /// <summary>
        ///     Returns the first key and it's associated value.
        /// </summary>
        public KeyValuePair<TKey, TValue> First()
        {
            KeyValuePair<TKey, TValue> result;
            if (!TryGetFirst(out result))
                throw new InvalidOperationException();
            return result;
        }

        /// <summary>
        ///     Returns the first key and it's associated value.
        /// </summary>
        public bool TryGetFirst(out KeyValuePair<TKey, TValue> item)
        {
            if (Count == 0)
            {
                item = default(KeyValuePair<TKey, TValue>);
                return false;
            }
            item = _first.Values[0];
            return true;
        }

        /// <summary>
        ///     Returns the last key and it's associated value.
        /// </summary>
        public KeyValuePair<TKey, TValue> Last()
        {
            KeyValuePair<TKey, TValue> result;
            if (!TryGetLast(out result))
                throw new InvalidOperationException();
            return result;
        }

        /// <summary>
        ///     Returns the last key and it's associated value.
        /// </summary>
        public bool TryGetLast(out KeyValuePair<TKey, TValue> item)
        {
            if (Count == 0)
            {
                item = default(KeyValuePair<TKey, TValue>);
                return false;
            }
            item = _last.Values[_last.Count - 1];
            return true;
        }

        /// <summary>
        ///     Inclusivly enumerates from start key to the end of the collection
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> EnumerateFrom(TKey start)
        {
            SeekResult result;
            Seek(_root, start, out result);
            return new Enumerator(result.Parent, result.ParentIx);
        }

        /// <summary>
        ///     Inclusivly enumerates from start key to stop key
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> EnumerateRange(TKey start, TKey end)
        {
            SeekResult result;
            Seek(_root, start, out result);
            return new Enumerator(result.Parent, result.ParentIx, x => Comparer.Compare(x.Key, end) <= 0);
        }

        object ICloneable.Clone()
        {
            return Clone();
        }

        /// <summary>
        ///     Returns a writable clone of this collection.
        /// </summary>
        public BTreeDictionary<TKey, TValue> Clone()
        {
            BTreeDictionary<TKey, TValue> copy = (BTreeDictionary<TKey, TValue>) MemberwiseClone();
            copy._first = copy._last = null;
            copy._root = copy._root.Clone(ref copy._first, ref copy._last);
            copy.IsReadOnly = false;
            return copy;
        }

        /// <summary>
        ///     Returns a read-only clone of this collection.  If this instance is already read-only the method will return this.
        /// </summary>
        public BTreeDictionary<TKey, TValue> MakeReadOnly()
        {
            if (IsReadOnly) return this;
            BTreeDictionary<TKey, TValue> copy = Clone();
            copy.IsReadOnly = true;
            return copy;
        }

        #endregion

        #region Implementation Details

        private void Modify()
        {
            if (IsReadOnly) throw new NotSupportedException("Collection is read-only.");
        }

        private struct SeekResult
        {
            public TValue Value;
            public Node Parent;
            public int ParentIx;
        }

        private bool Seek(Node from, TKey item, out SeekResult found)
        {
            found = new SeekResult();
            found.Value = default(TValue);
            found.Parent = from;
            found.ParentIx = -1;

            KeyValuePair<TKey, Node> seek = new KeyValuePair<TKey, Node>(item, null);
            while (!found.Parent.IsLeaf)
            {
                int ix = Array.BinarySearch(found.Parent.Children, 1, found.Parent.Count - 1, seek, _kvcomparer);
                if (ix < 0 && (ix = ~ix) > 0)
                    ix--;
                found.Parent = found.Parent.Children[ix].Value;
            }

            found.ParentIx = Array.BinarySearch(found.Parent.Values, 0, found.Parent.Count,
                new KeyValuePair<TKey, TValue>(item, found.Value), _kvcomparer);
            if (found.ParentIx < 0)
            {
                found.ParentIx = ~found.ParentIx;
                return false;
            }

            found.Value = found.Parent.Values[found.ParentIx].Value;
            return true;
        }

        private bool Add(Node parent, int myIx, Node me, KeyValuePair<TKey, TValue> item, bool adding)
        {
            if (me.Count == _order)
            {
                if (parent == null)
                {
                    _root = parent = new Node(_order, default(TKey), me);
                    myIx = 0;
                }
                TKey split;
                Node next = new Node(_order, me, out split, ref _first, ref _last);
                parent.Add(myIx + 1, split, next);
                if (Comparer.Compare(split, item.Key) <= 0)
                    me = next;
            }
            int ix;
            if (me.IsLeaf)
            {
                ix = Array.BinarySearch(me.Values, 0, me.Count, item, _kvcomparer);
                if (ix < 0)
                {
                    ix = ~ix;
                }
                else
                {
                    if (adding) return false;
                    me.Values[ix] = item;
                    return true;
                }
                me.Add(ix, item);
                Count++;
                return true;
            }

            KeyValuePair<TKey, Node> seek = new KeyValuePair<TKey, Node>(item.Key, null);
            ix = Array.BinarySearch(me.Children, 1, me.Count - 1, seek, _kvcomparer);
            if (ix < 0 && (ix = ~ix) > 0)
                ix--;
            return Add(me, ix, me.Children[ix].Value, item, adding);
        }

        private bool Remove(Node parent, int myIx, Node me, ref KeyValuePair<TKey, TValue> item)
        {
            if (parent == null && ReferenceEquals(me, _root) && me.Count == 1 && me.IsLeaf == false)
                _root = me.Children[0].Value;
            if (parent != null && me.Count <= _order >> 2)
            {
                if (myIx == 0)
                    parent.Join(myIx, myIx + 1, ref _first, ref _last);
                else
                    parent.Join(myIx - 1, myIx, ref _first, ref _last);
                return Remove(null, -1, parent, ref item);
            }

            int ix;
            if (me.IsLeaf)
            {
                ix = Array.BinarySearch(me.Values, 0, me.Count, item, _kvcomparer);
                if (ix < 0)
                    return false;

                item = me.Values[ix];
                me.RemoveValue(ix);
                Count--;
                return true;
            }

            KeyValuePair<TKey, Node> seek = new KeyValuePair<TKey, Node>(item.Key, null);
            ix = Array.BinarySearch(me.Children, 1, me.Count - 1, seek, _kvcomparer);
            if (ix < 0 && (ix = ~ix) > 0)
                ix--;

            Node child = me.Children[ix].Value;
            if (!Remove(me, ix, child, ref item))
                return false;

            return true;
        }

        #endregion
    }
}