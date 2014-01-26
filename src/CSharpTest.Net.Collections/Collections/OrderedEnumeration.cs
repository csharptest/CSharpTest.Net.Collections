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
using System.IO;
using System.Collections.Generic;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    /// <summary> Defines how duplicate keys are handled </summary>
    public enum DuplicateHandling
    {
        /// <summary> Do nothing and pass-through all duplicates </summary>
        None = 0,
        /// <summary> Remove all but the first item of duplicates </summary>
        FirstValueWins,
        /// <summary> Remove all but the last item of duplicates </summary>
        LastValueWins,
        /// <summary> Throw an error on duplicates </summary>
        RaisesException,
    }

    /// <summary>
    /// Creates an ordered enumeration from an unordered enumeration by paginating the data, sorting the page,
    /// and then performing a binary-tree grouped mergesort on the resulting pages.  When the page size (memoryLimit)
    /// is hit, the page will be unloaded to disk and restored on demand if a serializer is provided.
    /// </summary>
    public class OrderedEnumeration<T> : IEnumerable<T>
    {
        private const int DefaultLimit = 0x10000;
        private const int LimitMax = int.MaxValue;
        private readonly IEnumerable<T> _unordered;
        private IComparer<T> _comparer;
        private ISerializer<T> _serializer;
        private int _memoryLimit;
        private DuplicateHandling _duplicateHandling;
        private bool _enumerated;

        /// <summary> Constructs an ordered enumeration from an unordered enumeration </summary>
        public OrderedEnumeration(IEnumerable<T> unordered) : this(Comparer<T>.Default, unordered, null, DefaultLimit) { }
        /// <summary> Constructs an ordered enumeration from an unordered enumeration </summary>
        public OrderedEnumeration(IComparer<T> comparer, IEnumerable<T> unordered) : this(comparer, unordered, null, DefaultLimit) { }
        /// <summary> Constructs an ordered enumeration from an unordered enumeration </summary>
        public OrderedEnumeration(IComparer<T> comparer, IEnumerable<T> unordered, ISerializer<T> serializer) : this(comparer, unordered, serializer, DefaultLimit) { }
        /// <summary> Constructs an ordered enumeration from an unordered enumeration </summary>
        public OrderedEnumeration(IComparer<T> comparer, IEnumerable<T> unordered, ISerializer<T> serializer, int memoryLimit)
        {
            _enumerated = false;
            _comparer = comparer;
            _unordered = unordered;
            _serializer = serializer;
            _memoryLimit = Check.InRange(memoryLimit, 1, LimitMax);
            _duplicateHandling = DuplicateHandling.None;
        }

        /// <summary>
        /// Gets or sets the comparer to use when ordering the items.
        /// </summary>
        public IComparer<T> Comparer
        {
            get { return _comparer; }
            set { _comparer = value ?? Comparer<T>.Default; }
        }

        /// <summary>
        /// Gets or sets the serializer to use when paging to disk.
        /// </summary>
        public ISerializer<T> Serializer
        {
            get { return _serializer; }
            set { _serializer = value; }
        }

        /// <summary>
        /// Gets or sets the number of instances to keep in memory before sorting/paging to disk.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">You must specify the Serializer before setting this property</exception>
        public int InMemoryLimit
        {
            get { return _memoryLimit; }
            set { _memoryLimit = Check.InRange(value, 1, LimitMax); }
        }

        /// <summary> Gets or sets the duplicate item handling policy </summary>
        public DuplicateHandling DuplicateHandling
        {
            get { return _duplicateHandling; }
            set { _duplicateHandling = value; }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">GetEnumerator() may only be called once.</exception>
        /// <exception cref="System.IO.InvalidDataException">Enumeration is out of sequence.</exception>
        /// <exception cref="System.ArgumentException">Duplicate item in enumeration.</exception>
        public IEnumerator<T> GetEnumerator()
        {
            if (_enumerated)
                throw new InvalidOperationException();
            _enumerated = true;
            return new OrderedEnumerator(PagedAndOrdered(), _comparer, _duplicateHandling);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        { return GetEnumerator(); }

        private IEnumerable<T> PagedAndOrdered()
        {
            T[] items = new T[Math.Min(InMemoryLimit, 2048)];
            using (DisposingList resources = new DisposingList())
            {
                List<IEnumerable<T>> orderedSet = new List<IEnumerable<T>>();
                int count = 0;

                foreach (T item in _unordered)
                {
                    if (_memoryLimit > 0 && count == _memoryLimit)
                    {
                        if (_serializer != null)
                        {
                            TempFile temp = new TempFile();
                            resources.Add(temp);
                            Stream io;
                            resources.Add(io = new FileStream(temp.TempPath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read));

                            MergeSort.Sort(items, _comparer);
                            foreach (T i in items)
                                _serializer.WriteTo(i, io);
                            io.Position = 0;
                            orderedSet.Add(Read(temp, io));
                        }
                        else
                        {
                            T[] copy;
                            MergeSort.Sort(items, out copy, 0, items.Length, _comparer);
                            orderedSet.Add(items);
                            items = copy;
                        }
                        Array.Clear(items, 0, items.Length);
                        count = 0;
                    }

                    if (count == items.Length)
                        Array.Resize(ref items, Math.Min(InMemoryLimit, items.Length * 2));
                    items[count++] = item;
                }

                if (count != items.Length)
                    Array.Resize(ref items, count);

                MergeSort.Sort(items, _comparer);

                IEnumerable<T> result;
                if (orderedSet.Count == 0)
                    result = items;
                else
                {
                    orderedSet.Add(items);
                    result = Merge(_comparer, orderedSet.ToArray());
                }

                foreach (T item in result)
                    yield return item;
            }
        }

        private IEnumerable<T> Read(TempFile file, Stream io)
        {
            using (file)
            using (io)
            {
                for (int i = 0; i < _memoryLimit; i++)
                    yield return _serializer.ReadFrom(io);
            }
        }
        #region static Merge(...)
        /// <summary>
        /// Merges two ordered enumerations based on the comparer provided.
        /// </summary>
        public static IEnumerable<T> Merge(IComparer<T> comparer, IEnumerable<T> x, IEnumerable<T> y)
        {
            using(IEnumerator<T> left = x.GetEnumerator())
            using(IEnumerator<T> right = y.GetEnumerator())
            {
                bool lvalid = left.MoveNext();
                bool rvalid = right.MoveNext();
                while(lvalid || rvalid)
                {
                    int cmp = !rvalid ? -1 : !lvalid ? 1 : comparer.Compare(left.Current, right.Current);
                    if (cmp <= 0)
                    {
                        yield return left.Current;
                        lvalid = left.MoveNext();
                    }
                    else
                    {
                        yield return right.Current;
                        rvalid = right.MoveNext();
                    }
                }
            }
        }
        /// <summary>
        /// Merges n-number of ordered enumerations based on the default comparer of T.
        /// </summary>
        public static IEnumerable<T> Merge(params IEnumerable<T>[] enums)
        {
            return Merge(Comparer<T>.Default, 0, enums.Length, enums);
        }
        /// <summary>
        /// Merges n-number of ordered enumerations based on the comparer provided.
        /// </summary>
        public static IEnumerable<T> Merge(IComparer<T> comparer, params IEnumerable<T>[] enums)
        {
            return Merge(comparer, 0, enums.Length, enums);
        }
        /// <summary>
        /// Merges n-number of ordered enumerations based on the comparer provided.
        /// </summary>
        public static IEnumerable<T> Merge(IComparer<T> comparer, DuplicateHandling duplicateHandling, params IEnumerable<T>[] enums)
        {
            return WithDuplicateHandling(Merge(comparer, enums), comparer, duplicateHandling);
        }

        private static IEnumerable<T> Merge(IComparer<T> comparer, int start, int count, IEnumerable<T>[] enums)
        {
            if (count <= 0)
                return new T[0];
            if (count == 1)
                return enums[start];
            if (count == 2)
                return Merge(comparer, enums[start], enums[start + 1]);

            int half = count/2;
            return Merge(comparer, 
                Merge(comparer, start, half, enums),
                Merge(comparer, start + half, count - half, enums)
            );
        }
        #endregion

        private class OrderedEnumerator : IEnumerator<T>
        {
            private readonly IEnumerable<T> _ordered;
            private IEnumerator<T> _enumerator;
            private readonly IComparer<T> _comparer;
            private readonly DuplicateHandling _duplicateHandling;

            private bool _isValid, _hasNext, _isFirst;
            private T _current;
            private T _next;

            public OrderedEnumerator(
                IEnumerable<T> enumerator,
                IComparer<T> comparer,
                DuplicateHandling duplicateHandling)
            {
                _ordered = enumerator;
                _enumerator = null;
                _comparer = comparer;
                _duplicateHandling = duplicateHandling;
                _isFirst = true;
            }

            public void Dispose()
            {
                if (_enumerator != null)
                    _enumerator.Dispose();
                _enumerator = null;
            }

            public bool MoveNext()
            {
                if (_isFirst)
                {
                    _isFirst = false;
                    _enumerator = _ordered.GetEnumerator();
                    _hasNext = _enumerator.MoveNext();
                    if (_hasNext)
                        _next = _enumerator.Current;
                }
                _isValid = _hasNext;
                _current = _next;

                if (!_isValid)
                    return false;

// ReSharper disable RedundantBoolCompare
                while ((_hasNext = _enumerator.MoveNext()) == true)
// ReSharper restore RedundantBoolCompare
                {
                    _next = _enumerator.Current;
                    int cmp = _comparer.Compare(_current, _next);
                    if (cmp > 0)
                        throw new InvalidDataException("Enumeration out of sequence.");
                    if (cmp != 0 || _duplicateHandling == DuplicateHandling.None)
                        break;

                    if (_duplicateHandling == DuplicateHandling.RaisesException)
                        throw new ArgumentException("Duplicate item in enumeration.");
                    if (_duplicateHandling == DuplicateHandling.LastValueWins)
                        _current = _next;
                }
                if (!_hasNext)
                    _next = default(T);

                return true;
            }

            public T Current
            {
                get
                {
                    if (!_isValid)
                        throw new InvalidOperationException();
                    return _current;
                }
            }

            object System.Collections.IEnumerator.Current { get { return Current; } }

            void System.Collections.IEnumerator.Reset()
            { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Wraps an existing enumeration of Key/value pairs with an assertion about ascending order and handling
        /// for duplicate keys.
        /// </summary>
        public static IEnumerable<T> WithDuplicateHandling(
            IEnumerable<T> items, IComparer<T> comparer, DuplicateHandling duplicateHandling)
        {
            return new EnumWithDuplicateKeyHandling(items, comparer, duplicateHandling);
        }

        private class EnumWithDuplicateKeyHandling : IEnumerable<T>
        {
            private readonly IEnumerable<T> _items;
            private readonly IComparer<T> _comparer;
            private readonly DuplicateHandling _duplicateHandling;

            public EnumWithDuplicateKeyHandling(IEnumerable<T> items, IComparer<T> comparer, DuplicateHandling duplicateHandling)
            {
                _items = items;
                _comparer = comparer;
                _duplicateHandling = duplicateHandling;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new OrderedEnumerator(_items, _comparer, _duplicateHandling);
            }
            [Obsolete]
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
