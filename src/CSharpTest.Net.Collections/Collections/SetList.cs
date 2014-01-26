#region Copyright 2009-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.Collections
{
	/// <summary> Represents an immutable collection of unique items that can be manipulated as a set, intersect/union/etc. </summary>
	[System.Diagnostics.DebuggerDisplay("{ToArray()}")]
    public class SetList<T> : IList<T>, ICollection<T>, IEnumerable<T>, IList, ICollection, IEnumerable, ICloneable<SetList<T>>
	{
		private static SetList<T> __empty;
		/// <summary> Provides an empty set </summary>
        public static SetList<T> EmptySet { get { return __empty ?? (__empty = new SetList<T>(0, new T[0], Comparer<T>.Default)); } }

		private readonly List<T> _list;
		private readonly IComparer<T> _comparer;

		/// <summary> Constructs a SetList </summary>
		public SetList() /*												*/ : this(0, EmptySet, Comparer<T>.Default) { }
		/// <summary> Constructs a SetList </summary>
        public SetList(IComparer<T> comparer) /*						*/ : this(0, EmptySet, comparer) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(int capacity) /*									*/ : this(capacity, EmptySet, Comparer<T>.Default) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(int capacity, IComparer<T> comparer) /*			*/ : this(capacity, EmptySet, comparer) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(IEnumerable<T> items) /*							*/ : this(0, items, Comparer<T>.Default) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(ICollection<T> items) /*							*/ : this(items.Count, items, Comparer<T>.Default) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(IEnumerable<T> items, IComparer<T> comparer) /*	*/ : this(0, items, comparer) { }
		/// <summary> Constructs a SetList </summary>
		public SetList(ICollection<T> items, IComparer<T> comparer) /*	*/ : this(items.Count, items, comparer) { }

		private SetList(int capacity, IEnumerable<T> items, IComparer<T> comparer)
		{
			if (capacity < 0) throw new ArgumentOutOfRangeException("capacity");
			if (items == null) throw new ArgumentNullException("items");
			if (comparer == null) throw new ArgumentNullException("comparer");

			_comparer = comparer;
			_list = new List<T>(capacity);
            AddRange(items);
		}

		private SetList(List<T> items, IComparer<T> comparer)
		{
			_list = items;
			_comparer = comparer;
		}

		#region IList<T> like members...

		/// <summary> Access an item by it's ordinal offset in the list </summary>
		public T this[int index] 
        {
            get { return _list[index]; }
            set { throw new NotSupportedException(); }
        }

        /// <summary> Returns true if the list is read-only </summary>
        public bool IsReadOnly { get { return false; } }

		/// <summary> Returns the count of items in the list </summary>
		public int Count { get { return _list.Count; } }

		/// <summary> Returns the zero-based index of the item or -1 </summary>
		public int IndexOf(T item)
		{
			int pos = _list.BinarySearch(item, _comparer);
			return pos >= 0 ? pos : -1;
		}

		/// <summary> Returns true if the item is already in the collection </summary>
		public bool Contains(T item)
		{
			int pos = _list.BinarySearch(item, _comparer);
			return pos >= 0;
		}

		/// <summary> Copy the collection to an array </summary>
		public void CopyTo(T[] array, int arrayIndex)
		{
			_list.CopyTo(array, arrayIndex);
		}

		/// <summary> Returns this collection as an array </summary>
		public T[] ToArray() { return _list.ToArray(); }

		#endregion

		#region SetList manipulation: Add, Remove, IntersectWith, UnionWith, ComplementOf, RemoveAll, ExclusiveOrWith

        /// <summary> Removes all items from the collection </summary>
        public void Clear()
        {
            _list.Clear();
        }
        
		/// <summary> Returns a new collection adding the item provided </summary>
		public void Add(T item)
		{
			int pos;
            Add(item, out pos);
        }

        /// <summary> Returns a new collection adding the item provided </summary>
        public void Add(T item, out int index)
        {
            int pos = _list.BinarySearch(item, _comparer);
            if (pos < 0)
            {
                _list.Insert(~pos, item);
                index = ~pos;
            }
            else 
                index = pos;
        }

        /// <summary> Adds a range of items to the collection </summary>
        public void AddRange(IEnumerable<T> other)
        {
            int pos;
            foreach (T item in other)
            {
                pos = _list.BinarySearch(item, _comparer);
                if (pos < 0)
                    _list.Insert(~pos, item);
            }
        }

		/// <summary> Adds or replaces an item in the collection, returns true if an entry was replaced </summary>
		public bool Replace(T item)
		{
			int pos = _list.BinarySearch(item, _comparer);
			if (pos < 0)
				_list.Insert(~pos, item);
			else
				_list[pos] = item;

			return pos >= 0;
		}

		/// <summary> Adds or replaces an item in the collection, returns true if any item was replaced </summary>
		public bool ReplaceAll(IEnumerable<T> items)
		{
			bool exists = false;
			foreach (T item in items)
			{
				int pos = _list.BinarySearch(item, _comparer);
				exists |= pos >= 0;
				if (pos < 0)
					_list.Insert(~pos, item);
				else
					_list[pos] = item;
			}
			return exists;
		}

		/// <summary> Not supported, the list is sorted. </summary>
        void IList<T>.Insert(int index, T item)
        { throw new NotSupportedException(); }

		/// <summary> Returns a new collection with the item provided removed </summary>
		public bool Remove(T item)
		{
			int pos = _list.BinarySearch(item, _comparer);
            if (pos >= 0)
            {
                RemoveAt(pos);
                return true;
            }
            return false;
		}

        /// <summary> Removes an item by it's ordinal index in the collection </summary>
        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        /// <summary> Removes the items in this set that are not in the provided set </summary>
        /// <example>{ 1, 2, 3 }.RemoveAll({ 2, 3, 4 }) == { 1 }</example>
        public void RemoveAll(IEnumerable<T> other)
        {
            int pos;
            foreach (T item in other)
            {
                pos = _list.BinarySearch(item, _comparer);
                if (pos >= 0)
                    _list.RemoveAt(pos);
            }
        }

		/// <summary> Returns the set of items that are in both this set and the provided set </summary>
		/// <example>{ 1, 2, 3 }.IntersectWith({ 2, 3, 4 }) == { 2, 3 }</example>
		public SetList<T> IntersectWith(SetList<T> other)
		{
			List<T> result = new List<T>(Math.Max(0, _list.Count - other._list.Count));
			int pos;
			foreach (T item in other._list)
			{
				pos = _list.BinarySearch(item, _comparer);
				if (pos >= 0)
					result.Add(item);
			}
			return new SetList<T>(result, _comparer);
		}

		/// <summary> Returns the set of items that are in either this set or the provided set </summary>
		/// <example>{ 1, 2, 3 }.UnionWith({ 2, 3, 4 }) == { 1, 2, 3, 4 }</example>
		public SetList<T> UnionWith(SetList<T> other)
        {
            SetList<T> copy = this.Clone();
            copy.AddRange(other._list);
            return copy;
		}

		/// <summary> Returns the items in the provided set that are not in this set </summary>
		/// <example>{ 1, 2, 3 }.ComplementOf({ 2, 3, 4 }) == { 4 }</example>
		public SetList<T> ComplementOf(SetList<T> other)
		{
			List<T> result = new List<T>(other._list.Count);
			int pos;
			foreach (T item in other._list)
			{
				pos = _list.BinarySearch(item, _comparer);
				if (pos < 0)
					result.Add(item);
			}
			return new SetList<T>(result, _comparer);
		}

		/// <summary> Returns the items in this set that are not in the provided set </summary>
		/// <example>{ 1, 2, 3 }.RemoveAll({ 2, 3, 4 }) == { 1 }</example>
		public SetList<T> SubtractSet(SetList<T> other)
		{
			List<T> result = new List<T>(_list.Count);
			int pos;
			foreach (T item in _list)
			{
				pos = other._list.BinarySearch(item, _comparer);
				if (pos < 0)
					result.Add(item);
			}
			return new SetList<T>(result, _comparer);
		}

		/// <summary> Returns the items in this set that are not in the provided set </summary>
		/// <example>{ 1, 2, 3 }.ExclusiveOrWith({ 2, 3, 4 }) == { 1, 4 }</example>
		public SetList<T> ExclusiveOrWith(SetList<T> other)
		{
			List<T> result = new List<T>(other._list.Count);
			int pos;
			foreach (T item in other._list)
			{
				pos = _list.BinarySearch(item, _comparer);
				if (pos < 0)
					result.Add(item);
			}
			foreach (T item in _list)
			{
				pos = other._list.BinarySearch(item, _comparer);
				if (pos < 0)
				{
					pos = result.BinarySearch(item, _comparer);
					result.Insert(~pos, item);
				}
			}
			return new SetList<T>(result, _comparer);
		}

		#endregion

		#region SetList testing subset/superset: IsEqualTo, IsSubsetOf, IsSupersetOf

		/// <summary> Returns true if all items in this set are also in the provided set </summary>
		/// <example>{ 1, 2 }.IsEqualTo({ 1, 2 }) == true &amp;&amp; {}.IsEqualTo({}) == true</example>
		public bool IsEqualTo(SetList<T> other)
		{
			if (_list.Count != other._list.Count)
				return false;

			for (int i = 0; i < _list.Count; i++)
			{
				if (_comparer.Compare(_list[i], other._list[i]) != 0)
					return false;
			}
			return true;
		}

		/// <summary> Returns true if all items in this set are also in the provided set </summary>
		/// <example>{ 1, 2, 4 }.IsSubsetOf({ 1, 2, 3, 4 }) == true &amp;&amp; {}.IsSubsetOf({ 1 }) == true</example>
		public bool IsSubsetOf(SetList<T> other)
		{
			int pos;
			foreach (T item in _list)
			{
				pos = other._list.BinarySearch(item, _comparer);
				if (pos < 0)
					return false;
			}
			return true;
		}

		/// <summary> Returns true if all items in the provided set are also in this set </summary>
		/// <example>{ 1, 2, 3, 4 }.IsSupersetOf({ 1, 2, 4 }) == true &amp;&amp; { 1 }.IsSupersetOf({}) == true</example>
		public bool IsSupersetOf(SetList<T> other)
		{
			int pos;
			foreach (T item in other._list)
			{
				pos = _list.BinarySearch(item, _comparer);
				if (pos < 0)
					return false;
			}
			return true;
		}

		#endregion

		#region ICollection Members

		/// <summary> Copies collection to array </summary>
		void ICollection.CopyTo(Array array, int index)
		{
			ICollection c = _list;
			c.CopyTo(array, index);
		}

		/// <summary> Returns false </summary>
		bool ICollection.IsSynchronized { get { return false; } }

		/// <summary> Returns SyncRoot </summary>
		object ICollection.SyncRoot { get { return this; } }

		/// <summary> Returns an enumerator </summary>
		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IEnumerable)_list).GetEnumerator();
		}

		/// <summary> Returns a typed enumerator </summary>
		public IEnumerator<T> GetEnumerator()
		{
			return _list.GetEnumerator();
		}

		#endregion

        #region IList Members

        int IList.Add(object value)
        {
            if (value is T)
            {
                int pos;
                this.Add((T)value, out pos);
                return pos;
            }
            else
                throw new ArgumentException();
        }

        bool IList.Contains(object value)
        {
            if (value is T)
                return this.Contains((T)value);
            else
                return false;
        }

        int IList.IndexOf(object value)
        {
            if (value is T)
                return this.IndexOf((T)value);
            else
                return -1;
        }

        void IList.Insert(int index, object value)
        {
            throw new NotSupportedException();
        }

        bool IList.IsFixedSize
        {
            get { return false; }
        }

        void IList.Remove(object value)
        {
            if (value is T)
                this.Remove((T)value);
            else
                throw new ArgumentException();
        }

        object IList.this[int index]
        {
            get { return this[index]; }
            set { throw new NotSupportedException(); }
        }

        #endregion

        #region ICloneable<T> Members

        /// <summary> Returns a shallow clone of this object </summary>
        public SetList<T> Clone()
        {
            return new SetList<T>(new List<T>(_list), _comparer);
        }

        object ICloneable.Clone()
        {
            return this.Clone();
        }

        #endregion
    }
}