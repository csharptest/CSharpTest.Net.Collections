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
using System.IO;

namespace CSharpTest.Net.Collections
{
	/// <summary>
	/// An ordinal list is a list optimized to store lists of integer data that can then be manipulated 
	/// as a set with intersect/union etc.  Each integer stored is translated to a bit offset and thus
	/// cann't be stored more than once or in any particular order.  Note: adding the value int.Max will 
	/// allocate int.Max/8 bytes of memory, so this is best used with ordinal indexes into a list that 
	/// is smaller than 8,388,608 (one megabyte of bits).  Pre-allocate with Ceiling = max for better
	/// performance, or add the integers in reverse order (highest to lowest).
	/// </summary>
	public class OrdinalList : ICollection<int>, ICollection, IEnumerable, ICloneable
	{
		#region Static cache for Count { get; }
		static readonly byte[] BitCount;

		static OrdinalList()
		{
			BitCount = new byte[byte.MaxValue + 1];
			for (int i = 0; i <= byte.MaxValue; i++)
			{
				if ((i & 0x0001) != 0) BitCount[i]++;
				if ((i & 0x0002) != 0) BitCount[i]++;
				if ((i & 0x0004) != 0) BitCount[i]++;
				if ((i & 0x0008) != 0) BitCount[i]++;
				if ((i & 0x0010) != 0) BitCount[i]++;
				if ((i & 0x0020) != 0) BitCount[i]++;
				if ((i & 0x0040) != 0) BitCount[i]++;
				if ((i & 0x0080) != 0) BitCount[i]++;
			}
		}
		#endregion

		byte[] _bits;

		/// <summary> Constructs an empty OrdinalList </summary>
		public OrdinalList()
		{
			Clear();
		}

		/// <summary> Constructs an OrdinalList from a set of bits represeting the ordinals </summary>
		public OrdinalList(byte[] fromBits)
		{
			Clear();
			_bits = (byte[])fromBits.Clone();
		}

		/// <summary> Constructs an OrdinalList from the integer ordinals provided </summary>
		public OrdinalList(IEnumerable<int> contents)
		{
			Clear();
			AddRange(contents);
		}

		/// <summary> Empty the OrdinalList </summary>
		public void Clear()
		{
			_bits = new byte[0];
		}

		/// <summary> Semi-expensive, returns the count of ordinals in the collection </summary>
		public int Count
		{
			get
			{
				int count = 0;
				foreach (byte b in _bits)
					count += BitCount[b];
				return count;
			}
		}

		/// <summary> 
		/// Gets or sets the maximum inclusive ordinal that can be stored in the memory currently
		/// allocated, ranges from -1 to int.MaxValue
		/// </summary>
		public int Ceiling
		{
			get { return (int)((((long)_bits.Length) << 3) - 1); }
			set { AllocFor(value); }
		}

		private void AllocFor(int max)
		{
            if (max >= 0)
                max = 1 + (max >> 3);
            else if (max == -1)
                max = 0;
            else
                throw new ArgumentOutOfRangeException();

			if (max > _bits.Length)
				Array.Resize(ref _bits, max);
		}

		/// <summary> Adds a range of integer ordinals into the collection </summary>
		public void AddRange(IEnumerable<int> contents)
		{
			int max = int.MinValue;

			foreach (int i in contents)
				max = Math.Max(i, max);

			if (max == int.MinValue)
				return;//empty

			//pre-alloc/adjust array
			AllocFor(max);

			foreach (int item in contents)
			{
				int offset = item >> 3;
				int bit = 1 << (item & 0x07);
				_bits[offset] |= unchecked((byte)bit);
			}
		}

		/// <summary> Adds an integer ordinal into the collection </summary>
		public void Add(int item)
		{
			AllocFor(item);
			int offset = item >> 3;
			int bit = 1 << (item & 0x07);
			_bits[offset] |= unchecked((byte)bit);
		}

		/// <summary> Removes an ordinal from the collection </summary>
		public bool Remove(int item)
		{
			int offset = item >> 3;
			int bit = 1 << (item & 0x07);
            if (offset < _bits.Length)
            {
                if (0 != (_bits[offset] & unchecked((byte)bit)))
                {
                    _bits[offset] &= unchecked((byte)~bit);
                    return true;
                }
            }
			return false;
		}

		/// <summary> Returns true if the ordinal is in the collection </summary>
		public bool Contains(int item)
		{
			int offset = item >> 3;
			int bit = 1 << (item & 0x07);
			if (offset < _bits.Length && (_bits[offset] & bit) == bit)
				return true;
			return false;
		}

		/// <summary> Extracts the ordinals into an array </summary>
		public void CopyTo(int[] array, int arrayIndex)
		{
			foreach(int ordinal in this)
				array[arrayIndex++] = ordinal;
		}

		/// <summary> Returns false </summary>
		public bool IsReadOnly
		{
			get { return false; }
		}

		/// <summary> Returns the array of ordinals that have been added. </summary>
		public int[] ToArray() { return new List<int>(this).ToArray(); }

		/// <summary> Returns the complete set of raw bytes for storage and reconstitution </summary>
		public byte[] ToByteArray() { return (byte[])_bits.Clone(); }

		#region Set Operations
        /// <summary> Returns the 1's compliment (inverts) of the list up to Ceiling </summary>
        public OrdinalList Invert(int ceiling)
        {
            unchecked
            {
                byte[] copy = new byte[_bits.Length];
                for (int i = 0; i < _bits.Length; i++)
                    copy[i] = (byte)~_bits[i];

                OrdinalList result = new OrdinalList();
                result._bits = copy;

                result.Ceiling = ceiling;
                int limit = result.Ceiling;
                for (int i = Ceiling; i < limit; i++)
                    result.Add(i);
                for (int i = ceiling + 1; i <= limit; i++)
                    result.Remove(i);

                return result;
            }
        }

        /// <summary> Returns the set of items that are in both this set and the provided set </summary>
        /// <example>{ 1, 2, 3 }.IntersectWith({ 2, 3, 4 }) == { 2, 3 }</example>
        public OrdinalList IntersectWith(OrdinalList other)
		{
			byte[] small, big;
			big = _bits.Length > other._bits.Length ? _bits : other._bits;
            small = _bits.Length > other._bits.Length ? other._bits : _bits;

			byte[] newbits = (byte[])small.Clone();
			for (int i = 0; i < small.Length; i++)
				newbits[i] &= big[i];

			OrdinalList result = new OrdinalList();
			result._bits = newbits;
			return result;
		}

		/// <summary> Returns the set of items that are in either this set or the provided set </summary>
		/// <example>{ 1, 2, 3 }.UnionWith({ 2, 3, 4 }) == { 1, 2, 3, 4 }</example>
		public OrdinalList UnionWith(OrdinalList other)
		{
			byte[] small, big;
			big = _bits.Length > other._bits.Length ? _bits : other._bits;
            small = _bits.Length > other._bits.Length ? other._bits : _bits;

			byte[] newbits = (byte[])big.Clone();
			for (int i = 0; i < small.Length; i++)
				newbits[i] |= small[i];

			OrdinalList result = new OrdinalList();
			result._bits = newbits;
			return result;
		}

		#endregion

		#region ICollection Members

		void ICollection.CopyTo(Array array, int index)
		{
			if (array is int[])
				this.CopyTo((int[])array, index);
			else if (array is byte[])
				_bits.CopyTo(array, index);
			else
				throw new ArgumentException();
		}

		bool ICollection.IsSynchronized
		{
			get { return false; }
		}

		object ICollection.SyncRoot
		{
            get { return this; }
        }

        /// <summary> Returns an enumeration of the ordinal values </summary>
        public IEnumerable<int> EnumerateFrom(int startAt)
        {
            return EnumerateRange(startAt, int.MaxValue);
        }

        /// <summary> Returns an enumeration of the ordinal values </summary>
        public IEnumerable<int> EnumerateRange(int startAt, int endAt)
        {
            int ordinal = startAt & ~0x07;
            //foreach (byte i in _bits)
            for(int ix = startAt >> 3; ix < _bits.Length; ix++)
            {
                if (_bits[ix] != 0)
                {
                    int i = _bits[ix];
                    if ((i & 0x0001) != 0) yield return ordinal + 0;
                    if ((i & 0x0002) != 0) yield return ordinal + 1;
                    if ((i & 0x0004) != 0) yield return ordinal + 2;
                    if ((i & 0x0008) != 0) yield return ordinal + 3;
                    if ((i & 0x0010) != 0) yield return ordinal + 4;
                    if ((i & 0x0020) != 0) yield return ordinal + 5;
                    if ((i & 0x0040) != 0) yield return ordinal + 6;
                    if ((i & 0x0080) != 0) yield return ordinal + 7;
                }
                ordinal += 8;
                if (ordinal > endAt)
                    break;
            }
        }

        /// <summary> Returns an enumeration of the ordinal values </summary>
        public IEnumerator<int> GetEnumerator()
        {
            return EnumerateFrom(0).GetEnumerator();
        }

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return EnumerateFrom(0).GetEnumerator();
		}

		#endregion

	    object ICloneable.Clone() { return Clone(); }

        /// <summary>
        /// Creates a new object that is a copy of the current instance.
        /// </summary>
	    public OrdinalList Clone()
	    {
            OrdinalList copy = new OrdinalList();
            copy._bits = (byte[])_bits.Clone();
            return copy;
	    }
	}
}
