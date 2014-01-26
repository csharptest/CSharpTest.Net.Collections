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

namespace CSharpTest.Net.Utils
{

    /// <summary>
    /// Provides an interface for tracking a limited number of references to objects for use in a WeakReference
    /// cache.
    /// </summary>
    public interface IObjectKeepAlive
    {
        /// <summary>
        /// Clears the entire keep-alive cache
        /// </summary>
        void Clear();

        /// <summary>
        /// Can be called periodically by external threads to ensure cleanup instead of depending upon calls to Add()
        /// </summary>
        void Tick();

        /// <summary>
        /// Cleans up expired items and adds the object to the list of items to keep alive.
        /// </summary>
        void Add(object item);
    }

    /// <summary>
    /// Provides a means of forcing the garbage collector to wait on objects aquired from permanent 
    /// storage while only holding WeakReference's of the object.  Essentially uses a simple lockless 
    /// algorithm to track the most recently loaded objects so that they will stay alive longer.
    /// </summary>
    [System.Diagnostics.DebuggerDisplay("{_head.Start}-{_tail.Last}")]
    public class ObjectKeepAlive : IObjectKeepAlive
    {
        private const int BucketSize = 100;

        private readonly int _minItems;
        private readonly int _maxItems;
        private readonly bool _externalTicks;
        private readonly long _maxAge;

        private long _position;
        private Entry _head;
        private Entry _tail;

        /// <summary>
        /// Configures the keep-alive policy for this container
        /// </summary>
        /// <param name="minItems">The minimum number of items desired in the list (kept event after age expires)</param>
        /// <param name="maxItems">The maximum number of items desired in the list (discarded even if age has not expired)</param>
        /// <param name="maxAge">Determines how long to keep an object if the count is between min and max</param>
        public ObjectKeepAlive(int minItems, int maxItems, TimeSpan maxAge)
            : this(minItems, maxItems, maxAge, false)
        { }

        /// <summary>
        /// Configures the keep-alive policy for this container
        /// </summary>
        /// <param name="minItems">The minimum number of items desired in the list (kept event after age expires)</param>
        /// <param name="maxItems">The maximum number of items desired in the list (discarded even if age has not expired)</param>
        /// <param name="maxAge">Determines how long to keep an object if the count is between min and max</param>
        /// <param name="externalTicks">True if you want to perform cleanup exclusivly on another thread by calling Tick(), otherwise false</param>
        public ObjectKeepAlive(int minItems, int maxItems, TimeSpan maxAge, bool externalTicks)
        {
            _minItems = minItems;
            _maxItems = maxItems;
            _externalTicks = externalTicks;
            _maxAge = maxAge.Ticks;

            _position = -1;
            _head = _tail = new Entry(0);
        }

        /// <summary>
        /// Clears the entire keep-alive cache
        /// </summary>
        public void Clear()
        {
            _position = -1;
            _head = _tail = new Entry(0);
        }

        /// <summary>
        /// Can be called periodically by external threads to ensure cleanup instead of depending upon calls to Add()
        /// </summary>
        public void Tick()
        {
            Tick(DateTime.UtcNow.Ticks);
        }

        /// <summary>
        /// Cleans up expired items and adds the object to the list of items to keep alive.
        /// </summary>
        public void Add(object item)
        {
            if (_maxItems == 0)
                return;

            long dtNow = DateTime.UtcNow.Ticks;
            if(!_externalTicks)
                Tick(dtNow);
            
            Entry current;
            long myPos;

            do {
                current = _tail;
                myPos = Interlocked.Increment(ref _position);
            } while (myPos < current.Start);

            int myOffset = (int)(myPos - current.Start);
            while (myOffset >= BucketSize)
            {
                if (current.Next == null)
                {
                    Entry next = new Entry(current.Start + BucketSize);
                    Interlocked.CompareExchange(ref current.Next, next, null);
                }

                Interlocked.CompareExchange(ref _tail, current.Next, current);
                current = current.Next;
                myOffset = (int)(myPos - current.Start);
            }

            current.Age = dtNow;
            current.Items[myOffset] = item;

            long lastPos = current.Last;
            while (lastPos <= myPos)
            {
                long test = Interlocked.CompareExchange(ref current.Last, myPos + 1, lastPos);
                if (lastPos == test)
                    break;
                lastPos = test;
            }
        }

        private void Tick(long dtNow)
        {
            long expireBefore = dtNow - _maxAge;

        killAnother:
            Entry item = _head;
            bool killEntry = false;

            long itemsFollowing = _position - item.Last;//how many items are we holding after this Entry

            killEntry |= itemsFollowing > _maxItems;//exceeding our maxItems limit?
            killEntry |= itemsFollowing > _minItems && item.Age < expireBefore;//timeout of all items?
            killEntry &= _head.Next != null && !ReferenceEquals(_head, _tail); //only kill when something follows.

            if (killEntry)
            {
                if (ReferenceEquals(item, Interlocked.CompareExchange(ref _head, item.Next, item)))
                    if((itemsFollowing - BucketSize) > _maxItems)
                        goto killAnother;
                return;
            }

            int offset = item.OffsetClear;
            long entryCount = item.Last - item.Start - offset;
            while (entryCount > 0 && offset < BucketSize && (entryCount > _maxItems || (item.Age < expireBefore && entryCount > _minItems)))
            {
                if (offset != Interlocked.CompareExchange(ref item.OffsetClear, offset + 1, offset))
                    break;

                item.Items[offset] = null;

                offset++;
                entryCount--;
            }
        }

        [System.Diagnostics.DebuggerDisplay("{Start}-{Last}")]
        class Entry
        {
            public readonly long Start;
            public readonly object[] Items;
            public long Last;
            public long Age;
            public int OffsetClear;
            public Entry Next;

            public Entry(long start)
            {
                Start = Last = start;
                OffsetClear = 0;
                Age = long.MaxValue;
                Items = new object[BucketSize];
                Next = null;
            }
        }
    }
}
