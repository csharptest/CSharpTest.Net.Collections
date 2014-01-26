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
        class Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>, IEnumerable<KeyValuePair<TKey, TValue>>
        {
            readonly BPlusTree<TKey, TValue> _tree;
            readonly Element[] _currentSet;

            TKey _nextKey;
            bool _hasMore;

            int _currentLimit;
            int _currentOffset;
            bool _enumComplete;

            readonly bool _hasStart;
            readonly TKey _startKey;
            readonly Predicate<KeyValuePair<TKey, TValue>> _fnContinue;

            public Enumerator(BPlusTree<TKey, TValue> tree)
            {
                tree.NotDisposed();
                _tree = tree;
                _currentSet = new Element[_tree._options.MaximumValueNodes];
                Reset();
            }

            public Enumerator(BPlusTree<TKey, TValue> tree, TKey startKey)
            {
                tree.NotDisposed();
                _tree = tree;
                _currentSet = new Element[_tree._options.MaximumValueNodes];
                _hasStart = true;
                _startKey = startKey;
                Reset();
            }

            public Enumerator(BPlusTree<TKey, TValue> tree, TKey startKey, Predicate<KeyValuePair<TKey, TValue>> fnContinue)
                : this(tree, startKey)
            {
                _fnContinue = fnContinue;
            }

            public void Reset()
            {
                if (_hasStart)
                {
                    _enumComplete = false;
                    _currentLimit = 0;
                    _currentOffset = 0;
                    _hasMore = true;
                    _nextKey = _startKey;
                }
                else
                {
                    using (RootLock root = _tree.LockRoot(LockType.Read, "Enumerator"))
                    using (NodePin first = SeekFirst(root.Pin, out _nextKey, out _hasMore))
                    {
                        FillFromNode(first.Ptr);
                        _enumComplete = _currentLimit == 0;
                    }
                }
            }

            private void FillFromNode(Node node)
            {
                if (!node.IsLeaf)
                    throw new InvalidOperationException();

                _currentOffset = -1;
                node.CopyTo(_currentSet, out _currentLimit);
            }

            public bool MoveNext()
            {
                if (_enumComplete)
                    return false;
                if (++_currentOffset < _currentLimit)
                {
                    if(_fnContinue == null || _fnContinue(Current))
                        return true;

                    Array.Clear(_currentSet, 0, _currentSet.Length);
                    _currentOffset = 0;
                    _currentLimit = 0;
                    _enumComplete = true;
                    return false;
                }

                bool success = false;
                using (RootLock root = _tree.LockRoot(LockType.Read, "Enumerator"))
                {
                    int offset;
                    NodePin next;

                    if (_hasMore && SeekNext(root.Pin, _nextKey, out next, out offset, out _nextKey, out _hasMore))
                    {
                        using (next)
                        {
                            FillFromNode(next.Ptr);
                            _currentOffset = offset;
                        }
                        success = true;
                    }
                }

                if (success)
                {
                    if (_currentOffset >= _currentLimit)
                        return MoveNext();

                    if(_fnContinue == null || _fnContinue(Current))
                        return true;
                }

                Array.Clear(_currentSet, 0, _currentSet.Length);
                _currentOffset = 0;
                _currentLimit = 0;
                _enumComplete = true;
                return false;
            }

            public KeyValuePair<TKey, TValue> Current
            {
                get { return _currentSet[_currentOffset].ToKeyValuePair(); }
            }

            object System.Collections.IEnumerator.Current { get { return Current; } }

            public void Dispose()
            { }

            private NodePin SeekFirst(NodePin thisLock, out TKey nextKey, out bool hasMore)
            {
                nextKey = default(TKey);
                hasMore = false;

                NodePin myPin = thisLock, nextPin = null;
                try
                {
                    while (myPin != null)
                    {
                        Node me = myPin.Ptr;
                        if (me.IsLeaf)
                        {
                            NodePin pin = myPin;
                            myPin = null;
                            return pin;
                        }

                        if (me.Count > 1)
                        {
                            nextKey = me[1].Key;
                            hasMore = true;
                        }
                        nextPin = _tree._storage.Lock(myPin, me[0].ChildNode);
                        myPin.Dispose();
                        myPin = nextPin;
                        nextPin = null;
                    }
                }
                finally
                {
                    if (myPin != null) myPin.Dispose();
                    if (nextPin != null) nextPin.Dispose();
                }
                throw new InvalidOperationException();
            }

            private bool SeekNext(NodePin thisLock, TKey key, out NodePin pin, out int offset, out TKey nextKey, out bool hasMore)
            {
                pin = null;
                offset = -1;
                nextKey = default(TKey);
                hasMore = false;

                Element find = new Element(key);
                NodePin next = null;
                NodePin current = thisLock;

                try
                {
                    int ordinal;

                    while (true)
                    {
                        Node me = current.Ptr;
                        me.BinarySearch(_tree._itemComparer, find, out ordinal);
                        if (me.IsLeaf)
                        {
                            pin = current;
                            offset = ordinal;
                            current = null;
                            return true;
                        }

                        if (me.Count > ordinal + 1)
                        {
                            nextKey = current.Ptr[ordinal + 1].Key;
                            hasMore = true;
                        }

                        next = _tree._storage.Lock(current, me[ordinal].ChildNode);
                        current.Dispose();
                        current = next;
                        next = null;
                    }

                }
                finally
                {
                    if (current != null) current.Dispose();
                    if (next != null) next.Dispose();
                }
            }

            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                if (_enumComplete)
                    Reset();
                return this;
            }

            [Obsolete]
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            { return GetEnumerator(); }
        }
    }
}