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
        enum RemoveResult { Ignored = 0, Removed = 1, NotFound = 2 }
        struct RemoveAlways : IRemoveValue<TKey, TValue>
        {
            private bool _removed;
            private TValue _value;
            public bool TryGetValue(out TValue value)
            {
                value = _value;
                return _removed;
            }
            bool IRemoveValue<TKey, TValue>.RemoveValue(TKey key, TValue value)
            {
                _value = value;
                return _removed = true;
            }
        }
        struct RemoveIfValue : IRemoveValue<TKey, TValue>
        {
            private readonly TValue _value;
            public RemoveIfValue(TKey key, TValue value)
            {
                _value = value;
            }
            bool IRemoveValue<TKey, TValue>.RemoveValue(TKey key, TValue value)
            {
                return EqualityComparer<TValue>.Default.Equals(value, _value);
            }
        }
        struct RemoveIfPredicate : IRemoveValue<TKey, TValue>
        {
            private readonly KeyValuePredicate<TKey, TValue> _test;
            public RemoveIfPredicate(KeyValuePredicate<TKey, TValue> test)
            {
                _test = test;
            }
            bool IRemoveValue<TKey, TValue>.RemoveValue(TKey key, TValue value)
            {
                return _test(key, value);
            }
        }

        private RemoveResult Delete<T>(NodePin thisLock, TKey key, ref T condition, NodePin parent, int parentIx)
            where T : IRemoveValue<TKey, TValue>
        {
            Node me = thisLock.Ptr;
            if (me.Count == 0) return RemoveResult.NotFound;
            int minimumKeys = me.IsLeaf ? _options.MinimumValueNodes : _options.MinimumChildNodes;
            if (me.Count <= minimumKeys && parent != null && !parent.Ptr.IsRoot)
            {
                if (parentIx < parent.Ptr.Count - 1)
                {
                    using (NodePin bigger = _storage.Lock(parent, parent.Ptr[parentIx + 1].ChildNode))
                        Join(thisLock, bigger, false, parent, parentIx);
                    thisLock.Dispose();
                    return Delete(parent, key, ref condition, null, int.MinValue);
                }
                if (parentIx > 0)
                {
                    using (NodePin smaller = _storage.Lock(parent, parent.Ptr[parentIx - 1].ChildNode))
                        Join(smaller, thisLock, true, parent, parentIx - 1);
                    thisLock.Dispose();
                    return Delete(parent, key, ref condition, null, int.MinValue);
                }
                Assert(false, "Failed to join node before delete.");
            }
            else if (parent != null && parent.Ptr.IsRoot && me.Count == 1 && !me.IsLeaf)
            {
                using (NodePin onlyChild = _storage.Lock(thisLock, me[0].ChildNode))
                {
                    using (NodeTransaction t = _storage.BeginTransaction())
                    {
                        RootNode rootNode = (RootNode)t.BeginUpdate(parent);
                        rootNode.ReplaceChild(0, thisLock.Handle, onlyChild.Handle);
                        t.Destroy(thisLock);
                        t.Commit();
                    }
                    return Delete(onlyChild, key, ref condition, null, int.MinValue);
                }
            }

            if (parent != null)
                parent.Dispose();//done with the parent lock.

            bool isValueNode = me.IsLeaf;
            int ordinal;
            if (me.BinarySearch(_itemComparer, new Element(key), out ordinal) && isValueNode)
            {
                if (condition.RemoveValue(key, me[ordinal].Payload))
                {
                    using (NodeTransaction t = _storage.BeginTransaction())
                    {
                        me = t.BeginUpdate(thisLock);
                        me.Remove(ordinal, new Element(key), _keyComparer);
                        t.RemoveValue(key);
                        t.Commit();
                        return RemoveResult.Removed;
                    }
                }
                return RemoveResult.Ignored;
            }

            if (isValueNode)
                return RemoveResult.NotFound;

            if (ordinal >= me.Count) ordinal = me.Count - 1;
            using (NodePin child = _storage.Lock(thisLock, me[ordinal].ChildNode))
                return Delete(child, key, ref condition, thisLock, ordinal);
        }

        private void CopyElements(Node src, int srcIndex, Node dest, int destIndex, int count, TKey firstSrcKey)
        {
            for (int ix = 0; ix < count; ix++)
            {
                Element item = src[srcIndex + ix];
                if ((destIndex + ix) == 0 && !src.IsLeaf)
                    item = new Element(default(TKey), item.ChildNode); //uses empty key at index zero on non-leaf
                else if ((srcIndex + ix) == 0 && !src.IsLeaf)
                    item = new Element(firstSrcKey, item.ChildNode); //replace empty key at index zero on non-leaf

                dest.Insert(destIndex + ix, item);
            }
        }

        private void Join(NodePin small, NodePin big, bool moveToBigger, NodePin parent, int parentSmallIndex)
        {
            int fillFactor = small.Ptr.IsLeaf ? _options.FillValueNodes : _options.FillChildNodes;
            int minimumKeys = small.Ptr.IsLeaf ? _options.MinimumValueNodes : _options.MinimumChildNodes;

            using (NodeTransaction trans = _storage.BeginTransaction())
            {
                int parentBigIndex = parentSmallIndex + 1;
                Assert(small.Handle.Equals(parent.Ptr[parentSmallIndex].ChildNode), "Incorrect parent ordinal for left.");
                Assert(big.Handle.Equals(parent.Ptr[parentBigIndex].ChildNode), "Incorrect parent ordinal for right.");

                TKey bigZeroKey = parent.Ptr[parentBigIndex].Key;

                trans.BeginUpdate(parent);
                //join the when they combined have less than the fillfactor, or when the both have minimum key counts...
                if (small.Ptr.Count + big.Ptr.Count <= fillFactor ||
                    (small.Ptr.Count + big.Ptr.Count) <= (minimumKeys * 2))
                {
                    using (NodePin joinNode = trans.Create(parent, small.Ptr.IsLeaf))
                    {
                        Element removeItem = parent.Ptr[parentBigIndex];
                        Assert(removeItem.IsNode && removeItem.ChildNode.Equals(big.Handle),
                               "Invalid parent index in join.");
                        parent.Ptr.Remove(parentBigIndex, removeItem, _keyComparer);

                        CopyElements(small.Ptr, 0, joinNode.Ptr, 0, small.Ptr.Count, small.Ptr[0].Key);
                        CopyElements(big.Ptr, 0, joinNode.Ptr, joinNode.Ptr.Count, big.Ptr.Count, bigZeroKey);

                        parent.Ptr.ReplaceChild(parentSmallIndex, small.Handle, joinNode.Handle);

                        trans.Destroy(big);
                        trans.Destroy(small);
                        trans.Commit();
                    }
                }
                else //ballances the distribution of nodes between two children, using moveToBigger to resolve rounding up/down
                {
                    using (NodePin newSmall = trans.Create(parent, small.Ptr.IsLeaf))
                    using (NodePin newBig = trans.Create(parent, big.Ptr.IsLeaf))
                    {
                        int total = small.Ptr.Count + big.Ptr.Count;
                        int borrowing = ((moveToBigger ? 0 : 1) + total) / 2;

                        TKey breakKey = borrowing < small.Ptr.Count ?
                            small.Ptr[borrowing].Key : big.Ptr[borrowing - small.Ptr.Count].Key;

                        CopyElements(small.Ptr, 0, newSmall.Ptr, 0, Math.Min(borrowing, small.Ptr.Count), small.Ptr[0].Key);
                        CopyElements(big.Ptr, 0, newSmall.Ptr, newSmall.Ptr.Count, borrowing - small.Ptr.Count, bigZeroKey);

                        CopyElements(small.Ptr, borrowing, newBig.Ptr, newBig.Ptr.Count, small.Ptr.Count - borrowing, default(TKey));
                        CopyElements(big.Ptr, Math.Max(0, borrowing - small.Ptr.Count), newBig.Ptr, newBig.Ptr.Count, Math.Min(big.Ptr.Count, total - borrowing), bigZeroKey);

                        parent.Ptr.ReplaceChild(parentSmallIndex, small.Handle, newSmall.Handle);

                        parent.Ptr.ReplaceKey(parentBigIndex, breakKey, _keyComparer);
                        parent.Ptr.ReplaceChild(parentBigIndex, big.Handle, newBig.Handle);

                        trans.Destroy(big);
                        trans.Destroy(small);
                        trans.Commit();
                    }
                }
            }
        }
    }
}
