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
using System.Collections;
using System.Collections.Generic;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    public abstract class TestGenericCollection<TList, TItem>
        where TList : ICollection<TItem>, new()
    {
        protected abstract TItem[] GetSample();

        protected TList CreateSample(TItem[] items)
        {
            TList list = new TList();

            int count = 0;
            Assert.Equal(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.Equal(++count, list.Count);
            }
            return list;
        }

        [Fact]
        public void TestAddRemove()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            int count = 0;
            Assert.Equal(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.Equal(++count, list.Count);
            }
            foreach (TItem item in items)
            {
                Assert.True(list.Remove(item));
                Assert.Equal(--count, list.Count);
            }
        }

        [Fact]
        public void TestAddReverseRemove()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            int count = 0;
            Assert.Equal(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.Equal(++count, list.Count);
            }
            for (int ix = items.Length - 1; ix >= 0; ix--)
            {
                Assert.True(list.Remove(items[ix]));
                Assert.Equal(--count, list.Count);
            }
        }

        [Fact]
        public void TestClear()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            foreach (TItem item in items)
                list.Add(item);
            Assert.Equal(items.Length, list.Count);

            Assert.NotEqual(0, list.Count);
            list.Clear();
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void TestContains()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            foreach (TItem item in items)
                list.Add(item);
            Assert.Equal(items.Length, list.Count);

            foreach (TItem item in items)
                Assert.True(list.Contains(item));
        }

        [Fact]
        public void TestCopyTo()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.Equal(items.Count, list.Count);

            TItem[] copy = new TItem[items.Count + 1];
            list.CopyTo(copy, 1);
            Assert.Equal(default(TItem), copy[0]);

            for (int i = 1; i < copy.Length; i++)
                Assert.True(items.Remove(copy[i]));

            Assert.Equal(0, items.Count);
        }

        [Fact]
        public void TestIsReadOnly()
        {
            Assert.False(new TList().IsReadOnly);
        }

        [Fact]
        public void TestGetEnumerator()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.Equal(items.Count, list.Count);

            foreach (TItem item in list)
                Assert.True(items.Remove(item));

            Assert.Equal(0, items.Count);
        }

        [Fact]
        public void TestGetEnumerator2()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.Equal(items.Count, list.Count);

            foreach (TItem item in (IEnumerable) list)
                Assert.True(items.Remove(item));

            Assert.Equal(0, items.Count);
        }

        public static void VerifyCollection<T, TC>(IEqualityComparer<T> comparer, ICollection<T> expected,
            TC collection) where TC : ICollection<T>
        {
            Assert.Equal(expected.IsReadOnly, collection.IsReadOnly);
            Assert.Equal(expected.Count, collection.Count);
            CompareEnumerations(comparer, expected, collection);
            using (IEnumerator<T> a = expected.GetEnumerator())
            using (IEnumerator<T> b = collection.GetEnumerator())
            {
                bool result;
                Assert.True(b.MoveNext());
                b.Reset();
                Assert.Equal(result = a.MoveNext(), b.MoveNext());
                while (result)
                {
                    Assert.True(comparer.Equals(a.Current, b.Current));
                    Assert.True(comparer.Equals(a.Current, (T) ((IEnumerator) b).Current));
                    Assert.Equal(result = a.MoveNext(), b.MoveNext());
                }
            }

            T[] items = new T[10 + collection.Count];
            collection.CopyTo(items, 5);
            Array.Copy(items, 5, items, 0, collection.Count);
            Array.Resize(ref items, collection.Count);
            CompareEnumerations(comparer, expected, collection);

            for (int i = 0; i < 5; i++)
                Assert.True(collection.Contains(items[i]));
        }

        public static void CompareEnumerations<T>(IEqualityComparer<T> comparer, IEnumerable<T> expected,
            IEnumerable<T> collection)
        {
            using (IEnumerator<T> a = expected.GetEnumerator())
            using (IEnumerator<T> b = collection.GetEnumerator())
            {
                bool result;
                Assert.Equal(result = a.MoveNext(), b.MoveNext());
                while (result)
                {
                    Assert.True(comparer.Equals(a.Current, b.Current));
                    Assert.Equal(result = a.MoveNext(), b.MoveNext());
                }
            }
        }
    }
}