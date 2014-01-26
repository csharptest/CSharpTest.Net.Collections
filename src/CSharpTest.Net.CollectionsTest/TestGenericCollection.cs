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
using System.Collections.Generic;
using NUnit.Framework;
using System;

namespace CSharpTest.Net.Library.Test
{
    public abstract class TestGenericCollection<TList, TItem> 
        where TList : ICollection<TItem>, new()
    {
        protected abstract TItem[] GetSample();

        protected TList CreateSample(TItem[] items)
        {
            TList list = new TList();

            int count = 0;
            Assert.AreEqual(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.AreEqual(++count, list.Count);
            }
            return list;
        }

        [Test]
        public void TestAddRemove()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            int count = 0;
            Assert.AreEqual(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.AreEqual(++count, list.Count);
            }
            foreach (TItem item in items)
            {
                Assert.IsTrue(list.Remove(item));
                Assert.AreEqual(--count, list.Count);
            }
        }

        [Test]
        public void TestAddReverseRemove()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            int count = 0;
            Assert.AreEqual(count, list.Count);

            foreach (TItem item in items)
            {
                list.Add(item);
                Assert.AreEqual(++count, list.Count);
            }
            for (int ix = items.Length - 1; ix >= 0; ix--)
            {
                Assert.IsTrue(list.Remove(items[ix]));
                Assert.AreEqual(--count, list.Count);
            }
        }

        [Test]
        public void TestClear()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            foreach (TItem item in items)
                list.Add(item);
            Assert.AreEqual(items.Length, list.Count);

            Assert.AreNotEqual(0, list.Count);
            list.Clear();
            Assert.AreEqual(0, list.Count);
        }

        [Test]
        public void TestContains()
        {
            TList list = new TList();
            TItem[] items = GetSample();

            foreach (TItem item in items)
                list.Add(item);
            Assert.AreEqual(items.Length, list.Count);

            foreach (TItem item in items)
                Assert.IsTrue(list.Contains(item));
        }

        [Test]
        public void TestCopyTo()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.AreEqual(items.Count, list.Count);

            TItem[] copy = new TItem[items.Count + 1];
            list.CopyTo(copy, 1);
            Assert.AreEqual(default(TItem), copy[0]);

            for (int i = 1; i < copy.Length; i++)
                Assert.IsTrue(items.Remove(copy[i]));

            Assert.AreEqual(0, items.Count);
        }

        [Test]
        public void TestIsReadOnly()
        {
            Assert.IsFalse(new TList().IsReadOnly);
        }

        [Test]
        public void TestGetEnumerator()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.AreEqual(items.Count, list.Count);

            foreach (TItem item in list)
                Assert.IsTrue(items.Remove(item));

            Assert.AreEqual(0, items.Count);
        }

        [Test]
        public void TestGetEnumerator2()
        {
            TList list = new TList();
            List<TItem> items = new List<TItem>(GetSample());

            foreach (TItem item in items)
                list.Add(item);
            Assert.AreEqual(items.Count, list.Count);

            foreach (TItem item in ((System.Collections.IEnumerable)list))
                Assert.IsTrue(items.Remove(item));

            Assert.AreEqual(0, items.Count);
        }

        public static void VerifyCollection<T, TC>(IEqualityComparer<T> comparer, ICollection<T> expected, TC collection) where TC : ICollection<T>
        {
            Assert.AreEqual(expected.IsReadOnly, collection.IsReadOnly);
            Assert.AreEqual(expected.Count, collection.Count);
            CompareEnumerations(comparer, expected, collection);
            using (var a = expected.GetEnumerator())
            using (var b = collection.GetEnumerator())
            {
                bool result;
                Assert.IsTrue(b.MoveNext());
                b.Reset();
                Assert.AreEqual(result = a.MoveNext(), b.MoveNext());
                while (result)
                {
                    Assert.IsTrue(comparer.Equals(a.Current, b.Current));
                    Assert.IsTrue(comparer.Equals(a.Current, (T)((System.Collections.IEnumerator)b).Current));
                    Assert.AreEqual(result = a.MoveNext(), b.MoveNext());
                }
            }

            T[] items = new T[10 + collection.Count];
            collection.CopyTo(items, 5);
            Array.Copy(items, 5, items, 0, collection.Count);
            Array.Resize(ref items, collection.Count);
            CompareEnumerations(comparer, expected, collection);

            for( int i=0; i < 5; i++)
                Assert.IsTrue(collection.Contains(items[i]));
        }

        public static void CompareEnumerations<T>(IEqualityComparer<T> comparer, IEnumerable<T> expected, IEnumerable<T> collection)
        {
            using (var a = expected.GetEnumerator())
            using (var b = collection.GetEnumerator())
            {
                bool result;
                Assert.AreEqual(result = a.MoveNext(), b.MoveNext());
                while (result)
                {
                    Assert.IsTrue(comparer.Equals(a.Current, b.Current));
                    Assert.AreEqual(result = a.MoveNext(), b.MoveNext());
                }
            }
        }
    }
}