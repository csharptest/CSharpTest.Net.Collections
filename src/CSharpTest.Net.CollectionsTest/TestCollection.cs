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
using CSharpTest.Net.Interfaces;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    public abstract class TestCollection<TList, TFactory, TItem>
        where TList : ICollection<TItem>, IDisposable
        where TFactory : IFactory<TList>, new()
    {
        protected readonly TFactory Factory = new TFactory();
        protected abstract TItem[] GetSample();

        [Fact]
        public void TestAddRemove()
        {
            using (TList list = Factory.Create())
            {
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
        }

        [Fact]
        public void TestAddReverseRemove()
        {
            using (TList list = Factory.Create())
            {
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
        }

        [Fact]
        public void TestClear()
        {
            using (TList list = Factory.Create())
            {
                TItem[] items = GetSample();

                foreach (TItem item in items)
                    list.Add(item);
                Assert.Equal(items.Length, list.Count);

                Assert.NotEqual(0, list.Count);
                list.Clear();
                Assert.Equal(0, list.Count);
            }
        }

        [Fact]
        public void TestContains()
        {
            using (TList list = Factory.Create())
            {
                TItem[] items = GetSample();

                foreach (TItem item in items)
                    list.Add(item);
                Assert.Equal(items.Length, list.Count);

                foreach (TItem item in items)
                    Assert.True(list.Contains(item));
            }
        }

        [Fact]
        public void TestCopyTo()
        {
            using (TList list = Factory.Create())
            {
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
        }

        [Fact]
        public void TestIsReadOnly()
        {
            using (TList list = Factory.Create())
            {
                Assert.False(list.IsReadOnly);
            }
        }

        [Fact]
        public void TestGetEnumerator()
        {
            using (TList list = Factory.Create())
            {
                List<TItem> items = new List<TItem>(GetSample());

                foreach (TItem item in items)
                    list.Add(item);
                Assert.Equal(items.Count, list.Count);

                foreach (TItem item in list)
                    Assert.True(items.Remove(item));

                Assert.Equal(0, items.Count);
            }
        }

        [Fact]
        public void TestGetEnumerator2()
        {
            using (TList list = Factory.Create())
            {
                List<TItem> items = new List<TItem>(GetSample());

                foreach (TItem item in items)
                    list.Add(item);
                Assert.Equal(items.Count, list.Count);

                foreach (TItem item in (IEnumerable) list)
                    Assert.True(items.Remove(item));

                Assert.Equal(0, items.Count);
            }
        }
    }
}