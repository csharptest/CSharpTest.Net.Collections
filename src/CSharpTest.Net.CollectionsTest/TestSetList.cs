﻿#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using CSharpTest.Net.Collections.Test.Bases;
using CSharpTest.Net.Interfaces;
using Xunit;

#pragma warning disable 1591

namespace CSharpTest.Net.Collections.Test
{
    public class TestSetList
    {
        private class MyValue : Comparable<MyValue>
        {
            public readonly int Value;

            public MyValue(int value)
            {
                Value = value;
            }

            protected override int HashCode => Value.GetHashCode();

            public override int CompareTo(MyValue other)
            {
                return Value.CompareTo(other.Value);
            }
        }

        [Fact]
        [Trait("Category", "Benchmark")]
        public void BenchmarkTest()
        {
            TestBasics();
            TestICollection();
            TestIList();
            TestIntersectUnion();
            TestSubsets();

            long temp = 0;
            int repeat = 10;
            for (int i = 0; i < repeat; i++)
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();

                for (int i2 = 0; i2 < 10000; i2++)
                {
                    TestBasics();
                    TestICollection();
                    TestIList();
                    TestIntersectUnion();
                    TestSubsets();
                }

                sw.Stop();
                Console.Error.WriteLine("Benchmark: {0}", sw.ElapsedMilliseconds);
                temp += sw.ElapsedMilliseconds;
            }

            Console.Error.WriteLine("Average over {0}: {1}", repeat, temp / repeat);
        }


        [Fact]
        public void TestBasics()
        {
            SetList<int> list = new SetList<int>();
            Assert.False(list.IsReadOnly);

            for (int i = 512; i >= 0; i--)
                list.Add(i);

            int offset = 0;
            foreach (int item in list)
                Assert.Equal(offset++, item);

            Assert.Equal(513, offset);
            Assert.Equal(513, list.Count);

            list.Clear();
            list.AddRange(new[] { 5, 10, 20 });
            list.AddRange(new int[] { });

            Assert.Equal(3, list.Count);

            Assert.True(list.Contains(20));
            Assert.True(list.Remove(20));

            Assert.False(list.Contains(20));
            Assert.False(list.Remove(20));

            Assert.Equal(2, list.Count);

            int pos;
            list.Add(10, out pos);
            Assert.Equal(1, pos);
            Assert.Equal(2, list.Count);

            int[] items = new int[2];
            list.CopyTo(items, 0);
            Assert.Equal(5, items[0]);
            Assert.Equal(10, items[1]);

            items = list.ToArray();
            Assert.Equal(5, items[0]);
            Assert.Equal(10, items[1]);

            List<int> tmp = new List<int>();
            foreach (int i in list)
                tmp.Add(i);
            Assert.Equal(2, tmp.Count);
            Assert.Equal(5, tmp[0]);
            Assert.Equal(10, tmp[1]);
        }

        [Fact]
        public void TestCTors()
        {
            SetList<string> list = new SetList<string>((IEnumerable<string>)new[] { "a", "B" });
            Assert.Equal("a,B", string.Join(",", list.ToArray()));

            list = new SetList<string>(2);
            Assert.Equal("", string.Join(",", list.ToArray()));
            list.Add("a");
            list.Add("B");
            Assert.Equal("a,B", string.Join(",", list.ToArray()));

            list = new SetList<string>(2, StringComparer.Ordinal);
            Assert.Equal("", string.Join(",", list.ToArray()));
            list.Add("a");
            list.Add("B");
            Assert.Equal("B,a", string.Join(",", list.ToArray()));

            list = new SetList<string>(2, StringComparer.OrdinalIgnoreCase);
            list.Add("a");
            list.Add("B");
            Assert.Equal("a,B", string.Join(",", list.ToArray()));

            list = new SetList<string>(new[] { "B", "a" }, StringComparer.Ordinal);
            Assert.Equal("B,a", string.Join(",", list.ToArray()));

            list = new SetList<string>((IEnumerable<string>)new[] { "B", "a" }, StringComparer.OrdinalIgnoreCase);
            Assert.Equal("a,B", string.Join(",", list.ToArray()));
        }

        [Fact]
        public void TestICollection()
        {
            SetList<int> list = new SetList<int>();
            list.AddRange(new[] { 5, 10, 20 });

            ICollection coll = list;
            Assert.False(coll.IsSynchronized);
            Assert.True(ReferenceEquals(coll, coll.SyncRoot));

            int[] copy = new int[3];
            coll.CopyTo(copy, 0);
            Assert.Equal(5, copy[0]);
            Assert.Equal(10, copy[1]);
            Assert.Equal(20, copy[2]);

            List<int> tmp = new List<int>();
            foreach (int i in coll)
                tmp.Add(i);
            Assert.Equal(3, tmp.Count);
            Assert.Equal(5, tmp[0]);
            Assert.Equal(10, tmp[1]);
            Assert.Equal(20, tmp[2]);
        }

        [Fact]
        public void TestIList()
        {
            IList list = new SetList<string>(StringComparer.Ordinal);
            Assert.Equal(0, list.Count);
            list.Add("a");
            Assert.Equal(1, list.Count);
            list.Add("B");
            Assert.Equal(2, list.Count);

            Assert.Equal("B", list[0]);
            Assert.Equal("a", list[1]);

            Assert.True(list.Contains("a"));
            Assert.False(list.Contains("A"));
            Assert.False(list.Contains("b"));
            Assert.True(list.Contains("B"));

            Assert.Equal(0, list.IndexOf("B"));
            Assert.Equal(1, list.IndexOf("a"));

            list = new SetList<string>((IEnumerable<string>)list, StringComparer.OrdinalIgnoreCase);
            Assert.Equal(2, list.Count);
            Assert.Equal("a", list[0]);
            Assert.Equal("B", list[1]);

            Assert.True(list.Contains("a"));
            Assert.True(list.Contains("A"));
            Assert.True(list.Contains("b"));
            Assert.True(list.Contains("B"));

            Assert.False(list.Contains(5));
            Assert.True(list.IndexOf(5) < 0);

            Assert.False(list.IsFixedSize);
            list.Remove("b");
            Assert.False(list.Contains("b"));
            Assert.False(list.Contains("B"));

            list = ((ICloneable<SetList<string>>)list).Clone();
            Assert.Equal(typeof(SetList<string>), list.GetType());
            Assert.True(list.Contains("a"));
            Assert.False(list.Contains("b"));
            Assert.Equal(0, list.IndexOf("a"));
        }

        [Fact]
        public void TestIntersectUnion()
        {
            SetList<int> lista = new SetList<int>(new[] { 5, 10, 20 });
            SetList<int> listb = new SetList<int>(new[] { 2, 4, 6, 8, 10 });

            SetList<int> union = lista.UnionWith(listb);
            Assert.Equal(7, union.Count);
            foreach (int i in union)
                Assert.True(lista.Contains(i) || listb.Contains(i));

            Assert.Equal(0, union.IndexOf(2));
            Assert.Equal(6, union.IndexOf(20));

            SetList<int> inter = lista.IntersectWith(listb);
            Assert.Equal(1, inter.Count);
            foreach (int i in inter)
                Assert.Equal(10, i);
        }

        [Fact]
        public void TestReplaceAll()
        {
            MyValue one = new MyValue(1);
            MyValue two = new MyValue(2);
            SetList<MyValue> set = new SetList<MyValue>();
            set.Add(one);
            set.Add(two);

            Assert.Equal(2, set.Count);
            Assert.True(ReferenceEquals(one, set[0]));
            Assert.True(set.ReplaceAll(new[] { new MyValue(1), new MyValue(3) }));
            Assert.False(ReferenceEquals(one, set[0]));
            Assert.Equal(3, set.Count);
        }

        [Fact]
        public void TestReplaceOne()
        {
            MyValue one = new MyValue(1);
            MyValue two = new MyValue(2);
            SetList<MyValue> set = new SetList<MyValue>();
            set.Add(one);
            set.Add(two);

            Assert.True(ReferenceEquals(one, set[0]));
            Assert.True(set.Replace(new MyValue(1)));
            Assert.False(ReferenceEquals(one, set[0]));

            Assert.Equal(2, set.Count);
            Assert.False(set.Replace(new MyValue(3))); //not replaced, then added
            Assert.Equal(3, set.Count);
        }

        [Fact]
        public void TestSubsets()
        {
            SetList<int> lista = new SetList<int>(new[] { 5, 10, 20 });
            SetList<int> listb = new SetList<int>(new[] { 2, 4, 6, 8, 10 });

            SetList<int> subt = lista.SubtractSet(listb);
            Assert.False(subt.IsEqualTo(lista));
            Assert.True(subt.Contains(5));
            Assert.False(subt.Contains(10));
            Assert.True(subt.IsEqualTo(new SetList<int>(new[] { 5, 20 })));

            Assert.True(subt.IsSubsetOf(lista));
            Assert.False(subt.IsSupersetOf(lista));

            Assert.True(lista.IsSupersetOf(subt));
            Assert.False(lista.IsSubsetOf(subt));

            SetList<int> copy = lista.Clone();
            copy.RemoveAll(listb);
            Assert.False(copy.IsEqualTo(lista));
            Assert.True(copy.IsEqualTo(subt));

            copy.Add(11);
            Assert.False(copy.IsEqualTo(lista));

            SetList<int> xor = lista.ExclusiveOrWith(listb);
            Assert.True(xor.IsEqualTo(new SetList<int>(new[] { 2, 4, 6, 8, 5, 20 })));

            SetList<int> comp = lista.ComplementOf(listb);
            Assert.True(comp.IsEqualTo(new SetList<int>(new[] { 2, 4, 6, 8 })));
        }
    }

    public class TestSetListNegative
    {
        [Fact]
        public void TestBadArgumentTypeForAdd()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                IList list = new SetList<string>();
                list.Add(5);
            });
        }

        [Fact]
        public void TestBadArgumentTypeForRemove()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                IList list = new SetList<string>();
                list.Remove(5);
            });
        }

        [Fact]
        public void TestBadCapacity()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                IList list = new SetList<string>(-1);
            });
        }

        [Fact]
        public void TestBadComparer()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                IComparer<string> novalue = null;
                IList list = new SetList<string>(novalue);
            });
        }

        [Fact]
        public void TestBadEnumerable()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                IEnumerable<string> novalue = null;
                IList list = new SetList<string>(novalue);
            });
        }

        [Fact]
        public void TestInsertAt()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                IList list = new SetList<string>(StringComparer.Ordinal);
                list.Insert(0, "");
            });
        }

        [Fact]
        public void TestInsertAt2()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                IList<string> list = new SetList<string>(StringComparer.Ordinal);
                list.Insert(0, "");
            });
        }

        [Fact]
        public void TestSetIndex()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                IList list = new SetList<string>();
                list.Add("");
                Assert.Equal("", list[0]);
                list[0] = "error";
            });
        }

        [Fact]
        public void TestSetIndex2()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                SetList<string> list = new SetList<string>();
                list.Add("");
                Assert.Equal("", list[0]);
                list[0] = "error";
            });
        }
    }
}