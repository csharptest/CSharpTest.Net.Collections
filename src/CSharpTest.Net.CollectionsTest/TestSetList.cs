#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Diagnostics;
using CSharpTest.Net.Bases;
using NUnit.Framework;
using System.Collections.Generic;
using CSharpTest.Net.Collections;
using System.Collections;

#pragma warning disable 1591

namespace CSharpTest.Net.Library.Test
{
	[TestFixture]
	[Category("TestSetList")]
	public partial class TestSetList
	{
		[Test, Explicit]
		public void BenchmarkTest()
		{
			this.TestBasics();
			this.TestICollection();
			this.TestIList();
			this.TestIntersectUnion();
			this.TestSubsets();

			long temp = 0;
			int repeat = 10;
			for (int i = 0; i < repeat; i++)
			{
				Stopwatch sw = new Stopwatch();
				sw.Start();

				for (int i2 = 0; i2 < 10000; i2++)
				{
					this.TestBasics();
					this.TestICollection();
					this.TestIList();
					this.TestIntersectUnion();
					this.TestSubsets();
				}

				sw.Stop();
				Console.Error.WriteLine("Benchmark: {0}", sw.ElapsedMilliseconds);
				temp += sw.ElapsedMilliseconds;
			}

			Console.Error.WriteLine("Average over {0}: {1}", repeat, temp / repeat);
		}


		[Test]
        public void TestBasics()
        {
            SetList<int> list = new SetList<int>();
            Assert.IsFalse(list.IsReadOnly);

            for (int i = 512; i >= 0; i--)
                list.Add(i);

            int offset = 0;
            foreach (int item in list)
                Assert.AreEqual(offset++, item);

            Assert.AreEqual(513, offset);
            Assert.AreEqual(513, list.Count);

            list.Clear();
            list.AddRange(new int[] { 5, 10, 20 });
            list.AddRange(new int[] { });

            Assert.AreEqual(3, list.Count);

            Assert.IsTrue(list.Contains(20));
            Assert.IsTrue(list.Remove(20));

            Assert.IsFalse(list.Contains(20));
            Assert.IsFalse(list.Remove(20));

            Assert.AreEqual(2, list.Count);

            int pos;
            list.Add(10, out pos);
            Assert.AreEqual(1, pos);
            Assert.AreEqual(2, list.Count);
            
            int[] items = new int[2];
            list.CopyTo(items, 0);
            Assert.AreEqual(5, items[0]);
            Assert.AreEqual(10, items[1]);

            items = list.ToArray();
            Assert.AreEqual(5, items[0]);
            Assert.AreEqual(10, items[1]);

            List<int> tmp = new List<int>();
            foreach (int i in list)
                tmp.Add(i);
            Assert.AreEqual(2, tmp.Count);
            Assert.AreEqual(5, tmp[0]);
            Assert.AreEqual(10, tmp[1]);
        }

		class MyValue : Comparable<MyValue>
		{
			public MyValue(int value) { Value = value; }
			public readonly int Value;
			public override int CompareTo(MyValue other) { return Value.CompareTo(other.Value); }
			protected override int HashCode { get { return Value.GetHashCode(); } }
		}

		[Test]
		public void TestReplaceOne()
		{
			MyValue one = new MyValue(1);
			MyValue two = new MyValue(2);
			SetList<MyValue> set = new SetList<MyValue>();
			set.Add(one);
			set.Add(two);

			Assert.IsTrue(Object.ReferenceEquals(one, set[0]));
			Assert.IsTrue(set.Replace(new MyValue(1)));
			Assert.IsFalse(Object.ReferenceEquals(one, set[0]));

			Assert.AreEqual(2, set.Count);
			Assert.IsFalse(set.Replace(new MyValue(3))); //not replaced, then added
			Assert.AreEqual(3, set.Count);
		}

		[Test]
		public void TestReplaceAll()
		{
			MyValue one = new MyValue(1);
			MyValue two = new MyValue(2);
			SetList<MyValue> set = new SetList<MyValue>();
			set.Add(one);
			set.Add(two);

			Assert.AreEqual(2, set.Count);
			Assert.IsTrue(Object.ReferenceEquals(one, set[0]));
			Assert.IsTrue(set.ReplaceAll(new MyValue[] { new MyValue(1), new MyValue(3) }));
			Assert.IsFalse(Object.ReferenceEquals(one, set[0]));
			Assert.AreEqual(3, set.Count);
		}

        [Test]
        public void TestICollection()
        {
            SetList<int> list = new SetList<int>();
            list.AddRange(new int[] { 5, 10, 20 });

            ICollection coll = list;
            Assert.IsFalse(coll.IsSynchronized);
            Assert.IsTrue(Object.ReferenceEquals(coll, coll.SyncRoot));

            int[] copy = new int[3];
            coll.CopyTo(copy, 0);
            Assert.AreEqual(5, copy[0]);
            Assert.AreEqual(10, copy[1]);
            Assert.AreEqual(20, copy[2]);

            List<int> tmp = new List<int>();
            foreach (int i in coll)
                tmp.Add(i);
            Assert.AreEqual(3, tmp.Count);
            Assert.AreEqual(5, tmp[0]);
            Assert.AreEqual(10, tmp[1]);
            Assert.AreEqual(20, tmp[2]);
        }

        [Test]
        public void TestIntersectUnion()
        {
            SetList<int> lista = new SetList<int>(new int[] { 5, 10, 20 });
            SetList<int> listb = new SetList<int>(new int[] { 2, 4, 6, 8, 10 });

            SetList<int> union = lista.UnionWith(listb);
            Assert.AreEqual(7, union.Count);
            foreach (int i in union)
                Assert.IsTrue(lista.Contains(i) || listb.Contains(i));

            Assert.AreEqual(0, union.IndexOf(2));
            Assert.AreEqual(6, union.IndexOf(20));

            SetList<int> inter = lista.IntersectWith(listb);
            Assert.AreEqual(1, inter.Count);
            foreach (int i in inter)
                Assert.AreEqual(10, i);
        }

        [Test]
        public void TestIList()
        {
            IList list = new SetList<string>(StringComparer.Ordinal);
            Assert.AreEqual(0, list.Count);
            list.Add("a");
            Assert.AreEqual(1, list.Count);
            list.Add("B");
            Assert.AreEqual(2, list.Count);

            Assert.AreEqual("B", list[0]);
            Assert.AreEqual("a", list[1]);

            Assert.IsTrue(list.Contains("a"));
            Assert.IsFalse(list.Contains("A"));
            Assert.IsFalse(list.Contains("b"));
            Assert.IsTrue(list.Contains("B"));

            Assert.AreEqual(0, list.IndexOf("B"));
            Assert.AreEqual(1, list.IndexOf("a"));

            list = new SetList<string>((IEnumerable<string>)list, StringComparer.OrdinalIgnoreCase);
            Assert.AreEqual(2, list.Count);
            Assert.AreEqual("a", list[0]);
            Assert.AreEqual("B", list[1]);

            Assert.IsTrue(list.Contains("a"));
            Assert.IsTrue(list.Contains("A"));
            Assert.IsTrue(list.Contains("b"));
            Assert.IsTrue(list.Contains("B"));

            Assert.IsFalse(list.Contains(5));
            Assert.IsTrue(list.IndexOf(5) < 0);

            Assert.IsFalse(list.IsFixedSize);
            list.Remove("b");
            Assert.IsFalse(list.Contains("b"));
            Assert.IsFalse(list.Contains("B"));

            list = (IList)((ICloneable)list).Clone();
            Assert.AreEqual(typeof(SetList<string>), list.GetType());
            Assert.IsTrue(list.Contains("a"));
            Assert.IsFalse(list.Contains("b"));
            Assert.AreEqual(0, list.IndexOf("a"));
        }

        [Test]
        public void TestSubsets()
        {
            SetList<int> lista = new SetList<int>(new int[] { 5, 10, 20 });
            SetList<int> listb = new SetList<int>(new int[] { 2, 4, 6, 8, 10 });

            SetList<int>subt = lista.SubtractSet(listb);
            Assert.IsFalse(subt.IsEqualTo(lista));
            Assert.IsTrue(subt.Contains(5));
            Assert.IsFalse(subt.Contains(10));
            Assert.IsTrue(subt.IsEqualTo(new SetList<int>(new int[] { 5, 20 })));

            Assert.IsTrue(subt.IsSubsetOf(lista));
            Assert.IsFalse(subt.IsSupersetOf(lista));

            Assert.IsTrue(lista.IsSupersetOf(subt));
            Assert.IsFalse(lista.IsSubsetOf(subt));
            
            SetList<int> copy = lista.Clone();
            copy.RemoveAll(listb);
            Assert.IsFalse(copy.IsEqualTo(lista));
            Assert.IsTrue(copy.IsEqualTo(subt));

            copy.Add(11);
            Assert.IsFalse(copy.IsEqualTo(lista));

            SetList<int> xor = lista.ExclusiveOrWith(listb);
            Assert.IsTrue(xor.IsEqualTo(new SetList<int>(new int[] { 2, 4, 6, 8, 5, 20 })));

            SetList<int> comp = lista.ComplementOf(listb);
            Assert.IsTrue(comp.IsEqualTo(new SetList<int>(new int[] { 2, 4, 6, 8 })));
        }

        [Test]
        public void TestCTors()
        {
            SetList<string> list = new SetList<string>((IEnumerable<string>)new string[] { "a", "B" });
            Assert.AreEqual("a,B", String.Join(",", list.ToArray())); 
            
            list = new SetList<string>(2);
            Assert.AreEqual("", String.Join(",", list.ToArray()));
            list.Add("a");
            list.Add("B");
            Assert.AreEqual("a,B", String.Join(",", list.ToArray()));

            list = new SetList<string>(2, StringComparer.Ordinal);
            Assert.AreEqual("", String.Join(",", list.ToArray()));
            list.Add("a");
            list.Add("B");
            Assert.AreEqual("B,a", String.Join(",", list.ToArray()));

            list = new SetList<string>(2, StringComparer.OrdinalIgnoreCase);
            list.Add("a");
            list.Add("B");
            Assert.AreEqual("a,B", String.Join(",", list.ToArray()));

            list = new SetList<string>(new string[] { "B", "a" }, StringComparer.Ordinal);
            Assert.AreEqual("B,a", String.Join(",", list.ToArray()));

            list = new SetList<string>((IEnumerable<string>)new string[] { "B", "a" }, StringComparer.OrdinalIgnoreCase);
            Assert.AreEqual("a,B", String.Join(",", list.ToArray()));
        }
	}

    [TestFixture]
    [Category("TestSetList")]
    public partial class TestSetListNegative
    {
        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestBadCapacity()
        {
            IList list = new SetList<string>(-1);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestBadEnumerable()
        {
            IEnumerable<string> novalue = null;
            IList list = new SetList<string>(novalue);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void TestBadComparer()
        {
            IComparer<string> novalue = null;
            IList list = new SetList<string>(novalue);
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void TestInsertAt()
        {
            IList list = new SetList<string>(StringComparer.Ordinal);
            list.Insert(0, "");
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void TestInsertAt2()
        {
            IList<string> list = new SetList<string>(StringComparer.Ordinal);
            list.Insert(0, "");
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void TestSetIndex()
        {
            IList list = new SetList<string>();
            list.Add("");
            Assert.AreEqual("", list[0]);
            list[0] = "error";
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void TestSetIndex2()
        {
            SetList<string> list = new SetList<string>();
            list.Add("");
            Assert.AreEqual("", list[0]);
            list[0] = "error";
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBadArgumentTypeForAdd()
        {
            IList list = new SetList<string>();
            list.Add(5);
        }

        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBadArgumentTypeForRemove()
        {
            IList list = new SetList<string>();
            list.Remove(5);
        }
    }
}
