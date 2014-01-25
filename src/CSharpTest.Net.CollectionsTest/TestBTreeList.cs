#region Copyright 2012-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using NUnit.Framework;
using System.Diagnostics;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestBTreeList : CSharpTest.Net.BPlusTree.Test.TestCollection<BTreeList<int>, TestBTreeList.BTreeFactory, int>
    {
        protected static IComparer<int> Comparer { get { return Comparer<int>.Default; } }

        public class BTreeFactory : IFactory<BTreeList<int>>
        {
            public BTreeList<int> Create()
            {
                return new BTreeList<int>(5, Comparer);
            }
        }

        protected override int[] GetSample()
        {
            Random r = new Random();
            Dictionary<int, int> sample = new Dictionary<int, int>();
            for(int i=0; i < 1000; i++)
            {
                int value = r.Next();
                sample[value] = value;
            }
            return new List<int>(sample.Keys).ToArray();
        }

        [Test]
        public void TestIndexOf()
        {
            BTreeList<int> test = new BTreeList<int>();
            IList<int> list = test;

            for (int i = 20; i >= 0; i--)
                test.Add(i);

            Assert.AreEqual(-1, list.IndexOf(int.MaxValue));
            Assert.AreEqual(-1, list.IndexOf(int.MinValue));

            for (int i=0; i <=20; i++)
            {
                Assert.AreEqual(i, list.IndexOf(i));
                Assert.AreEqual(i, list[i]);
            }
        }

        [Test]
        public void TestRemoveAt()
        {
            BTreeList<int> test = new BTreeList<int>(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            IList<int> list = test;
            for (int i = 10; i < 1000; i++)
                test.Add(i);

            for(int i=900; i >0; i-=100)
            {
                Assert.IsTrue(test.Contains(i));
                Assert.AreEqual(i, list.IndexOf(i));
                list.RemoveAt(i);
                Assert.IsFalse(test.Contains(i));
                Assert.AreEqual(-1, list.IndexOf(i));
                Assert.AreEqual(i + 1, list[i]);
            }

            list.RemoveAt(0);
            list.RemoveAt(1);
            list.RemoveAt(2);
            Assert.AreEqual(1, list[0]);
            Assert.AreEqual(3, list[1]);
            Assert.AreEqual(5, list[2]);

            Assert.AreEqual(1000 - 12, list.Count);
        }

        [Test]
        public void TestRangeEnumerate()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryAddItem(i));

            int ix = 0;
            foreach (int kv in data.EnumerateRange(-500, 5000))
                Assert.AreEqual(ix++, kv);
            Assert.AreEqual(100, ix);

            foreach (
                KeyValuePair<int, int> range in
                    new Dictionary<int, int> {{6, 25}, {7, 25}, {8, 25}, {9, 25}, {22, 25}, {28, 28}})
            {
                ix = range.Key;
                foreach (int kv in data.EnumerateRange(ix, range.Value))
                    Assert.AreEqual(ix++, kv);
                Assert.AreEqual(range.Value, ix - 1);
            }
        }

        [Test]
        public void TestClone()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            BTreeList<int> copy = (BTreeList<int>) ((ICloneable) data).Clone();
            using(IEnumerator<int> e1 = data.GetEnumerator())
            using(IEnumerator<int> e2 = copy.GetEnumerator())
            {
                while(e1.MoveNext() && e2.MoveNext())
                {
                    Assert.AreEqual(e1.Current, e2.Current);
                }
                Assert.IsFalse(e1.MoveNext() || e2.MoveNext());
            }
        }

        [Test]
        public void TestIndexer()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            BTreeList<int> copy = new BTreeList<int>();
            copy.AddRange(data);
            Assert.AreEqual(copy.Count, data.Count);
            IList<int> lista = data, listb = copy;
            for (int ix = 0; ix < data.Count; ix++)
                Assert.AreEqual(lista[ix], listb[ix]);
        }

        [Test]
        public void TestReadOnly()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            Assert.IsFalse(data.IsReadOnly);
            
            BTreeList<int> copy = data.MakeReadOnly();
            Assert.IsFalse(ReferenceEquals(data, copy));
            Assert.AreEqual(data.Count, copy.Count);
            Assert.IsTrue(copy.IsReadOnly);

            Assert.IsTrue(ReferenceEquals(copy, copy.MakeReadOnly()));
            data = copy.Clone();
            Assert.IsFalse(data.IsReadOnly);
            Assert.IsFalse(ReferenceEquals(copy, data));
            Assert.AreEqual(data.Count, copy.Count);
        }

        [Test]
        public void TestArray()
        {
            List<int> sample = new List<int>(GetSample());
            BTreeList<int> data = new BTreeList<int>(Comparer, sample);

            sample.Sort((a, b) => data.Comparer.Compare(a, b));
            int[] array = data.ToArray();

            Assert.AreEqual(sample.Count, array.Length);
            for( int i=0; i < sample.Count; i++)
            {
                Assert.AreEqual(sample[i], array[i]);
            }
        }

        [Test]
        public void TestEnumerateFrom()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryAddItem(i));

            Assert.AreEqual(50, new List<int>(data.EnumerateFrom(50)).Count);
            Assert.AreEqual(25, new List<int>(data.EnumerateFrom(75)).Count);

            for (int i = 0; i < 100; i++)
            {
                int first = -1;
                foreach (int kv in data.EnumerateFrom(i))
                {
                    first = kv;
                    break;
                }
                Assert.AreEqual(i, first);
            }
        }

        [Test, ExpectedException(typeof(NotSupportedException))]
        public void TestExceptionOnInsertAt()
        {
            IList<int> tmp = new BTreeList<int>();
            tmp.Insert(0, 0);
            Assert.Fail("Should throw NotSupportedException");
        }

        [Test, ExpectedException(typeof(NotSupportedException))]
        public void TestExceptionOnModifyIndex()
        {
            IList<int> tmp = new BTreeList<int>();
            tmp.Add(0);
            tmp[0] = 1;
            Assert.Fail("Should throw NotSupportedException");
        }

        [Test]
        public void TestForwardInsertTo1000()
        {
            SequencedTest(0, 1, 1000, "Forward");
        }

        [Test]
        public void TestForwardInsertTo5000()
        {
            SequencedTest(0, 1, 5000, "Forward");
        }

        [Test]
        public void TestReverseInsertTo1000()
        {
            SequencedTest(1000, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo5000()
        {
            SequencedTest(5000, -1, 0, "Reverse");
        }

        void SequencedTest(int start, int incr, int stop, string name)
        {
            int count = Math.Abs(start - stop) / Math.Abs(incr);

            BTreeList<int> data = new BTreeList<int>(Comparer);
            Stopwatch time = new Stopwatch();
            time.Start();
            //large order-forward
            for (int i = start; i != stop; i += incr)
                if (!data.TryAddItem(i)) throw new ApplicationException();

            Trace.TraceInformation("{0} insert  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!data.Contains(i)) throw new ApplicationException();

            Trace.TraceInformation("{0} seek    {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            int tmpCount = 0;
            foreach (int tmp in data)
                if (tmp < Math.Min(start, stop) || tmp > Math.Max(start, stop)) throw new ApplicationException();
                else tmpCount++;
            if (tmpCount != count) throw new ApplicationException();

            Trace.TraceInformation("{0} foreach {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!data.Remove(i)) throw new ApplicationException();

            Trace.TraceInformation("{0} delete  {1} in {2}", name, count, time.ElapsedMilliseconds);

            for (int i = start; i != stop; i += incr)
                if (data.Contains(i)) throw new ApplicationException();
        }
    }
}
