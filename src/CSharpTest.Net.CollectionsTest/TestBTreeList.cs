#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Diagnostics;
using CSharpTest.Net.Interfaces;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{

    public class TestBTreeList : TestCollection<BTreeList<int>, TestBTreeList.BTreeFactory, int>
    {
        protected static IComparer<int> Comparer => Comparer<int>.Default;

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
            for (int i = 0; i < 1000; i++)
            {
                int value = r.Next();
                sample[value] = value;
            }
            return new List<int>(sample.Keys).ToArray();
        }

        private void SequencedTest(int start, int incr, int stop, string name)
        {
            int count = Math.Abs(start - stop) / Math.Abs(incr);

            BTreeList<int> data = new BTreeList<int>(Comparer);
            Stopwatch time = new Stopwatch();
            time.Start();
            //large order-forward
            for (int i = start; i != stop; i += incr)
                if (!data.TryAddItem(i)) throw new Exception();

            Trace.TraceInformation("{0} insert  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!data.Contains(i)) throw new Exception();

            Trace.TraceInformation("{0} seek    {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            int tmpCount = 0;
            foreach (int tmp in data)
                if (tmp < Math.Min(start, stop) || tmp > Math.Max(start, stop)) throw new Exception();
                else tmpCount++;
            if (tmpCount != count) throw new Exception();

            Trace.TraceInformation("{0} foreach {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!data.Remove(i)) throw new Exception();

            Trace.TraceInformation("{0} delete  {1} in {2}", name, count, time.ElapsedMilliseconds);

            for (int i = start; i != stop; i += incr)
                if (data.Contains(i)) throw new Exception();
        }

        [Fact]
        public void TestArray()
        {
            List<int> sample = new List<int>(GetSample());
            BTreeList<int> data = new BTreeList<int>(Comparer, sample);

            sample.Sort((a, b) => data.Comparer.Compare(a, b));
            int[] array = data.ToArray();

            Assert.Equal(sample.Count, array.Length);
            for (int i = 0; i < sample.Count; i++)
                Assert.Equal(sample[i], array[i]);
        }

        [Fact]
        public void TestClone()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            BTreeList<int> copy = ((ICloneable<BTreeList<int>>)data).Clone();
            using (IEnumerator<int> e1 = data.GetEnumerator())
            using (IEnumerator<int> e2 = copy.GetEnumerator())
            {
                while (e1.MoveNext() && e2.MoveNext())
                    Assert.Equal(e1.Current, e2.Current);
                Assert.False(e1.MoveNext() || e2.MoveNext());
            }
        }

        [Fact]
        public void TestEnumerateFrom()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.True(data.TryAddItem(i));

            Assert.Equal(50, new List<int>(data.EnumerateFrom(50)).Count);
            Assert.Equal(25, new List<int>(data.EnumerateFrom(75)).Count);

            for (int i = 0; i < 100; i++)
            {
                int first = -1;
                foreach (int kv in data.EnumerateFrom(i))
                {
                    first = kv;
                    break;
                }
                Assert.Equal(i, first);
            }
        }

        [Fact]
        public void TestExceptionOnInsertAt()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                IList<int> tmp = new BTreeList<int>();
                tmp.Insert(0, 0);
                Assert.True(false, "Should throw NotSupportedException");
            });
        }

        [Fact]
        public void TestExceptionOnModifyIndex()
        {
            Assert.Throws<NotSupportedException>(() =>
            {
                IList<int> tmp = new BTreeList<int>();
                tmp.Add(0);
                tmp[0] = 1;
                Assert.True(false, "Should throw NotSupportedException");
            });
        }

        [Fact]
        public void TestForwardInsertTo1000()
        {
            SequencedTest(0, 1, 1000, "Forward");
        }

        [Fact]
        public void TestForwardInsertTo5000()
        {
            SequencedTest(0, 1, 5000, "Forward");
        }

        [Fact]
        public void TestIndexer()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            BTreeList<int> copy = new BTreeList<int>();
            copy.AddRange(data);
            Assert.Equal(copy.Count, data.Count);
            IList<int> lista = data, listb = copy;
            for (int ix = 0; ix < data.Count; ix++)
                Assert.Equal(lista[ix], listb[ix]);
        }

        [Fact]
        public void TestIndexOf()
        {
            BTreeList<int> test = new BTreeList<int>();
            IList<int> list = test;

            for (int i = 20; i >= 0; i--)
                test.Add(i);

            Assert.Equal(-1, list.IndexOf(int.MaxValue));
            Assert.Equal(-1, list.IndexOf(int.MinValue));

            for (int i = 0; i <= 20; i++)
            {
                Assert.Equal(i, list.IndexOf(i));
                Assert.Equal(i, list[i]);
            }
        }

        [Fact]
        public void TestRangeEnumerate()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.True(data.TryAddItem(i));

            int ix = 0;
            foreach (int kv in data.EnumerateRange(-500, 5000))
                Assert.Equal(ix++, kv);
            Assert.Equal(100, ix);

            foreach (
                KeyValuePair<int, int> range in
                new Dictionary<int, int> { { 6, 25 }, { 7, 25 }, { 8, 25 }, { 9, 25 }, { 22, 25 }, { 28, 28 } })
            {
                ix = range.Key;
                foreach (int kv in data.EnumerateRange(ix, range.Value))
                    Assert.Equal(ix++, kv);
                Assert.Equal(range.Value, ix - 1);
            }
        }

        [Fact]
        public void TestReadOnly()
        {
            BTreeList<int> data = new BTreeList<int>(Comparer, GetSample());
            Assert.False(data.IsReadOnly);

            BTreeList<int> copy = data.MakeReadOnly();
            Assert.False(ReferenceEquals(data, copy));
            Assert.Equal(data.Count, copy.Count);
            Assert.True(copy.IsReadOnly);

            Assert.True(ReferenceEquals(copy, copy.MakeReadOnly()));
            data = copy.Clone();
            Assert.False(data.IsReadOnly);
            Assert.False(ReferenceEquals(copy, data));
            Assert.Equal(data.Count, copy.Count);
        }

        [Fact]
        public void TestRemoveAt()
        {
            BTreeList<int> test = new BTreeList<int>(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            IList<int> list = test;
            for (int i = 10; i < 1000; i++)
                test.Add(i);

            for (int i = 900; i > 0; i -= 100)
            {
                Assert.True(test.Contains(i));
                Assert.Equal(i, list.IndexOf(i));
                list.RemoveAt(i);
                Assert.False(test.Contains(i));
                Assert.Equal(-1, list.IndexOf(i));
                Assert.Equal(i + 1, list[i]);
            }

            list.RemoveAt(0);
            list.RemoveAt(1);
            list.RemoveAt(2);
            Assert.Equal(1, list[0]);
            Assert.Equal(3, list[1]);
            Assert.Equal(5, list[2]);

            Assert.Equal(1000 - 12, list.Count);
        }

        [Fact]
        public void TestReverseInsertTo1000()
        {
            SequencedTest(1000, -1, 0, "Reverse");
        }

        [Fact]
        public void TestReverseInsertTo5000()
        {
            SequencedTest(5000, -1, 0, "Reverse");
        }
    }
}