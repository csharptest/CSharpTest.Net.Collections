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

//#define DEBUG

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using CSharpTest.Net.Interfaces;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    public class TestBTreeDictionary : TestDictionary<BTreeDictionary<int, string>, TestBTreeDictionary.BTreeFactory,
        int, string>
    {
        protected static IComparer<int> Comparer => Comparer<int>.Default;

        public class BTreeFactory : IFactory<BTreeDictionary<int, string>>
        {
            public BTreeDictionary<int, string> Create()
            {
                return new BTreeDictionary<int, string>(5, Comparer);
            }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            Random r = new Random();
            Dictionary<int, string> sample = new Dictionary<int, string>();
            for (int i = 0; i < 1000; i++)
            {
                int value = r.Next();
                sample[value] = value.ToString();
            }
            return new List<KeyValuePair<int, string>>(sample).ToArray();
        }

        private void SequencedTest(int start, int incr, int stop, string name)
        {
            int count = Math.Abs(start - stop) / Math.Abs(incr);
            const string myTestValue1 = "T1", myTestValue2 = "t2";
            string test;

            BTreeDictionary<int, string> tree = new BTreeDictionary<int, string>(8, Comparer);
            FactoryMethod<BTreeDictionary<int, string>> verify = delegate
            {
                tree.DebugAssert();
                return tree;
            };
            Stopwatch time = new Stopwatch();
            time.Start();
            //large order-forward
            for (int i = start; i != stop; i += incr)
            {
                if (i == 988)
                    i = 988;
                if (!verify().TryAdd(i, myTestValue1)) throw new Exception();
            }

            Trace.TraceInformation("{0} insert  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryGetValue(i, out test) || test != myTestValue1) throw new Exception();

            Trace.TraceInformation("{0} seek    {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryUpdate(i, myTestValue2)) throw new Exception();

            Trace.TraceInformation("{0} modify  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryGetValue(i, out test) || test != myTestValue2) throw new Exception();

            Trace.TraceInformation("{0} seek#2  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            int tmpCount = 0;
            foreach (KeyValuePair<int, string> tmp in verify())
                if (tmp.Value != myTestValue2) throw new Exception();
                else tmpCount++;
            if (tmpCount != count) throw new Exception();

            Trace.TraceInformation("{0} foreach {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
            {
                if (i == 16)
                    i = 16;
                if (!verify().Remove(i)) throw new Exception();
            }

            Trace.TraceInformation("{0} delete  {1} in {2}", name, count, time.ElapsedMilliseconds);

            for (int i = start; i != stop; i += incr)
                if (verify().TryGetValue(i, out test)) throw new Exception();
        }

        [Fact]
        public void TestArray()
        {
            List<KeyValuePair<int, string>> sample = new List<KeyValuePair<int, string>>(GetSample());
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, sample);

            sample.Sort((a, b) => data.Comparer.Compare(a.Key, b.Key));
            KeyValuePair<int, string>[] array = data.ToArray();

            Assert.Equal(sample.Count, array.Length);
            for (int i = 0; i < sample.Count; i++)
            {
                Assert.Equal(sample[i].Key, array[i].Key);
                Assert.Equal(sample[i].Value, array[i].Value);
            }
        }

        [Fact]
        public void TestClone()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            BTreeDictionary<int, string> copy = ((ICloneable<BTreeDictionary<int, string>>)data).Clone();
            using (IEnumerator<KeyValuePair<int, string>> e1 = data.GetEnumerator())
            using (IEnumerator<KeyValuePair<int, string>> e2 = copy.GetEnumerator())
            {
                while (e1.MoveNext() && e2.MoveNext())
                {
                    Assert.Equal(e1.Current.Key, e2.Current.Key);
                    Assert.Equal(e1.Current.Value, e2.Current.Value);
                }
                Assert.False(e1.MoveNext() || e2.MoveNext());
            }
        }

        [Fact]
        public void TestEnumerateFrom()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.True(data.TryAdd(i, i.ToString()));

            Assert.Equal(50, new List<KeyValuePair<int, string>>(data.EnumerateFrom(50)).Count);
            Assert.Equal(25, new List<KeyValuePair<int, string>>(data.EnumerateFrom(75)).Count);

            for (int i = 0; i < 100; i++)
            {
                int first = -1;
                foreach (KeyValuePair<int, string> kv in data.EnumerateFrom(i))
                {
                    first = kv.Key;
                    break;
                }
                Assert.Equal(i, first);
            }
        }

        [Fact]
        public void TestFirstAndLast()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>();
            data.Add(1, "a");
            data.Add(2, "b");
            data.Add(3, "c");
            data.Add(4, "d");
            data.Add(5, "e");

            Assert.Equal(1, data.First().Key);
            Assert.Equal("a", data.First().Value);
            data.Remove(1);
            Assert.Equal(2, data.First().Key);
            Assert.Equal("b", data.First().Value);

            Assert.Equal(5, data.Last().Key);
            Assert.Equal("e", data.Last().Value);
            data.Remove(5);
            Assert.Equal(4, data.Last().Key);
            Assert.Equal("d", data.Last().Value);

            data.Remove(4);
            data.Remove(3);

            KeyValuePair<int, string> kv;
            Assert.True(data.TryGetLast(out kv));
            Assert.True(data.TryGetFirst(out kv));
            data.Remove(2);
            Assert.False(data.TryGetLast(out kv));
            Assert.False(data.TryGetFirst(out kv));

            try
            {
                data.First();
                Assert.True(false,"Should raise InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
            try
            {
                data.Last();
                Assert.True(false,"Should raise InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
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
        public void TestGetOrAdd()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            Assert.Equal("a", data.GetOrAdd(1, "a"));
            Assert.Equal("a", data.GetOrAdd(1, "b"));
        }

        [Fact]
        public void TestIndexer()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            BTreeDictionary<int, string> copy = new BTreeDictionary<int, string>();
            copy.AddRange(data);
            Assert.Equal(copy.Count, data.Count);
            foreach (int key in data.Keys)
                Assert.Equal(data[key], copy[key]);
        }

        [Fact]
        public void TestKeyValueCollections()
        {
            List<KeyValuePair<int, string>> sample = new List<KeyValuePair<int, string>>(GetSample());
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(sample);
            //Key collection
            Assert.Equal(data.Count, data.Keys.Count);
            Assert.True(data.Keys.IsReadOnly);
            for (int i = 0; i < sample.Count && i < 5; i++)
                Assert.True(data.Keys.Contains(sample[i].Key));

            IEnumerator<int> ek = data.Keys.GetEnumerator();
            Assert.True(ek.MoveNext());
            int firstkey = ek.Current;
            Assert.True(ek.MoveNext());
            Assert.NotEqual(firstkey, ek.Current);
            ek.Reset();
            Assert.True(ek.MoveNext());
            Assert.Equal(firstkey, ek.Current);
            Assert.Equal(firstkey, ((IEnumerator)ek).Current);

            //Value collection
            Assert.Equal(data.Count, data.Values.Count);
            Assert.True(data.Values.IsReadOnly);
            for (int i = 0; i < sample.Count && i < 5; i++)
                Assert.True(data.Values.Contains(sample[i].Value));

            IEnumerator<string> ev = data.Values.GetEnumerator();
            Assert.True(ev.MoveNext());
            string firstvalue = ev.Current;
            Assert.True(ev.MoveNext());
            Assert.NotEqual(firstvalue, ev.Current);
            ev.Reset();
            Assert.True(ev.MoveNext());
            Assert.Equal(firstvalue, ((IEnumerator)ev).Current);
        }

        [Fact]
        public void TestRangeEnumerate()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.True(data.TryAdd(i, i.ToString()));

            int ix = 0;
            foreach (KeyValuePair<int, string> kv in data.EnumerateRange(-500, 5000))
                Assert.Equal(ix++, kv.Key);
            Assert.Equal(100, ix);

            foreach (
                KeyValuePair<int, int> range in
                new Dictionary<int, int> { { 6, 25 }, { 7, 25 }, { 8, 25 }, { 9, 25 }, { 22, 25 }, { 28, 28 } })
            {
                ix = range.Key;
                foreach (KeyValuePair<int, string> kv in data.EnumerateRange(ix, range.Value))
                    Assert.Equal(ix++, kv.Key);
                Assert.Equal(range.Value, ix - 1);
            }
        }

        [Fact]
        public void TestReadOnly()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            Assert.False(data.IsReadOnly);

            BTreeDictionary<int, string> copy = data.MakeReadOnly();
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
        public void TestReverseInsertTo1000()
        {
            SequencedTest(1000, -1, 0, "Reverse");
        }

        [Fact]
        public void TestReverseInsertTo5000()
        {
            SequencedTest(5000, -1, 0, "Reverse");
        }

        [Fact]
        public void TestTryRoutines()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            Assert.True(data.TryAdd(1, "a"));
            Assert.False(data.TryAdd(1, "a"));

            Assert.True(data.TryUpdate(1, "a"));
            Assert.True(data.TryUpdate(1, "c"));
            Assert.True(data.TryUpdate(1, "d", "c"));
            Assert.False(data.TryUpdate(1, "f", "c"));
            Assert.Equal("d", data[1]);
            Assert.True(data.TryUpdate(1, "a", data[1]));
            Assert.Equal("a", data[1]);
            Assert.False(data.TryUpdate(2, "b"));

            string val;
            Assert.True(data.TryRemove(1, out val) && val == "a");
            Assert.False(data.TryRemove(2, out val));
            Assert.NotEqual(val, "a");
        }
    }
}