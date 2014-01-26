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
using System.Collections.Generic;
using NUnit.Framework;
using System.Diagnostics;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestBTreeDictionary : CSharpTest.Net.BPlusTree.Test.TestDictionary<BTreeDictionary<int, string>, TestBTreeDictionary.BTreeFactory, int, string>
    {
        protected static IComparer<int> Comparer { get { return Comparer<int>.Default; } }

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
            for(int i=0; i < 1000; i++)
            {
                int value = r.Next();
                sample[value] = value.ToString();
            }
            return new List<KeyValuePair<int,string>>(sample).ToArray();
        }

        [Test]
        public void TestRangeEnumerate()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryAdd(i, i.ToString()));

            int ix = 0;
            foreach (KeyValuePair<int, string> kv in data.EnumerateRange(-500, 5000))
                Assert.AreEqual(ix++, kv.Key);
            Assert.AreEqual(100, ix);

            foreach (
                KeyValuePair<int, int> range in
                    new Dictionary<int, int> {{6, 25}, {7, 25}, {8, 25}, {9, 25}, {22, 25}, {28, 28}})
            {
                ix = range.Key;
                foreach (KeyValuePair<int, string> kv in data.EnumerateRange(ix, range.Value))
                    Assert.AreEqual(ix++, kv.Key);
                Assert.AreEqual(range.Value, ix - 1);
            }
        }

        [Test]
        public void TestGetOrAdd()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            Assert.AreEqual("a", data.GetOrAdd(1, "a"));
            Assert.AreEqual("a", data.GetOrAdd(1, "b"));
        }

        [Test]
        public void TestTryRoutines()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            Assert.IsTrue(data.TryAdd(1, "a"));
            Assert.IsFalse(data.TryAdd(1, "a"));

            Assert.IsTrue(data.TryUpdate(1, "a"));
            Assert.IsTrue(data.TryUpdate(1, "c"));
            Assert.IsTrue(data.TryUpdate(1, "d", "c"));
            Assert.IsFalse(data.TryUpdate(1, "f", "c"));
            Assert.AreEqual("d", data[1]);
            Assert.IsTrue(data.TryUpdate(1, "a", data[1]));
            Assert.AreEqual("a", data[1]);
            Assert.IsFalse(data.TryUpdate(2, "b"));

            string val;
            Assert.IsTrue(data.TryRemove(1, out val) && val == "a");
            Assert.IsFalse(data.TryRemove(2, out val));
            Assert.AreNotEqual(val, "a");
        }

        [Test]
        public void TestClone()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            BTreeDictionary<int, string> copy = (BTreeDictionary<int, string>) ((ICloneable) data).Clone();
            using(IEnumerator<KeyValuePair<int, string>> e1 = data.GetEnumerator())
            using(IEnumerator<KeyValuePair<int, string>> e2 = copy.GetEnumerator())
            {
                while(e1.MoveNext() && e2.MoveNext())
                {
                    Assert.AreEqual(e1.Current.Key, e2.Current.Key);
                    Assert.AreEqual(e1.Current.Value, e2.Current.Value);
                }
                Assert.IsFalse(e1.MoveNext() || e2.MoveNext());
            }
        }

        [Test]
        public void TestIndexer()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            BTreeDictionary<int, string> copy = new BTreeDictionary<int, string>();
            copy.AddRange(data);
            Assert.AreEqual(copy.Count, data.Count);
            foreach (int key in data.Keys)
                Assert.AreEqual(data[key], copy[key]);
        }

        [Test]
        public void TestReadOnly()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, GetSample());
            Assert.IsFalse(data.IsReadOnly);
            
            BTreeDictionary<int, string> copy = data.MakeReadOnly();
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
            List<KeyValuePair<int, string>> sample = new List<KeyValuePair<int,string>>(GetSample());
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer, sample);

            sample.Sort((a, b) => data.Comparer.Compare(a.Key, b.Key));
            KeyValuePair<int, string>[] array = data.ToArray();

            Assert.AreEqual(sample.Count, array.Length);
            for( int i=0; i < sample.Count; i++)
            {
                Assert.AreEqual(sample[i].Key, array[i].Key);
                Assert.AreEqual(sample[i].Value, array[i].Value);
            }
        }

        [Test]
        public void TestKeyValueCollections()
        {
            List<KeyValuePair<int, string>> sample = new List<KeyValuePair<int,string>>(GetSample());
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(sample);
            //Key collection
            Assert.AreEqual(data.Count, data.Keys.Count);
            Assert.IsTrue(data.Keys.IsReadOnly);
            for (int i = 0; i < sample.Count && i < 5; i++)
                Assert.IsTrue(data.Keys.Contains(sample[i].Key));

            IEnumerator<int> ek = data.Keys.GetEnumerator();
            Assert.IsTrue(ek.MoveNext());
            int firstkey = ek.Current;
            Assert.IsTrue(ek.MoveNext());
            Assert.AreNotEqual(firstkey, ek.Current);
            ek.Reset();
            Assert.IsTrue(ek.MoveNext());
            Assert.AreEqual(firstkey, ek.Current);
            Assert.AreEqual(firstkey, ((System.Collections.IEnumerator)ek).Current);

            //Value collection
            Assert.AreEqual(data.Count, data.Values.Count);
            Assert.IsTrue(data.Values.IsReadOnly);
            for (int i = 0; i < sample.Count && i < 5; i++)
                Assert.IsTrue(data.Values.Contains(sample[i].Value));

            IEnumerator<string> ev = data.Values.GetEnumerator();
            Assert.IsTrue(ev.MoveNext());
            string firstvalue = ev.Current;
            Assert.IsTrue(ev.MoveNext());
            Assert.AreNotEqual(firstvalue, ev.Current);
            ev.Reset();
            Assert.IsTrue(ev.MoveNext());
            Assert.AreEqual(firstvalue, ((System.Collections.IEnumerator)ev).Current);
        }

        [Test]
        public void TestEnumerateFrom()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>(Comparer);
            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryAdd(i, i.ToString()));

            Assert.AreEqual(50, new List<KeyValuePair<int, string>>(data.EnumerateFrom(50)).Count);
            Assert.AreEqual(25, new List<KeyValuePair<int, string>>(data.EnumerateFrom(75)).Count);

            for (int i = 0; i < 100; i++)
            {
                int first = -1;
                foreach (KeyValuePair<int, string> kv in data.EnumerateFrom(i))
                {
                    first = kv.Key;
                    break;
                }
                Assert.AreEqual(i, first);
            }
        }

        [Test]
        public void TestFirstAndLast()
        {
            BTreeDictionary<int, string> data = new BTreeDictionary<int, string>();
            data.Add(1, "a");
            data.Add(2, "b");
            data.Add(3, "c");
            data.Add(4, "d");
            data.Add(5, "e");

            Assert.AreEqual(1, data.First().Key);
            Assert.AreEqual("a", data.First().Value);
            data.Remove(1);
            Assert.AreEqual(2, data.First().Key);
            Assert.AreEqual("b", data.First().Value);

            Assert.AreEqual(5, data.Last().Key);
            Assert.AreEqual("e", data.Last().Value);
            data.Remove(5);
            Assert.AreEqual(4, data.Last().Key);
            Assert.AreEqual("d", data.Last().Value);

            data.Remove(4);
            data.Remove(3);

            KeyValuePair<int, string> kv;
            Assert.IsTrue(data.TryGetLast(out kv));
            Assert.IsTrue(data.TryGetFirst(out kv));
            data.Remove(2);
            Assert.IsFalse(data.TryGetLast(out kv));
            Assert.IsFalse(data.TryGetFirst(out kv));

            try
            {
                data.First();
                Assert.Fail("Should raise InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
            try
            {
                data.Last();
                Assert.Fail("Should raise InvalidOperationException");
            }
            catch (InvalidOperationException)
            {
            }
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
            const string myTestValue1 = "T1", myTestValue2 = "t2";
            string test;

            BTreeDictionary<int, string> tree = new BTreeDictionary<int, string>(8, Comparer);
            FactoryMethod<BTreeDictionary<int, string>> verify = delegate() { tree.DebugAssert(); return tree; };
            Stopwatch time = new Stopwatch();
            time.Start();
            //large order-forward
            for (int i = start; i != stop; i += incr)
            {
                if (i == 988)
                    i = 988;
                if (!verify().TryAdd(i, myTestValue1)) throw new ApplicationException();
            }

            Trace.TraceInformation("{0} insert  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryGetValue(i, out test) || test != myTestValue1) throw new ApplicationException();

            Trace.TraceInformation("{0} seek    {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryUpdate(i, myTestValue2)) throw new ApplicationException();

            Trace.TraceInformation("{0} modify  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
                if (!verify().TryGetValue(i, out test) || test != myTestValue2) throw new ApplicationException();

            Trace.TraceInformation("{0} seek#2  {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            int tmpCount = 0;
            foreach (KeyValuePair<int, string> tmp in verify())
                if (tmp.Value != myTestValue2) throw new ApplicationException();
                else tmpCount++;
            if (tmpCount != count) throw new ApplicationException();

            Trace.TraceInformation("{0} foreach {1} in {2}", name, count, time.ElapsedMilliseconds);
            time.Reset();
            time.Start();

            for (int i = start; i != stop; i += incr)
            {
                if (i == 16)
                    i = 16;
                if (!verify().Remove(i)) throw new ApplicationException();
            }

            Trace.TraceInformation("{0} delete  {1} in {2}", name, count, time.ElapsedMilliseconds);

            for (int i = start; i != stop; i += incr)
                if (verify().TryGetValue(i, out test)) throw new ApplicationException();
        }
    }
}
