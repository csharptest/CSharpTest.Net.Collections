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

#define DEBUG

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using CSharpTest.Net.Collections;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class BasicTests
    {
        protected Random Random = new Random();

        protected virtual BPlusTreeOptions<int, string> Options
        {
            get
            {
                BPlusTree<int, string>.Options opts = new BPlusTree<int, string>.Options(new PrimitiveSerializer(), new PrimitiveSerializer());
                return opts;
            }
        }

        protected virtual BPlusTree<int, string> Create(BPlusTreeOptions<int, string> options)
        {
            return new BPlusTree<int, string>(options);
        }

        private static void Insert(BPlusTree<int, string> data, IEnumerable<KeyValuePair<int, string>> items, bool bulk,
            bool sorted)
        {
            if (bulk)
                if (sorted) data.AddRangeSorted(items);
                else data.AddRange(items);
            else
                foreach (KeyValuePair<int, string> kv in items)
                    data.Add(kv.Key, kv.Value);
        }

        private static IEnumerable<KeyValuePair<int, string>> CreateRandom(int count, int max)
        {
            Dictionary<int, int> unique = new Dictionary<int, int>();
            Random r = new Random();
            for (int i = 0; i < count; i++)
            {
                int key = r.Next() % max;
                while (unique.ContainsKey(key))
                    key = r.Next() % max;

                unique.Add(key, key);
                yield return new KeyValuePair<int, string>(key, key.ToString());
            }
        }

        private static IEnumerable<KeyValuePair<int, string>> CreateCount(int start, int increment, int count)
        {
            for (int i = 0; i < count; i++)
            {
                int value = start + i * increment;
                yield return new KeyValuePair<int, string>(value, value.ToString());
            }
        }


        private struct AddUpdateValue : ICreateOrUpdateValue<int, string>, IRemoveValue<int, string>
        {
            public string OldValue;
            public string Value;

            public bool CreateValue(int key, out string value)
            {
                OldValue = null;
                value = Value;
                return Value != null;
            }

            public bool UpdateValue(int key, ref string value)
            {
                OldValue = value;
                value = Value;
                return Value != null;
            }

            public bool RemoveValue(int key, string value)
            {
                OldValue = value;
                return value == Value;
            }
        }

        private void TestRandomAddRemove(int repeat, int nodesz, int size)
        {
            List<int> keysAdded = new List<int>(250000);
            BPlusTreeOptions<int, string> options = Options;
            options.LockingFactory = new IgnoreLockFactory();

            Dictionary<int, string> keys = new Dictionary<int, string>();

            for (; repeat > 0; repeat--)
            {
                keys.Clear();
                options.BTreeOrder = nodesz;
                using (BPlusTree<int, string> data = Create(options))
                {
                    data.EnableCount();

                    AddRandomKeys(size, keys, data);
                    IsSameList(keys, data);
                    keysAdded.Clear();

                    for (int tc = 0; tc < 1; tc++)
                    {
                        int del = keys.Count / 3 + Random.Next(keys.Count / 3);
                        RemoveRandomKeys(del, keys, data);
                        IsSameList(keys, data);

                        data.Validate();

                        AddRandomKeys(del, keys, data);
                        IsSameList(keys, data);

                        data.Validate();
                    }

                    keysAdded.Clear();

                    foreach (KeyValuePair<int, string> kv in data)
                        keysAdded.Add(kv.Key);

                    foreach (int k in keysAdded)
                    {
                        Assert.IsTrue(data.Remove(k));
                        data.Add(k, k.ToString());
                        Assert.IsTrue(data.Remove(k));
                        string test;
                        Assert.IsFalse(data.TryGetValue(k, out test));
                        Assert.IsNull(test);
                    }
                }
            }
        }

        private void RemoveRandomKeys(int count, Dictionary<int, string> keys, BPlusTree<int, string> data)
        {
            Stopwatch time = new Stopwatch();
            time.Start();

            int ix = 0;
            int[] del = new int[count];
            foreach (int k in keys.Keys)
            {
                del[ix++] = k;
                if (ix == del.Length) break;
            }

            foreach (int k in del)
                keys.Remove(k);
            if (data != null)
            {
                for (int i = 0; i < count; i++)
                    data.Remove(del[i]);
                data.Remove(del[0]);
            }
            Trace.TraceInformation("Removed {0} in {1}", count, time.ElapsedMilliseconds);
        }

        private void AddRandomKeys(int count, Dictionary<int, string> keys, BPlusTree<int, string> data)
        {
            Stopwatch time = new Stopwatch();
            time.Start();

            for (int i = 0; i < count; i++)
            {
                int key = Random.Next(int.MaxValue);
                if (data.TryAdd(key, key.ToString()))
                    keys.Add(key, key.ToString());
            }

            Trace.TraceInformation("Added {0} in {1}", count, time.ElapsedMilliseconds);
        }

        private void IsSameList(Dictionary<int, string> keys, BPlusTree<int, string> data)
        {
            Stopwatch time = new Stopwatch();
            time.Start();

            Assert.AreEqual(keys.Count, data.Count);

            int count = 0;
            string test;
            foreach (int key in keys.Keys)
            {
                count++;
                Assert.IsTrue(data.TryGetValue(key, out test));
                Assert.IsTrue(test == key.ToString());
            }

            Trace.TraceInformation("Seek {0} in {1}", count, time.ElapsedMilliseconds);
        }

        [Test]
        public void ExplicitRangeAddRemove()
        {
            string test;
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.Add(2, "v2");
                data.Add(1, "v1");

                int i = 0;
                for (; i < 8; i++)
                    data.TryAdd(i, "v" + i);
                for (i = 16; i >= 8; i--)
                    data.TryAdd(i, "v" + i);
                data.TryAdd(13, "v" + i);

                for (i = 0; i <= 16; i++)
                {
                    if (!data.TryGetValue(i, out test))
                        throw new Exception();
                    Assert.AreEqual("v" + i, test);
                }

                data.Remove(1);
                data.Remove(3);
                IEnumerator<KeyValuePair<int, string>> e = data.GetEnumerator();
                Assert.IsTrue(e.MoveNext());
                Assert.AreEqual(0, e.Current.Key);
                data.Add(1, "v1");
                Assert.IsTrue(e.MoveNext());
                data.Add(3, "v3");
                Assert.IsTrue(e.MoveNext());
                data.Remove(8);
                Assert.IsTrue(e.MoveNext());
                e.Dispose();
                data.Add(8, "v8");

                i = 0;
                foreach (KeyValuePair<int, string> pair in data)
                    Assert.AreEqual(pair.Key, i++);

                for (i = 0; i <= 16; i++)
                    Assert.IsTrue(data.Remove(i) && data.TryAdd(i, "v" + i));

                for (i = 6; i <= 12; i++)
                    Assert.IsTrue(data.Remove(i));

                for (i = 6; i <= 12; i++)
                {
                    Assert.IsFalse(data.TryGetValue(i, out test));
                    Assert.IsNull(test);
                }

                for (i = 0; i <= 5; i++)
                {
                    Assert.IsTrue(data.TryGetValue(i, out test));
                    Assert.AreEqual("v" + i, test);
                }

                for (i = 13; i <= 16; i++)
                {
                    Assert.IsTrue(data.TryGetValue(i, out test));
                    Assert.AreEqual("v" + i, test);
                }

                for (i = 0; i <= 16; i++)
                    Assert.AreEqual(i < 6 || i > 12, data.Remove(i));
            }
        }

        [Test]
        public void RandomSequenceTest()
        {
            int iterations = 5;
            int limit = 255;

            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();

                List<int> numbers = new List<int>();
                while (iterations-- > 0)
                {
                    data.Clear();
                    numbers.Clear();
                    data.DebugSetValidateOnCheckpoint(true);

                    for (int i = 0; i < limit; i++)
                    {
                        int id = Random.Next(limit);
                        if (!numbers.Contains(id))
                        {
                            numbers.Add(id);
                            data.Add(id, "V" + id);
                        }
                    }

                    Assert.AreEqual(numbers.Count, data.Count);

                    foreach (int number in numbers)
                        Assert.IsTrue(data.Remove(number));

                    Assert.AreEqual(0, data.Count);
                }
            }
        }

        [Test]
        public void TestAtomicAdd()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();
                int[] counter = {-1};
                for (int i = 0; i < 100; i++)
                    Assert.IsTrue(data.TryAdd(i, k => (++counter[0]).ToString()));
                Assert.AreEqual(100, data.Count);
                Assert.AreEqual(100, counter[0] + 1);

                //Inserts of existing keys will not call method
                Assert.IsFalse(data.TryAdd(50, k => { throw new InvalidOperationException(); }));
                Assert.AreEqual(100, data.Count);
            }
        }

        [Test]
        public void TestAtomicAddOrUpdate()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();
                int[] counter = {-1};

                for (int i = 0; i < 100; i++)
                    data.AddOrUpdate(i, k => (++counter[0]).ToString(),
                        (k, v) => { throw new InvalidOperationException(); });

                for (int i = 0; i < 100; i++)
                    Assert.AreEqual((i & 1) == 1, data.TryRemove(i, (k, v) => (int.Parse(v) & 1) == 1));

                for (int i = 0; i < 100; i++)
                    data.AddOrUpdate(i, k => (++counter[0]).ToString(), (k, v) => (++counter[0]).ToString());

                Assert.AreEqual(100, data.Count);
                Assert.AreEqual(200, counter[0] + 1);

                for (int i = 0; i < 100; i++)
                    Assert.IsTrue(data.TryRemove(i, (k, v) => int.Parse(v) - 100 == i));

                Assert.AreEqual(0, data.Count);
            }
        }

        [Test]
        public void TestAtomicInterfaces()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();
                data[1] = "a";

                AddUpdateValue update = new AddUpdateValue();
                Assert.IsFalse(data.AddOrUpdate(1, ref update));
                Assert.AreEqual("a", update.OldValue);
                Assert.IsFalse(data.AddOrUpdate(2, ref update));
                Assert.IsNull(update.OldValue);
                Assert.IsFalse(data.TryRemove(1, ref update));
                Assert.AreEqual("a", update.OldValue);

                Assert.AreEqual(1, data.Count);
                Assert.AreEqual("a", data[1]);

                update.Value = "b";
                Assert.IsTrue(data.AddOrUpdate(1, ref update));
                Assert.AreEqual("a", update.OldValue);
                Assert.IsTrue(data.AddOrUpdate(2, ref update));
                Assert.IsNull(update.OldValue);

                Assert.AreEqual(2, data.Count);
                Assert.AreEqual("b", data[1]);
                Assert.AreEqual("b", data[2]);

                Assert.IsTrue(data.TryRemove(1, ref update));
                Assert.AreEqual("b", update.OldValue);
                Assert.IsTrue(data.TryRemove(2, ref update));
                Assert.AreEqual("b", update.OldValue);
                Assert.AreEqual(0, data.Count);
            }
        }

        [Test]
        public void TestBulkInsert()
        {
            Stopwatch sw = Stopwatch.StartNew();
            BPlusTreeOptions<int, string> options = Options.Clone();
            using (TempFile temp = new TempFile())
            {
                //using (BPlusTree<int, string> data = Create(Options))
                using (BPlusTree<int, string> data = Create(options))
                {
                    const bool bulk = true;
                    Insert(data, CreateRandom(1000, 3000), bulk, false);

                    data.EnableCount();
                    Assert.AreEqual(1000, data.Count);

                    Insert(data, CreateCount(data.Last().Key + 1, 1, 1000), bulk, true);
                    Assert.AreEqual(2000, data.Count);

                    Insert(data, CreateCount(data.Last().Key + 10001, -1, 1000), bulk, false);
                    Assert.AreEqual(3000, data.Count);

                    int lastKey = data.Last().Key;
                    data.AddRange(CreateCount(1, 2, lastKey / 2), true);
                }
                temp.Dispose();
            }
            Trace.WriteLine("Inserted in " + sw.Elapsed);
        }

        [Test]
        public void TestConditionalRemove()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();
                for (int i = 0; i < 100; i++)
                    data.Add(i, i.ToString());
                for (int i = 0; i < 100; i++)
                    Assert.AreEqual((i & 1) == 1, data.TryRemove(i, (k, v) => (int.Parse(v) & 1) == 1));
                Assert.AreEqual(50, data.Count);
                for (int i = 0; i < 100; i++)
                    Assert.AreEqual(i % 10 == 0, data.TryRemove(i, (k, v) => int.Parse(v) % 10 == 0));
                Assert.AreEqual(40, data.Count);
            }
        }

        [Test]
        public void TestCounts()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                Assert.AreEqual(int.MinValue, data.Count);
                data.EnableCount();

                Assert.AreEqual(0, data.Count);
                Assert.IsTrue(data.TryAdd(1, "test"));
                Assert.AreEqual(1, data.Count);
                Assert.IsTrue(data.TryAdd(2, "test"));
                Assert.AreEqual(2, data.Count);

                Assert.IsFalse(data.TryAdd(2, "test"));
                Assert.AreEqual(2, data.Count);
                Assert.IsTrue(data.Remove(1));
                Assert.AreEqual(1, data.Count);
                Assert.IsTrue(data.Remove(2));
                Assert.AreEqual(0, data.Count);
            }
        }

        [Test]
        public void TestEnumerateFrom()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                for (int i = 0; i < 100; i++)
                    data.Add(i, i.ToString());

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
        }

        [Test]
        public void TestEnumeration()
        {
            BPlusTreeOptions<int, string> options = Options;
            options.BTreeOrder = 4;
            using (BPlusTree<int, string> data = new BPlusTree<int, string>(options))
            {
                data.EnableCount();

                data.DebugSetOutput(new StringWriter());
                data.DebugSetValidateOnCheckpoint(true);

                for (int id = 0; id < 10; id++)
                    data.Add(id, id.ToString());

                using (IEnumerator<KeyValuePair<int, string>> enu = data.GetEnumerator())
                {
                    Assert.IsTrue(enu.MoveNext());
                    Assert.AreEqual(0, enu.Current.Key);

                    for (int id = 2; id < 10; id++)
                        Assert.IsTrue(data.Remove(id));
                    for (int id = 6; id < 11; id++)
                        data.Add(id, id.ToString());

                    Assert.IsTrue(enu.MoveNext());
                    Assert.AreEqual(1, enu.Current.Key);
                    Assert.IsTrue(enu.MoveNext());
                    Assert.AreEqual(6, enu.Current.Key);
                    Assert.IsTrue(enu.MoveNext());
                    Assert.AreEqual(7, enu.Current.Key);
                    Assert.IsTrue(data.Remove(8));
                    Assert.IsTrue(data.Remove(9));
                    Assert.IsTrue(data.Remove(10));
                    data.Add(11, 11.ToString());
                    Assert.IsTrue(enu.MoveNext());
                    Assert.AreEqual(11, enu.Current.Key);
                    Assert.IsTrue(false == enu.MoveNext());
                }
                data.Clear();
            }
        }

        [Test]
        public void TestFirstAndLast()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
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
        }

        [Test]
        public void TestGetOrAdd()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                Assert.AreEqual("a", data.GetOrAdd(1, "a"));
                Assert.AreEqual("a", data.GetOrAdd(1, "b"));

                Assert.AreEqual("b", data.GetOrAdd(2, k => "b"));
                Assert.AreEqual("b", data.GetOrAdd(2, k => "c"));
            }
        }

        [Test]
        public void TestInserts()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                data.EnableCount();

                int[][] TestArrays =
                {
                    new[] {10, 18, 81, 121, 76, 31, 250, 174, 24, 38, 246, 79},
                    new[] {110, 191, 84, 218, 170, 217, 199, 232, 184, 254, 32, 90, 241, 136, 181, 28, 226, 69, 52}
                };

                foreach (int[] arry in TestArrays)
                {
                    data.Clear();
                    Assert.AreEqual(0, data.Count);

                    int count = 0;
                    foreach (int id in arry)
                    {
                        data.Add(id, id.ToString());
                        Assert.AreEqual(++count, data.Count);
                    }

                    Assert.AreEqual(arry.Length, data.Count);
                    data.UnloadCache();

                    foreach (int id in arry)
                    {
                        Assert.AreEqual(id.ToString(), data[id]);
                        data[id] = string.Empty;
                        Assert.AreEqual(string.Empty, data[id]);

                        Assert.IsTrue(data.Remove(id));
                        Assert.AreEqual(--count, data.Count);
                    }

                    Assert.AreEqual(0, data.Count);
                }
            }
        }

        [Test]
        public void TestKeyValueCollections()
        {
            List<KeyValuePair<int, string>> sample = new List<KeyValuePair<int, string>>();
            for (int i = 0; i < 20; i++)
                sample.Add(new KeyValuePair<int, string>(i, i.ToString()));

            using (BPlusTree<int, string> data = Create(Options))
            {
                data.AddRange(sample);
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
                Assert.AreEqual(firstkey, ((IEnumerator) ek).Current);

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
                Assert.AreEqual(firstvalue, ((IEnumerator) ev).Current);
            }
        }

        [Test]
        public void TestNewAddOrUpdate()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                Assert.AreEqual("a", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));
                Assert.AreEqual("1", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));

                Assert.AreEqual("b", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
                Assert.AreEqual("2", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
            }
        }

        [Test]
        public void TestRandomAddRemoveOrder16()
        {
            TestRandomAddRemove(1, 16, 1000);
        }

        [Test]
        public void TestRandomAddRemoveOrder4()
        {
            TestRandomAddRemove(1, 4, 1000);
        }

        [Test]
        public void TestRandomAddRemoveOrder64()
        {
            TestRandomAddRemove(1, 64, 1000);
        }

        [Test]
        public void TestRangeEnumerate()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
                for (int i = 0; i < 100; i++)
                    data.Add(i, i.ToString());

                int ix = 0;
                foreach (KeyValuePair<int, string> kv in data.EnumerateRange(-500, 5000))
                    Assert.AreEqual(ix++, kv.Key);
                Assert.AreEqual(100, ix);

                foreach (KeyValuePair<int, int> range in new Dictionary<int, int> {{6, 25}, {7, 25}, {8, 25}, {9, 25}, {22, 25}, {28, 28}})
                {
                    ix = range.Key;
                    foreach (KeyValuePair<int, string> kv in data.EnumerateRange(ix, range.Value))
                        Assert.AreEqual(ix++, kv.Key);
                    Assert.AreEqual(range.Value, ix - 1);
                }
            }
        }

        [Test]
        public void TestTryRoutines()
        {
            using (BPlusTree<int, string> data = Create(Options))
            {
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
        }
    }
}