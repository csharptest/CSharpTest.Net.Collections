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
using CSharpTest.Net.Collections;
using NUnit.Framework;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestLurchTable : TestGenericCollection<TestLurchTable.LurchTableTest<int, string>, KeyValuePair<int, string>>
    {
        public class LurchTableTest<TKey, TValue> : LurchTable<TKey, TValue>
        {
            public LurchTableTest() : base(1024, LurchTableOrder.Access)
            { }
            public LurchTableTest(LurchTableOrder order) : base(1024, order)
            { }
            public LurchTableTest(IEqualityComparer<TKey> comparer) : base(1024, LurchTableOrder.Access, comparer)
            { }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            var results = new List<KeyValuePair<int, string>>();
            Random r = new Random();
            for (int i = 1; i < 100; i += r.Next(1, 3))
                results.Add(new KeyValuePair<int, string>(i, i.ToString()));
            return results.ToArray();
        }

        class IntComparer : IEqualityComparer<int>
        {
            bool IEqualityComparer<int>.Equals(int x, int y)
            {
                return false;
            }

            int IEqualityComparer<int>.GetHashCode(int obj)
            {
                return 0;
            }
        }
        [Test]
        public void TestCTors()
        {
            var cmp = new IntComparer();
            const int limit = 5;

            Assert.AreEqual(LurchTableOrder.None, new LurchTable<int, int>(1).Ordering);
            Assert.AreEqual(LurchTableOrder.Insertion, new LurchTable<int, int>(1, LurchTableOrder.Insertion).Ordering);
            Assert.IsTrue(ReferenceEquals(cmp, new LurchTable<int, int>(1, LurchTableOrder.Insertion, cmp).Comparer));
            Assert.AreEqual(LurchTableOrder.Modified, new LurchTable<int, int>(LurchTableOrder.Modified, limit).Ordering);
            Assert.AreEqual(limit, new LurchTable<int, int>(LurchTableOrder.Modified, limit).Limit);
            Assert.AreEqual(LurchTableOrder.Access, new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Ordering);
            Assert.AreEqual(limit, new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Limit);
            Assert.IsTrue(ReferenceEquals(cmp, new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Comparer));
            Assert.AreEqual(LurchTableOrder.Access, new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Ordering);
            Assert.AreEqual(limit, new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Limit);
            Assert.IsTrue(ReferenceEquals(cmp, new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Comparer));
        }

        [Test]
        public void TestDequeueByInsertion()
        {
            var test = new LurchTableTest<int, string>(LurchTableOrder.Insertion);
            Assert.AreEqual(LurchTableOrder.Insertion, test.Ordering);
            var sample = GetSample();
            Array.Reverse(sample);
            foreach (var item in sample)
                test.Add(item.Key, item.Value);
            
            KeyValuePair<int, string> value;
            foreach (var item in sample)
            {
                Assert.IsTrue(test.Peek(out value));
                Assert.AreEqual(item.Key, value.Key);
                Assert.AreEqual(item.Value, value.Value);
                value = test.Dequeue();
                Assert.AreEqual(item.Key, value.Key);
                Assert.AreEqual(item.Value, value.Value);
            }

            Assert.IsFalse(test.Peek(out value));
            Assert.IsFalse(test.TryDequeue(out value));
        }

        [Test]
        public void TestDequeueByModified()
        {
            var test = new LurchTableTest<int, string>(LurchTableOrder.Modified);
            Assert.AreEqual(LurchTableOrder.Modified, test.Ordering);
            var sample = GetSample();
            foreach (var item in sample)
                test.Add(item.Key, item.Value);

            Array.Reverse(sample);
            for (int i = 0; i < sample.Length; i++)
            {
                var item = new KeyValuePair<int, string>(sample[i].Key, sample[i].Value + "-2");
                test[item.Key] = item.Value;
                sample[i] = item;
            }

            KeyValuePair<int, string> value;
            foreach (var item in sample)
            {
                Assert.IsTrue(test.Peek(out value));
                Assert.AreEqual(item.Key, value.Key);
                Assert.AreEqual(item.Value, value.Value);
                value = test.Dequeue();
                Assert.AreEqual(item.Key, value.Key);
                Assert.AreEqual(item.Value, value.Value);
            }

            Assert.IsFalse(test.Peek(out value));
            Assert.IsFalse(test.TryDequeue(out value));
        }

        [Test]
        public void TestDequeueByAccess()
        {
            var test = new LurchTableTest<int, string>(LurchTableOrder.Access);
            Assert.AreEqual(LurchTableOrder.Access, test.Ordering);
            var sample = GetSample();
            foreach (var item in sample)
                test.Add(item.Key, item.Value);

            Array.Reverse(sample);
            foreach (var item in sample)
                Assert.AreEqual(item.Value, test[item.Key]);

            KeyValuePair<int, string> value;
            foreach (var item in sample)
            {
                Assert.IsTrue(test.TryDequeue(out value));
                Assert.AreEqual(item.Key, value.Key);
                Assert.AreEqual(item.Value, value.Value);
            }

            Assert.IsFalse(test.Peek(out value));
            Assert.IsFalse(test.TryDequeue(out value));
        }

        [Test]
        public void TestKeysEnumerator()
        {
            var sample = GetSample();
            var test = CreateSample(sample);
            int ix = 0;
            foreach (var key in test.Keys)
                Assert.AreEqual(sample[ix++].Value, test[key]);
        }

        [Test]
        public void TestValuesEnumerator()
        {
            var sample = GetSample();
            var test = CreateSample(sample);
            int ix = 0;
            foreach (var value in test.Values)
                Assert.AreEqual(sample[ix++].Value, value);
        }

        [Test]
        public void TestLimitorByAccess()
        {
            //multiple of prime will produce hash collision, thus testing removal of non-first elements
            const int prime = 1103;
            var test = new LurchTable<int, string>(LurchTableOrder.Access, 3, prime, 10, 10, EqualityComparer<int>.Default);
            test[1 * prime] = "a";
            test[2 * prime] = "b";
            test[3 * prime] = "c";
            Assert.AreEqual(3, test.Count);
            Assert.AreEqual("b", test[2 * prime]); //access moves to front..
            test[4] = "d";
            test[5] = "e";
            Assert.AreEqual(3, test.Count); // still 3 items
            Assert.IsFalse(test.ContainsKey(1 * prime));
            Assert.IsTrue(test.ContainsKey(2 * prime)); //recently access is still there
            Assert.IsFalse(test.ContainsKey(3 * prime));
        }

        class RecordEvents<TKey, TValue>
        {
            public KeyValuePair<TKey, TValue> LastAdded, LastUpdate, LastRemove;

            public void ItemAdded(KeyValuePair<TKey, TValue> obj) { LastAdded = obj; }
            public void ItemUpdated(KeyValuePair<TKey, TValue> original, KeyValuePair<TKey, TValue> obj) { LastUpdate = obj; }
            public void ItemRemoved(KeyValuePair<TKey, TValue> obj) { LastRemove = obj; }
        }

        [Test]
        public void TestCrudEvents()
        {
            var recorder = new RecordEvents<int, string>();
            var test = new LurchTable<int, string>(LurchTableOrder.Access, 3, 1103, 10, 10, EqualityComparer<int>.Default);
            test.ItemAdded += recorder.ItemAdded;
            test.ItemUpdated += recorder.ItemUpdated;
            test.ItemRemoved += recorder.ItemRemoved;
            test[1] = "a";
            Assert.AreEqual("a", recorder.LastAdded.Value);
            test[2] = "b";
            Assert.AreEqual("b", recorder.LastAdded.Value);
            test[3] = "c";
            Assert.AreEqual("c", recorder.LastAdded.Value);
            Assert.AreEqual(3, test.Count);
            Assert.AreEqual("b", test[2]); //access moves to front..
            test[4] = "d";
            Assert.AreEqual("d", recorder.LastAdded.Value);
            Assert.AreEqual("a", recorder.LastRemove.Value);
            test[5] = "e";
            Assert.AreEqual("e", recorder.LastAdded.Value);
            Assert.AreEqual("c", recorder.LastRemove.Value);
            test[2] = "B";
            Assert.AreEqual("B", recorder.LastUpdate.Value);
            test[6] = "f";
            Assert.AreEqual("f", recorder.LastAdded.Value);
            Assert.AreEqual("d", recorder.LastRemove.Value);

            Assert.AreEqual(3, test.Count); // still 3 items
            string value;
            Assert.IsTrue(test.TryRemove(5, out value));
            Assert.AreEqual("e", value);
            Assert.AreEqual("e", recorder.LastRemove.Value);

            Assert.AreEqual("B", test.Dequeue().Value);
            Assert.AreEqual("f", test.Dequeue().Value);
            Assert.AreEqual(0, test.Count);
        }

        [Test]
        public void TestCollisionRemoval()
        {
            //multiple of prime will produce hash collision, thus testing removal of non-first elements
            const int prime = 1103;
            var test = new LurchTable<int, string>(LurchTableOrder.Access, 10, prime, 10, 10, EqualityComparer<int>.Default);
            test[1 * prime] = "a";
            test[2 * prime] = "b";
            test[3 * prime] = "c";
            test[4 * prime] = "d";
            test[5 * prime] = "e";
            Assert.IsTrue(test.Remove(4 * prime));
            Assert.IsTrue(test.Remove(2 * prime));
            Assert.IsTrue(test.Remove(5 * prime));
            Assert.IsTrue(test.Remove(1 * prime));
            Assert.IsTrue(test.Remove(3 * prime));
            Assert.AreEqual(0, test.Count);
        }

        [Test]
        public void TestAddRemoveByKey()
        {
            LurchTableTest<int, string> test = new LurchTableTest<int, string>();
            for (int i = 0; i < 10; i++)
                test.Add(i, i.ToString());
            
            for (int i = 0; i < 10; i++)
                Assert.IsTrue(test.ContainsKey(i));

            string cmp;
            for (int i = 0; i < 10; i++)
                Assert.IsTrue(test.TryGetValue(i, out cmp) && cmp == i.ToString());

            for (int i = 0; i < 10; i++)
                Assert.IsTrue(test.Remove(i));
        }

        [Test]
        public void TestComparer()
        {
            var test = new LurchTableTest<string, string>(StringComparer.OrdinalIgnoreCase);
            test["a"] = "b";
            Assert.IsTrue(test.ContainsKey("A"));

            test = new LurchTableTest<string, string>(StringComparer.OrdinalIgnoreCase);
            test["a"] = "b";
            Assert.IsTrue(test.ContainsKey("A"));
        }

        [Test]
        public void TestKeys()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>();
            test["a"] = "b";
            string all = String.Join("", new List<string>(test.Keys).ToArray());
            Assert.AreEqual("a", all);
        }

        [Test]
        public void TestValues()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>();
            test["a"] = "b";
            string all = String.Join("", new List<string>(test.Values).ToArray());
            Assert.AreEqual("b", all);
        }

        [Test]
        public void TestAtomicAdd()
        {
            var data = new LurchTableTest<int, string>();
            int[] counter = new int[] {-1};
            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryAdd(i, (k) => (++counter[0]).ToString()));
            Assert.AreEqual(100, data.Count);
            Assert.AreEqual(100, counter[0] + 1);

            //Inserts of existing keys will not call method
            Assert.IsFalse(data.TryAdd(50, (k) => { throw new InvalidOperationException(); }));
            Assert.AreEqual(100, data.Count);
        }

        [Test]
        public void TestAtomicAddOrUpdate()
        {
            var data = new LurchTableTest<int, string>();
            int[] counter = new int[] {-1};

            for (int i = 0; i < 100; i++)
                data.AddOrUpdate(i, (k) => (++counter[0]).ToString(), (k, v) => { throw new InvalidOperationException(); });

            for (int i = 0; i < 100; i++)
                Assert.AreEqual((i & 1) == 1, data.TryRemove(i, (k, v) => (int.Parse(v) & 1) == 1));

            for (int i = 0; i < 100; i++)
                data.AddOrUpdate(i, (k) => (++counter[0]).ToString(), (k, v) => (++counter[0]).ToString());

            Assert.AreEqual(100, data.Count);
            Assert.AreEqual(200, counter[0] + 1);

            for (int i = 0; i < 100; i++)
                Assert.IsTrue(data.TryRemove(i, (k, v) => int.Parse(v) - 100 == i));

            Assert.AreEqual(0, data.Count);
        }

        [Test]
        public void TestNewAddOrUpdate()
        {
            var data = new LurchTableTest<int, string>();
            Assert.AreEqual("a", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));
            Assert.AreEqual("1", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));

            Assert.AreEqual("b", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
            Assert.AreEqual("2", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
        }

        struct AddUpdateValue : ICreateOrUpdateValue<int, string>, IRemoveValue<int, string>
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

        [Test]
        public void TestAtomicInterfaces()
        {
            var data = new LurchTableTest<int, string>();

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

        [Test]
        public void TestGetOrAdd()
        {
            var data = new LurchTableTest<int, string>();
            Assert.AreEqual("a", data.GetOrAdd(1, "a"));
            Assert.AreEqual("a", data.GetOrAdd(1, "b"));

            Assert.AreEqual("b", data.GetOrAdd(2, k => "b"));
            Assert.AreEqual("b", data.GetOrAdd(2, k => "c"));
        }


        [Test]
        public void TestTryRoutines()
        {
            var data = new LurchTableTest<int, string>();

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

            Assert.IsFalse(data.TryUpdate(1, (k, x) => x.ToUpper()));
            data[1] = "a";
            data[1] = "b";
            Assert.IsTrue(data.TryUpdate(1, (k, x) => x.ToUpper()));
            Assert.AreEqual("B", data[1]);
        }

        [Test]
        public void TestInitialize()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>(StringComparer.Ordinal);
            test["a"] = "b";
            Assert.AreEqual(1, test.Count);
            test.Initialize();
            Assert.AreEqual(0, test.Count);
        }

        [Test]
        public void TestSampleCollection()
        {
            var sample = GetSample();
            var items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            VerifyCollection(new KeyValueEquality<int, string>(), new List<KeyValuePair<int, string>>(sample), items);
            VerifyCollection(new KeyValueEquality<int, string>(), new List<KeyValuePair<int, string>>(sample), dict);
        }

        [Test]
        public void TestSampleKeyCollection()
        {
            var sample = GetSample();
            var items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            var keys = new List<int>();
            foreach (var kv in sample)
                keys.Add(kv.Key);
            VerifyCollection(EqualityComparer<int>.Default, keys.AsReadOnly(), items.Keys);
            VerifyCollection(EqualityComparer<int>.Default, keys.AsReadOnly(), dict.Keys);
        }

        [Test]
        public void TestSampleValueCollection()
        {
            var sample = GetSample();
            var items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            var values = new List<string>();
            foreach (var kv in sample)
                values.Add(kv.Value);
            VerifyCollection(EqualityComparer<string>.Default, values.AsReadOnly(), items.Values);
            VerifyCollection(EqualityComparer<string>.Default, values.AsReadOnly(), dict.Values);
        }

        [Test, ExpectedException(typeof(ObjectDisposedException))]
        public void TestDisposed()
        {
            IConcurrentDictionary<int, string> test = new LurchTableTest<int, string>();
            test.Dispose();
            test.Add(1, "");
        }

        class KeyValueEquality<TKey, TValue> : IEqualityComparer<KeyValuePair<TKey, TValue>>
        {
            IEqualityComparer<TKey> KeyComparer = EqualityComparer<TKey>.Default;
            IEqualityComparer<TValue> ValueComparer = EqualityComparer<TValue>.Default;
            public bool Equals(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
            {
                return KeyComparer.Equals(x.Key, y.Key) && ValueComparer.Equals(x.Value, y.Value);
            }
            public int GetHashCode(KeyValuePair<TKey, TValue> obj)
            {
                return KeyComparer.GetHashCode(obj.Key) ^ ValueComparer.GetHashCode(obj.Value);
            }
        }
    }

    [TestFixture]
    public class TestLurchTableDictionary : CSharpTest.Net.BPlusTree.Test.TestDictionary<LurchTable<Guid, String>, TestLurchTableDictionary.Factory, Guid, String>
    {
        private const int SAMPLE_SIZE = 1000;
        public new class Factory : IFactory<LurchTable<Guid, String>>
        {
            public LurchTable<Guid, string> Create()
            {
                return new LurchTable<Guid, string>(SAMPLE_SIZE, LurchTableOrder.Access);
            }
        }

        protected override KeyValuePair<Guid, string>[] GetSample()
        {
            var results = new List<KeyValuePair<Guid, string>>();
            for (int i = 0; i < SAMPLE_SIZE; i++)
            {
                Guid id = Guid.NewGuid();
                results.Add(new KeyValuePair<Guid, string>(id, id.ToString()));
            }
            return results.ToArray();
        }
    }
}
