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
using CSharpTest.Net.Interfaces;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    
    public class TestLurchTable : TestGenericCollection<TestLurchTable.LurchTableTest<int, string>,
        KeyValuePair<int, string>>
    {
        public class LurchTableTest<TKey, TValue> : LurchTable<TKey, TValue>
        {
            public LurchTableTest() : base(1024, LurchTableOrder.Access)
            {
            }

            public LurchTableTest(LurchTableOrder order) : base(1024, order)
            {
            }

            public LurchTableTest(IEqualityComparer<TKey> comparer) : base(1024, LurchTableOrder.Access, comparer)
            {
            }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            List<KeyValuePair<int, string>> results = new List<KeyValuePair<int, string>>();
            Random r = new Random();
            for (int i = 1; i < 100; i += r.Next(1, 3))
                results.Add(new KeyValuePair<int, string>(i, i.ToString()));
            return results.ToArray();
        }

        private class IntComparer : IEqualityComparer<int>
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

        private class RecordEvents<TKey, TValue>
        {
            public KeyValuePair<TKey, TValue> LastAdded, LastUpdate, LastRemove;

            public void ItemAdded(KeyValuePair<TKey, TValue> obj)
            {
                LastAdded = obj;
            }

            public void ItemUpdated(KeyValuePair<TKey, TValue> original, KeyValuePair<TKey, TValue> obj)
            {
                LastUpdate = obj;
            }

            public void ItemRemoved(KeyValuePair<TKey, TValue> obj)
            {
                LastRemove = obj;
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

        private class KeyValueEquality<TKey, TValue> : IEqualityComparer<KeyValuePair<TKey, TValue>>
        {
            private readonly IEqualityComparer<TKey> KeyComparer = EqualityComparer<TKey>.Default;
            private readonly IEqualityComparer<TValue> ValueComparer = EqualityComparer<TValue>.Default;

            public bool Equals(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
            {
                return KeyComparer.Equals(x.Key, y.Key) && ValueComparer.Equals(x.Value, y.Value);
            }

            public int GetHashCode(KeyValuePair<TKey, TValue> obj)
            {
                return KeyComparer.GetHashCode(obj.Key) ^ ValueComparer.GetHashCode(obj.Value);
            }
        }

        [Fact]
        public void TestAddRemoveByKey()
        {
            LurchTableTest<int, string> test = new LurchTableTest<int, string>();
            for (int i = 0; i < 10; i++)
                test.Add(i, i.ToString());

            for (int i = 0; i < 10; i++)
                Assert.True(test.ContainsKey(i));

            string cmp;
            for (int i = 0; i < 10; i++)
                Assert.True(test.TryGetValue(i, out cmp) && cmp == i.ToString());

            for (int i = 0; i < 10; i++)
                Assert.True(test.Remove(i));
        }

        [Fact]
        public void TestAtomicAdd()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();
            int[] counter = {-1};
            for (int i = 0; i < 100; i++)
                Assert.True(data.TryAdd(i, k => (++counter[0]).ToString()));
            Assert.Equal(100, data.Count);
            Assert.Equal(100, counter[0] + 1);

            //Inserts of existing keys will not call method
            Assert.False(data.TryAdd(50, k => { throw new InvalidOperationException(); }));
            Assert.Equal(100, data.Count);
        }

        [Fact]
        public void TestAtomicAddOrUpdate()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();
            int[] counter = {-1};

            for (int i = 0; i < 100; i++)
                data.AddOrUpdate(i, k => (++counter[0]).ToString(),
                    (k, v) => { throw new InvalidOperationException(); });

            for (int i = 0; i < 100; i++)
                Assert.Equal((i & 1) == 1, data.TryRemove(i, (k, v) => (int.Parse(v) & 1) == 1));

            for (int i = 0; i < 100; i++)
                data.AddOrUpdate(i, k => (++counter[0]).ToString(), (k, v) => (++counter[0]).ToString());

            Assert.Equal(100, data.Count);
            Assert.Equal(200, counter[0] + 1);

            for (int i = 0; i < 100; i++)
                Assert.True(data.TryRemove(i, (k, v) => int.Parse(v) - 100 == i));

            Assert.Equal(0, data.Count);
        }

        [Fact]
        public void TestAtomicInterfaces()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();

            data[1] = "a";

            AddUpdateValue update = new AddUpdateValue();
            Assert.False(data.AddOrUpdate(1, ref update));
            Assert.Equal("a", update.OldValue);
            Assert.False(data.AddOrUpdate(2, ref update));
            Assert.Null(update.OldValue);
            Assert.False(data.TryRemove(1, ref update));
            Assert.Equal("a", update.OldValue);

            Assert.Equal(1, data.Count);
            Assert.Equal("a", data[1]);

            update.Value = "b";
            Assert.True(data.AddOrUpdate(1, ref update));
            Assert.Equal("a", update.OldValue);
            Assert.True(data.AddOrUpdate(2, ref update));
            Assert.Null(update.OldValue);

            Assert.Equal(2, data.Count);
            Assert.Equal("b", data[1]);
            Assert.Equal("b", data[2]);

            Assert.True(data.TryRemove(1, ref update));
            Assert.Equal("b", update.OldValue);
            Assert.True(data.TryRemove(2, ref update));
            Assert.Equal("b", update.OldValue);
            Assert.Equal(0, data.Count);
        }

        [Fact]
        public void TestCollisionRemoval()
        {
            //multiple of prime will produce hash collision, thus testing removal of non-first elements
            const int prime = 1103;
            LurchTable<int, string> test = new LurchTable<int, string>(LurchTableOrder.Access, 10, prime, 10, 10,
                EqualityComparer<int>.Default);
            test[1 * prime] = "a";
            test[2 * prime] = "b";
            test[3 * prime] = "c";
            test[4 * prime] = "d";
            test[5 * prime] = "e";
            Assert.True(test.Remove(4 * prime));
            Assert.True(test.Remove(2 * prime));
            Assert.True(test.Remove(5 * prime));
            Assert.True(test.Remove(1 * prime));
            Assert.True(test.Remove(3 * prime));
            Assert.Equal(0, test.Count);
        }

        [Fact]
        public void TestComparer()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>(StringComparer.OrdinalIgnoreCase);
            test["a"] = "b";
            Assert.True(test.ContainsKey("A"));

            test = new LurchTableTest<string, string>(StringComparer.OrdinalIgnoreCase);
            test["a"] = "b";
            Assert.True(test.ContainsKey("A"));
        }

        [Fact]
        public void TestCrudEvents()
        {
            RecordEvents<int, string> recorder = new RecordEvents<int, string>();
            LurchTable<int, string> test = new LurchTable<int, string>(LurchTableOrder.Access, 3, 1103, 10, 10,
                EqualityComparer<int>.Default);
            test.ItemAdded += recorder.ItemAdded;
            test.ItemUpdated += recorder.ItemUpdated;
            test.ItemRemoved += recorder.ItemRemoved;
            test[1] = "a";
            Assert.Equal("a", recorder.LastAdded.Value);
            test[2] = "b";
            Assert.Equal("b", recorder.LastAdded.Value);
            test[3] = "c";
            Assert.Equal("c", recorder.LastAdded.Value);
            Assert.Equal(3, test.Count);
            Assert.Equal("b", test[2]); //access moves to front..
            test[4] = "d";
            Assert.Equal("d", recorder.LastAdded.Value);
            Assert.Equal("a", recorder.LastRemove.Value);
            test[5] = "e";
            Assert.Equal("e", recorder.LastAdded.Value);
            Assert.Equal("c", recorder.LastRemove.Value);
            test[2] = "B";
            Assert.Equal("B", recorder.LastUpdate.Value);
            test[6] = "f";
            Assert.Equal("f", recorder.LastAdded.Value);
            Assert.Equal("d", recorder.LastRemove.Value);

            Assert.Equal(3, test.Count); // still 3 items
            string value;
            Assert.True(test.TryRemove(5, out value));
            Assert.Equal("e", value);
            Assert.Equal("e", recorder.LastRemove.Value);

            Assert.Equal("B", test.Dequeue().Value);
            Assert.Equal("f", test.Dequeue().Value);
            Assert.Equal(0, test.Count);
        }

        [Fact]
        public void TestCTors()
        {
            IntComparer cmp = new IntComparer();
            const int limit = 5;

            Assert.Equal(LurchTableOrder.None, new LurchTable<int, int>(1).Ordering);
            Assert.Equal(LurchTableOrder.Insertion, new LurchTable<int, int>(1, LurchTableOrder.Insertion).Ordering);
            Assert.True(ReferenceEquals(cmp, new LurchTable<int, int>(1, LurchTableOrder.Insertion, cmp).Comparer));
            Assert.Equal(LurchTableOrder.Modified,
                new LurchTable<int, int>(LurchTableOrder.Modified, limit).Ordering);
            Assert.Equal(limit, new LurchTable<int, int>(LurchTableOrder.Modified, limit).Limit);
            Assert.Equal(LurchTableOrder.Access,
                new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Ordering);
            Assert.Equal(limit, new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Limit);
            Assert.True(ReferenceEquals(cmp, new LurchTable<int, int>(LurchTableOrder.Access, limit, cmp).Comparer));
            Assert.Equal(LurchTableOrder.Access,
                new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Ordering);
            Assert.Equal(limit, new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Limit);
            Assert.True(ReferenceEquals(cmp,
                new LurchTable<int, int>(LurchTableOrder.Access, limit, 1, 1, 1, cmp).Comparer));
        }

        [Fact]
        public void TestDequeueByAccess()
        {
            LurchTableTest<int, string> test = new LurchTableTest<int, string>(LurchTableOrder.Access);
            Assert.Equal(LurchTableOrder.Access, test.Ordering);
            KeyValuePair<int, string>[] sample = GetSample();
            foreach (KeyValuePair<int, string> item in sample)
                test.Add(item.Key, item.Value);

            Array.Reverse(sample);
            foreach (KeyValuePair<int, string> item in sample)
                Assert.Equal(item.Value, test[item.Key]);

            KeyValuePair<int, string> value;
            foreach (KeyValuePair<int, string> item in sample)
            {
                Assert.True(test.TryDequeue(out value));
                Assert.Equal(item.Key, value.Key);
                Assert.Equal(item.Value, value.Value);
            }

            Assert.False(test.Peek(out value));
            Assert.False(test.TryDequeue(out value));
        }

        [Fact]
        public void TestDequeueByInsertion()
        {
            LurchTableTest<int, string> test = new LurchTableTest<int, string>(LurchTableOrder.Insertion);
            Assert.Equal(LurchTableOrder.Insertion, test.Ordering);
            KeyValuePair<int, string>[] sample = GetSample();
            Array.Reverse(sample);
            foreach (KeyValuePair<int, string> item in sample)
                test.Add(item.Key, item.Value);

            KeyValuePair<int, string> value;
            foreach (KeyValuePair<int, string> item in sample)
            {
                Assert.True(test.Peek(out value));
                Assert.Equal(item.Key, value.Key);
                Assert.Equal(item.Value, value.Value);
                value = test.Dequeue();
                Assert.Equal(item.Key, value.Key);
                Assert.Equal(item.Value, value.Value);
            }

            Assert.False(test.Peek(out value));
            Assert.False(test.TryDequeue(out value));
        }

        [Fact]
        public void TestDequeueByModified()
        {
            LurchTableTest<int, string> test = new LurchTableTest<int, string>(LurchTableOrder.Modified);
            Assert.Equal(LurchTableOrder.Modified, test.Ordering);
            KeyValuePair<int, string>[] sample = GetSample();
            foreach (KeyValuePair<int, string> item in sample)
                test.Add(item.Key, item.Value);

            Array.Reverse(sample);
            for (int i = 0; i < sample.Length; i++)
            {
                KeyValuePair<int, string> item = new KeyValuePair<int, string>(sample[i].Key, sample[i].Value + "-2");
                test[item.Key] = item.Value;
                sample[i] = item;
            }

            KeyValuePair<int, string> value;
            foreach (KeyValuePair<int, string> item in sample)
            {
                Assert.True(test.Peek(out value));
                Assert.Equal(item.Key, value.Key);
                Assert.Equal(item.Value, value.Value);
                value = test.Dequeue();
                Assert.Equal(item.Key, value.Key);
                Assert.Equal(item.Value, value.Value);
            }

            Assert.False(test.Peek(out value));
            Assert.False(test.TryDequeue(out value));
        }

        [Fact]
        public void TestDisposed()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                IConcurrentDictionary<int, string> test = new LurchTableTest<int, string>();
                test.Dispose();
                test.Add(1, "");
            });
        }

        [Fact]
        public void TestGetOrAdd()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();
            Assert.Equal("a", data.GetOrAdd(1, "a"));
            Assert.Equal("a", data.GetOrAdd(1, "b"));

            Assert.Equal("b", data.GetOrAdd(2, k => "b"));
            Assert.Equal("b", data.GetOrAdd(2, k => "c"));
        }

        [Fact]
        public void TestInitialize()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>(StringComparer.Ordinal);
            test["a"] = "b";
            Assert.Equal(1, test.Count);
            test.Initialize();
            Assert.Equal(0, test.Count);
        }

        [Fact]
        public void TestKeys()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>();
            test["a"] = "b";
            string all = string.Join("", new List<string>(test.Keys).ToArray());
            Assert.Equal("a", all);
        }

        [Fact]
        public void TestKeysEnumerator()
        {
            KeyValuePair<int, string>[] sample = GetSample();
            LurchTableTest<int, string> test = CreateSample(sample);
            int ix = 0;
            foreach (int key in test.Keys)
                Assert.Equal(sample[ix++].Value, test[key]);
        }

        [Fact]
        public void TestLimitorByAccess()
        {
            //multiple of prime will produce hash collision, thus testing removal of non-first elements
            const int prime = 1103;
            LurchTable<int, string> test = new LurchTable<int, string>(LurchTableOrder.Access, 3, prime, 10, 10,
                EqualityComparer<int>.Default);
            test[1 * prime] = "a";
            test[2 * prime] = "b";
            test[3 * prime] = "c";
            Assert.Equal(3, test.Count);
            Assert.Equal("b", test[2 * prime]); //access moves to front..
            test[4] = "d";
            test[5] = "e";
            Assert.Equal(3, test.Count); // still 3 items
            Assert.False(test.ContainsKey(1 * prime));
            Assert.True(test.ContainsKey(2 * prime)); //recently access is still there
            Assert.False(test.ContainsKey(3 * prime));
        }

        [Fact]
        public void TestNewAddOrUpdate()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();
            Assert.Equal("a", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));
            Assert.Equal("1", data.AddOrUpdate(1, "a", (k, v) => k.ToString()));

            Assert.Equal("b", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
            Assert.Equal("2", data.AddOrUpdate(2, k => "b", (k, v) => k.ToString()));
        }

        [Fact]
        public void TestSampleCollection()
        {
            KeyValuePair<int, string>[] sample = GetSample();
            LurchTableTest<int, string> items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            VerifyCollection(new KeyValueEquality<int, string>(), new List<KeyValuePair<int, string>>(sample), items);
            VerifyCollection(new KeyValueEquality<int, string>(), new List<KeyValuePair<int, string>>(sample), dict);
        }

        [Fact]
        public void TestSampleKeyCollection()
        {
            KeyValuePair<int, string>[] sample = GetSample();
            LurchTableTest<int, string> items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            List<int> keys = new List<int>();
            foreach (KeyValuePair<int, string> kv in sample)
                keys.Add(kv.Key);
            VerifyCollection(EqualityComparer<int>.Default, keys.AsReadOnly(), items.Keys);
            VerifyCollection(EqualityComparer<int>.Default, keys.AsReadOnly(), dict.Keys);
        }

        [Fact]
        public void TestSampleValueCollection()
        {
            KeyValuePair<int, string>[] sample = GetSample();
            LurchTableTest<int, string> items = CreateSample(sample);
            IDictionary<int, string> dict = items;
            List<string> values = new List<string>();
            foreach (KeyValuePair<int, string> kv in sample)
                values.Add(kv.Value);
            VerifyCollection(EqualityComparer<string>.Default, values.AsReadOnly(), items.Values);
            VerifyCollection(EqualityComparer<string>.Default, values.AsReadOnly(), dict.Values);
        }


        [Fact]
        public void TestTryRoutines()
        {
            LurchTableTest<int, string> data = new LurchTableTest<int, string>();

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

            Assert.False(data.TryUpdate(1, (k, x) => x.ToUpper()));
            data[1] = "a";
            data[1] = "b";
            Assert.True(data.TryUpdate(1, (k, x) => x.ToUpper()));
            Assert.Equal("B", data[1]);
        }

        [Fact]
        public void TestValues()
        {
            LurchTableTest<string, string> test = new LurchTableTest<string, string>();
            test["a"] = "b";
            string all = string.Join("", new List<string>(test.Values).ToArray());
            Assert.Equal("b", all);
        }

        [Fact]
        public void TestValuesEnumerator()
        {
            KeyValuePair<int, string>[] sample = GetSample();
            LurchTableTest<int, string> test = CreateSample(sample);
            int ix = 0;
            foreach (string value in test.Values)
                Assert.Equal(sample[ix++].Value, value);
        }
    }

    
    public class TestLurchTableDictionary : TestDictionary<LurchTable<Guid, string>, TestLurchTableDictionary.Factory,
        Guid, string>
    {
        private const int SAMPLE_SIZE = 1000;

        public new class Factory : IFactory<LurchTable<Guid, string>>
        {
            public LurchTable<Guid, string> Create()
            {
                return new LurchTable<Guid, string>(SAMPLE_SIZE, LurchTableOrder.Access);
            }
        }

        protected override KeyValuePair<Guid, string>[] GetSample()
        {
            List<KeyValuePair<Guid, string>> results = new List<KeyValuePair<Guid, string>>();
            for (int i = 0; i < SAMPLE_SIZE; i++)
            {
                Guid id = Guid.NewGuid();
                results.Add(new KeyValuePair<Guid, string>(id, id.ToString()));
            }
            return results.ToArray();
        }
    }
}