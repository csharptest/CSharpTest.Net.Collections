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
using CSharpTest.Net.Crypto;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [StructLayout(LayoutKind.Sequential)]
    struct TestInfo
    {
        public TestInfo(Guid guid)
            : this()
        {
            MyKey = guid;
        }
        public readonly Guid MyKey;
        public int SetNumber;
        public long CreateOrder;
        public long ReadCount;
        public long UpdateCount;
        public byte[] RandomBytes;

        public static IEnumerable<KeyValuePair<Guid, TestInfo>> Create(int count)
        { return CreateSet(1, count, null); }
        public static IEnumerable<KeyValuePair<Guid, TestInfo>> Create(int count, IDictionary<Guid, TestInfo> values)
        { return CreateSet(1, count, values); }
        
        
        public static IEnumerable<KeyValuePair<Guid, TestInfo>>[] CreateSets(int sets, int items)
        { return CreateSets(sets, items, null); }
        public static IEnumerable<KeyValuePair<Guid, TestInfo>>[] CreateSets(int sets, int items, IDictionary<Guid, TestInfo> values)
        {
            IEnumerable<KeyValuePair<Guid, TestInfo>>[] result = new IEnumerable<KeyValuePair<Guid, TestInfo>>[sets];
            for (int i = 1; i <= sets; i++)
                result[i-1] = CreateSet(i, items, values);
            return result;
        }

        private static IEnumerable<KeyValuePair<Guid, TestInfo>> CreateSet(int set, int count, IDictionary<Guid, TestInfo> values)
        {
            for (int i = 1; i <= count; i++)
            {
                TestInfo ti = new TestInfo(Guid.NewGuid())
                                  {
                                      SetNumber = set,
                                      CreateOrder = i,
                                      ReadCount = 0,
                                      UpdateCount = 0
                                  };
                if (values != null)
                {
                    lock (values)
                        values.Add(ti.MyKey, ti);
                }
                yield return new KeyValuePair<Guid, TestInfo>(ti.MyKey, ti);
            }
        }

        public static void AssertEquals(IDictionary<Guid, TestInfo> x, IEnumerable<KeyValuePair<Guid, TestInfo>> y)
        {
            Dictionary<Guid, TestInfo> copy = new Dictionary<Guid, TestInfo>(x);
            foreach(KeyValuePair<Guid, TestInfo> item in y)
            {
                TestInfo value;
                Assert.IsTrue(x.TryGetValue(item.Key, out value));
                Assert.AreEqual(item.Key, item.Value.MyKey);
                Assert.AreEqual(item.Value.MyKey, value.MyKey);
                Assert.AreEqual(item.Value.SetNumber, value.SetNumber);
                Assert.AreEqual(item.Value.CreateOrder, value.CreateOrder);
                Assert.IsTrue(BinaryComparer.Equals(item.Value.RandomBytes, value.RandomBytes));
                Assert.IsTrue(copy.Remove(item.Key));
            }
            Assert.IsEmpty(copy);
        }
    }

    struct TestInfoSerializer : ISerializer<TestInfo>
    {
        TestInfo ISerializer<TestInfo>.ReadFrom(System.IO.Stream stream)
        {
            return new TestInfo(PrimitiveSerializer.Guid.ReadFrom(stream))
            {
                SetNumber = PrimitiveSerializer.Int32.ReadFrom(stream),
                CreateOrder = PrimitiveSerializer.Int64.ReadFrom(stream),
                ReadCount = PrimitiveSerializer.Int64.ReadFrom(stream),
                UpdateCount = PrimitiveSerializer.Int64.ReadFrom(stream),
                RandomBytes = PrimitiveSerializer.Bytes.ReadFrom(stream)
            };
        }

        void ISerializer<TestInfo>.WriteTo(TestInfo value, System.IO.Stream stream)
        {
            PrimitiveSerializer.Guid.WriteTo(value.MyKey, stream);
            PrimitiveSerializer.Int32.WriteTo(value.SetNumber, stream);
            PrimitiveSerializer.Int64.WriteTo(value.CreateOrder, stream);
            PrimitiveSerializer.Int64.WriteTo(value.ReadCount, stream);
            PrimitiveSerializer.Int64.WriteTo(value.UpdateCount, stream);
            PrimitiveSerializer.Bytes.WriteTo(value.RandomBytes, stream);
        }
    }
}
