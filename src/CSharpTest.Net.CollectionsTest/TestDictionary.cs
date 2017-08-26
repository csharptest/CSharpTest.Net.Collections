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

using System;
using System.Collections.Generic;
using CSharpTest.Net.Interfaces;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    public abstract class
        TestDictionary<TDictionary, TFactory, TKey, TValue> : TestCollection<TDictionary, TFactory,
            KeyValuePair<TKey, TValue>>
        where TDictionary : IDictionary<TKey, TValue>, IDisposable
        where TFactory : IFactory<TDictionary>, new()
    {
        [Fact]
        public void TestAddRemoveByKey()
        {
            KeyValuePair<TKey, TValue>[] sample = GetSample();

            using (TDictionary test = Factory.Create())
            {
                foreach (KeyValuePair<TKey, TValue> kv in sample)
                    test.Add(kv.Key, kv.Value);

                foreach (KeyValuePair<TKey, TValue> kv in sample)
                    Assert.True(test.ContainsKey(kv.Key));

                TValue cmp;
                foreach (KeyValuePair<TKey, TValue> kv in sample)
                    Assert.True(test.TryGetValue(kv.Key, out cmp) && kv.Value.Equals(cmp));

                foreach (KeyValuePair<TKey, TValue> kv in sample)
                    Assert.True(test.Remove(kv.Key));
            }
        }

        [Fact]
        public void TestKeys()
        {
            KeyValuePair<TKey, TValue>[] sample = GetSample();

            using (TDictionary test = Factory.Create())
            {
                List<TKey> keys = new List<TKey>();

                foreach (KeyValuePair<TKey, TValue> kv in sample)
                {
                    test[kv.Key] = kv.Value;
                    keys.Add(kv.Key);
                }

                List<TKey> cmp = new List<TKey>(test.Keys);

                Assert.Equal(keys.Count, cmp.Count);
                for (int i = 0; i < keys.Count; i++)
                    Assert.True(test.ContainsKey(keys[i]));
            }
        }

        [Fact]
        public void TestValues()
        {
            KeyValuePair<TKey, TValue>[] sample = GetSample();

            using (TDictionary test = Factory.Create())
            {
                List<TValue> values = new List<TValue>();

                foreach (KeyValuePair<TKey, TValue> kv in sample)
                {
                    test[kv.Key] = kv.Value;
                    values.Add(kv.Value);
                }

                List<TValue> cmp = new List<TValue>(test.Values);
                Assert.Equal(values.Count, cmp.Count);
            }
        }
    }
}