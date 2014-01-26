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
using CSharpTest.Net.BPlusTree.Test.SampleTypes;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Crypto;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class SampleCustomTypeTest : TestDictionary<BPlusTree<KeyInfo, DataValue>, SampleCustomTypeTest.BTreeFactory, KeyInfo, DataValue>
    {
        protected TempFile TempFile;
        #region TestFixture SetUp/TearDown
        [TestFixtureSetUp]
        public virtual void Setup()
        {
            TempFile = new TempFile();
        }

        [TestFixtureTearDown]
        public virtual void Teardown()
        {
            TempFile.Dispose();
        }
        #endregion

        public class BTreeFactory : IFactory<BPlusTree<KeyInfo, DataValue>>
        {
            public BPlusTree<KeyInfo, DataValue> Create()
            {
                BPlusTree<KeyInfo, DataValue>.Options options =
                    new BPlusTree<KeyInfo, DataValue>.Options(new KeyInfoSerializer(), new DataValueSerializer(), new KeyInfoComparer())
                    {
                        MinimumChildNodes = 16,
                        MaximumChildNodes = 24,
                        MinimumValueNodes = 4,
                        MaximumValueNodes = 12,
                    };

                BPlusTree<KeyInfo, DataValue> tree = new BPlusTree<KeyInfo, DataValue>(options);
                tree.EnableCount();
                return tree;
            }
        }

        protected override KeyValuePair<KeyInfo, DataValue>[] GetSample()
        {
            List<KeyValuePair<KeyInfo, DataValue>> all = new List<KeyValuePair<KeyInfo, DataValue>>();

            Random rand = new Random();
            byte[] data = new byte[255];

            for (int i = 0; i < 100; i++)
            {
                KeyInfo k1 = new KeyInfo();
                rand.NextBytes(data);
                all.Add(new KeyValuePair<KeyInfo,DataValue>(k1, new DataValue(k1, data)));
            }
            return all.ToArray();
        }

        [Test]
        public void TestCommonConfiguration()
        {
            BPlusTree<KeyInfo, DataValue>.Options options =
                new BPlusTree<KeyInfo, DataValue>.Options(new KeyInfoSerializer(), new DataValueSerializer(), new KeyInfoComparer());
            options.CalcBTreeOrder(32, 300);//we can simply just guess close
            options.FileName = TempFile.TempPath;
            options.CreateFile = CreatePolicy.Always;//obviously this is just for testing
            Assert.AreEqual(FileVersion.Version1, options.FileVersion);

            Random rand = new Random();
            KeyInfo k1 = new KeyInfo(), k2 = new KeyInfo();

            using (BPlusTree<KeyInfo, DataValue> tree = new BPlusTree<KeyInfo, DataValue>(options))
            {
                byte[] data = new byte[255];

                rand.NextBytes(data);
                tree.Add(k1, new DataValue(k1, data));

                Assert.IsTrue(tree.ContainsKey(k1));
                Assert.IsFalse(tree.ContainsKey(k1.Next()));
                Assert.AreEqual(data, tree[k1].Bytes);

                rand.NextBytes(data);
                tree.Add(k2, new DataValue(k2, data));

                Assert.IsTrue(tree.ContainsKey(k2));
                Assert.IsFalse(tree.ContainsKey(k2.Next()));
                Assert.AreEqual(data, tree[k2].Bytes);
            }
            options.CreateFile = CreatePolicy.Never;
            using (BPlusTree<KeyInfo, DataValue> tree = new BPlusTree<KeyInfo, DataValue>(options))
            {
                Assert.IsTrue(tree.ContainsKey(k1));
                Assert.IsTrue(tree.ContainsKey(k2));
            }
        }
    }
}
