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
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;
using CSharpTest.Net.IO;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class BasicTestsVersion2 : BasicTests
    {
        private static TempFile _tempFile;

        protected override BPlusTreeOptions<int, string> Options
        {
            get
            {
                if (_tempFile != null)
                    _tempFile.Dispose();

                _tempFile = new TempFile();
                var options = new BPlusTree<int, string>.OptionsV2(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    CreateFile = CreatePolicy.Always,
                    FileName = _tempFile.TempPath,
                }.CalcBTreeOrder(4, 10);

                Assert.AreEqual(FileVersion.Version2, options.FileVersion);
                return options;
            }
        }

        [Test]
        public void TestLogOptions()
        {
            var options = (BPlusTree<int, string>.OptionsV2)Options;

            Assert.AreEqual(ExistingLogAction.Default, options.ExistingLogAction);
            options.ExistingLogAction = ExistingLogAction.Ignore;
            Assert.AreEqual(ExistingLogAction.Ignore, options.ExistingLogAction);

            Assert.AreEqual(-1, options.TransactionLogLimit);
            options.TransactionLogLimit = int.MaxValue;
            Assert.AreEqual(int.MaxValue, options.TransactionLogLimit);
        }

        [Test]
        public void TestAutoCommit()
        {
            var options = (BPlusTree<int, string>.OptionsV2)Options;
            options.TransactionLogLimit = 30;

            using (BPlusTree<int, string> tree = Create(options))
            {
                tree.EnableCount();
                Assert.AreEqual(0, tree.Count);

                tree.Add(1, "A");
                tree.Rollback();
                Assert.AreEqual(0, tree.Count);

                tree.Add(1, "A");
                tree.Add(2, "B"); //The second write exceeds 30 bytes and auto-commits
                tree.Rollback();
                Assert.AreEqual(2, tree.Count);
                tree.Add(3, "C");
                tree.Add(4, "D"); //The second write will commit, but not the last
                tree.Add(5, "E");
                tree.Rollback();
                Assert.AreEqual(4, tree.Count);
                Assert.IsFalse(tree.ContainsKey(5));
            }
        }

        [Test]
        public void TestCommitRollback()
        {
            using (BPlusTree<int, string> tree = Create(Options))
            {
                tree.EnableCount();
                Assert.AreEqual(0, tree.Count);
                tree.Rollback();
                Assert.AreEqual(0, tree.Count);
                tree.Commit();
                Assert.AreEqual(0, tree.Count);

                tree.Add(1, "A");
                tree.Rollback();
                Assert.AreEqual(0, tree.Count);
                tree.Commit();
                Assert.AreEqual(0, tree.Count);

                tree.Add(1, "A");
                tree.Commit();
                Assert.AreEqual(1, tree.Count);
                tree.Rollback();
                Assert.AreEqual(1, tree.Count);

                tree.Add(2, "B");
                tree.Rollback();
                Assert.AreEqual(1, tree.Count);
                tree[1] = "abc";
                tree.Commit();
                Assert.AreEqual(1, tree.Count);
                tree.Rollback();

                Assert.AreEqual("abc", tree[1]);
                Assert.IsFalse(tree.ContainsKey(2));
            }

        }
    }
    [TestFixture]
    public class BasicTestsVersion2NoCache : BasicTests
    {
        private static TempFile _tempFile;

        protected override BPlusTreeOptions<int, string> Options
        {
            get
            {
                if (_tempFile != null)
                    _tempFile.Dispose();

                _tempFile = new TempFile();
                var options = new BPlusTree<int, string>.OptionsV2(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    CreateFile = CreatePolicy.Always,
                    CachePolicy = CachePolicy.None,
                    FileName = _tempFile.TempPath,
                    StoragePerformance = StoragePerformance.Fastest,
                }.CalcBTreeOrder(4, 10);

                Assert.AreEqual(FileVersion.Version2, options.FileVersion);
                return options;
            }
        }
    }
    [TestFixture]
    public class DictionaryTestsVersion2 : TestDictionary<BPlusTree<int, string>, TestSimpleDictionary.BTreeFactory, int, string>
    {
        public class BTreeFactory : IFactory<BPlusTree<int, string>>
        {
            private static TempFile _tempFile;

            public BPlusTree<int, string> Create()
            {
                if (_tempFile != null)
                    _tempFile.Dispose();

                _tempFile = new TempFile();
                BPlusTree<int, string> tree = new BPlusTree<int, string>(
                    new BPlusTree<int, string>.OptionsV2(PrimitiveSerializer.Instance, PrimitiveSerializer.Instance)
                    {
                            CreateFile = CreatePolicy.Always,
                            FileName = _tempFile.TempPath,
                        }.CalcBTreeOrder(4, 10)
                    );
                tree.EnableCount();
                return tree;
            }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            List<KeyValuePair<int, string>> items = new List<KeyValuePair<int, string>>();
            for (int i = 1; i <= 1000; i++)
                items.Add(new KeyValuePair<int, string>(i, (~i).ToString()));
            return items.ToArray();
        }
    }
}