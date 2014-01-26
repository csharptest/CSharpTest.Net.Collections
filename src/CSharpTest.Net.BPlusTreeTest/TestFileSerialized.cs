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
using System.IO;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class BasicFileTests : BasicTests
    {
        TempFile TempFile;

        [SetUp]
        public void Setup()
        { TempFile = new TempFile(); }

        [TearDown]
        public void TearDown()
        { TempFile.Dispose(); }

        protected override BPlusTreeOptions<int, string> Options
        {
            get
            {
                return new BPlusTree<int, string>.Options(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    BTreeOrder = 4,
                    LockingFactory = new IgnoreLockFactory(),
                    CachePolicy = CachePolicy.All,
                    CreateFile = CreatePolicy.Always,
                    FileName = TempFile.TempPath,
                };
            }
        }

        [Test]
        public void TestRecoverCorruptedFile()
        {
            BPlusTree<int, string>.Options options = (BPlusTree<int, string>.Options)Options;
            options.BTreeOrder = 4;
            options.FileBlockSize = 512;
            options.FileGrowthRate = 25;
            options.ConcurrentWriters = 4;
            options.FileOpenOptions = FileOptions.None;

            using (BPlusTree<int, string> tree = new BPlusTree<int, string>(options))
            {
                for(int i=0; i < 100; i++)
                    Assert.IsTrue(tree.TryAdd(i, i.ToString()));
            }

            using (Stream io = TempFile.Open())
            {
                //first we can corrupt the root node, which is always at an offset of BlockSize
                io.Seek(512, SeekOrigin.Begin);
                io.Write(new byte[512], 0, 512);

                //Now let's corrupt one byte from the end of the node at index 3
                io.Seek(1024 + 16, SeekOrigin.Begin);
                int len = PrimitiveSerializer.Int32.ReadFrom(io);
                io.Seek(1024 + 32 + len - 1, SeekOrigin.Begin);//secrets of fragmented file revealed... ugly i know.
                io.WriteByte(255); //overwrite last used byte in chunk.
            }

            options.CreateFile = CreatePolicy.Never;

            //Now that we've corrupted part of the file content, let's take a peek
            try
            {
                using (BPlusTree<int, string> tree = new BPlusTree<int, string>(options))
                {
                    foreach (KeyValuePair<int, string> kv in tree)
                        Assert.AreEqual(kv.Key.ToString(), kv.Value);
                }
                Assert.Fail("Expected InvalidDataException");
            }
            catch (InvalidDataException)
            { }

            Dictionary<int, string> found = new Dictionary<int, string>();
            List<int> duplicates = new List<int>();
            foreach (KeyValuePair<int, string> kv in BPlusTree<int, string>.RecoveryScan(options, FileShare.None))
            {
                if (!found.ContainsKey(kv.Key))
                    found.Add(kv.Key, kv.Value);
                else
                    duplicates.Add(kv.Key);
                
                Assert.AreEqual(kv.Key.ToString(), kv.Value);
            }
            Assert.AreNotEqual(0, found.Count);

            //The following may change...
            Assert.AreEqual(99, found.Count);
            Assert.IsFalse(found.ContainsKey(3), "should be missing #3");
            Assert.AreEqual(0, duplicates.Count);
        }

        [Test]
        public void TestDeleteUnderlyingFile()
        {
            try
            {
                using (BPlusTree<int, string> tree = new BPlusTree<int, string>(Options))
                {
                    Assert.IsTrue(tree.TryAdd(1, "hi"));
                    TempFile.Delete();
                }
                Assert.Fail();
            }
            catch(IOException) { }
        }

        [Test]
        public void TestDeleteAfterGarbageCollection()
        {
            System.Threading.ThreadStart fn = delegate()
            {
                BPlusTree<int, string> tree = new BPlusTree<int, string>(Options);
                Assert.IsTrue(tree.TryAdd(1, "hi"));
                tree = null;
            };
            
            fn();

            //Allow the GC to collect the BTree
            System.Threading.Thread.Sleep(10);
            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();

            //Make sure the file has been released
            TempFile.Delete();
        }
    }
    [TestFixture]
    public class TestFileSerialized : TestDictionary<BPlusTree<int, string>, TestSimpleDictionary.BTreeFactory, int, string>
    {
        public class BTreeFactory : IFactory<BPlusTree<int, string>>
        {
            static TempFile TempFile;

            public BPlusTree<int, string> Create()
            {
                TempFile = TempFile ?? new TempFile();

                BPlusTree<int, string> tree = new BPlusTree<int, string>(
                    new BPlusTree<int, string>.Options(PrimitiveSerializer.Instance, PrimitiveSerializer.Instance)
                        {
                            BTreeOrder = 16,
                            LockingFactory = new IgnoreLockFactory(),
                            FileName = TempFile.TempPath,
                            CreateFile = CreatePolicy.Always,
                        }
                    );
                tree.EnableCount();
                return tree;
            }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            return new[] 
                       {
                           new KeyValuePair<int,string>(1, "1"),
                           new KeyValuePair<int,string>(3, "3"),
                           new KeyValuePair<int,string>(5, "5"),
                           new KeyValuePair<int,string>(7, "7"),
                           new KeyValuePair<int,string>(9, "9"),
                           new KeyValuePair<int,string>(11, "11"),
                           new KeyValuePair<int,string>(13, "13"),
                       };
        }
    }
}