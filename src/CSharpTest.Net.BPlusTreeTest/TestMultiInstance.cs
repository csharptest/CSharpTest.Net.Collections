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
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    /// <summary>
    /// Demonstrates a few possible ways to use a BPlusTree across process boundaries by having a single-writer and multiple
    /// readers.  Calling Commit() on the writer can be dangerous to the readers, thus a cross-process thread synchronization
    /// primitive must be used to ensure that readers are aware of Commit() calls, and, in the case of read-only, not actively
    /// reading the tree in it's previous state.  The writer guarantees that the file is consistent at any given moment in 
    /// time; however, since the reader traverses the data over time it is not possible to guarantee a consistent read.  I do
    /// not have any intentions of further support for this capability, it's generally just a bad idea ;)  Instead, IIWY I 
    /// would look to use RPC to talk to the process controlling the writes.
    /// </summary>
    [TestFixture]
    public partial class TestMultiInstance
    {
        IEnumerable<KeyValuePair<int, string>> MakeValues(int start, int count)
        {
            for (int ix = start; count > 0; count--, ix++)
                yield return new KeyValuePair<int, string>(ix, ix.ToString());
        }

        // Opens and reviews a 'read-only' instance of an already open B+Tree.  Will only have access to data that the writer
        // has comitted to disk.
        [Test]
        public void TestReadOnlyCopy()
        {
            using (var tempFile = new TempFile())
            {
                var options = new BPlusTree<int, string>.OptionsV2(new PrimitiveSerializer(), new PrimitiveSerializer())
                                  {
                                      CreateFile = CreatePolicy.Always,
                                      FileName = tempFile.TempPath,
                                  }.CalcBTreeOrder(4, 10);

                var readcopy = options.Clone();
                readcopy.CreateFile = CreatePolicy.Never;
                readcopy.ReadOnly = true;

                using (var tree = new BPlusTree<int, string>(options))
                {
                    using (var copy = new BPlusTree<int, string>(readcopy))
                    {
                        copy.EnableCount();
                        Assert.AreEqual(0, copy.Count);
                    }

                    //insert some data...
                    tree.AddRange(MakeValues(0, 100));

                    using (var copy = new BPlusTree<int, string>(readcopy))
                    {
                        copy.EnableCount();
                        Assert.AreEqual(0, copy.Count);
                    }
                    tree.Commit();

                    //insert some data...
                    for (int i = 0; i < 100; i++)
                        tree.Remove(i);
                    tree.AddRange(MakeValues(1000, 1000));

                    using (var copy = new BPlusTree<int, string>(readcopy))
                    {
                        copy.EnableCount();
                        Assert.AreEqual(100, copy.Count);
                        Assert.AreEqual(0, copy.First().Key);
                        Assert.AreEqual(99, copy.Last().Key);
                    }

                    tree.Commit();

                }
            }
        }

        // Demonstrates creating a second copy of an existing tree while its still open, and then keeping that copy in sync
        // with the orignal by replaying the log periodically.  Calling Commit() on the writer instance will reset the log, 
        // so periodic calls to Commit should be avoided to allow reading the log file.
        [Test]
        public void TestSyncFromLogging()
        {
            using (var tempFile = new TempFile())
            using (var logfile = new TempFile())
            using (var tempCopy = new TempFile())
            {
                var options = new BPlusTree<int, string>.OptionsV2(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    CreateFile = CreatePolicy.Always,
                    FileName = tempFile.TempPath,
                    TransactionLogFileName = logfile.TempPath,
                }.CalcBTreeOrder(4, 10);

                var readcopy = options.Clone();
                readcopy.FileName = tempCopy.TempPath;
                readcopy.StoragePerformance = StoragePerformance.Fastest;

                using (var tree = new BPlusTree<int, string>(options))
                using (var copy = new BPlusTree<int, string>(readcopy))
                using (var tlog = new TransactionLog<int, string>(
                    new TransactionLogOptions<int, string>(logfile.TempPath, PrimitiveSerializer.Int32, PrimitiveSerializer.String) { ReadOnly = true }))
                {
                    tree.Add(0, "0");
                    tree.Commit();

                    long logpos = 0;
                    copy.EnableCount();
                    //start by copying the data from tree's file into the copy instance:
                    copy.BulkInsert(
                        BPlusTree<int, string>.EnumerateFile(options),
                        new BulkInsertOptions { InputIsSorted = true, CommitOnCompletion = false, ReplaceContents = true }
                        );

                    Assert.AreEqual(1, copy.Count);
                    Assert.AreEqual("0", copy[0]);

                    tlog.ReplayLog(copy, ref logpos);
                    Assert.AreEqual(1, copy.Count);

                    //insert some data...
                    tree.AddRange(MakeValues(1, 99));

                    tlog.ReplayLog(copy, ref logpos);
                    Assert.AreEqual(100, copy.Count);

                    //insert some data...
                    for (int i = 0; i < 100; i++)
                        tree.Remove(i);
                    tlog.ReplayLog(copy, ref logpos);
                    Assert.AreEqual(0, copy.Count);

                    tree.AddRange(MakeValues(1000, 1000));

                    tlog.ReplayLog(copy, ref logpos);
                    Assert.AreEqual(1000, copy.Count);
                }
            }
        }
    }
}
