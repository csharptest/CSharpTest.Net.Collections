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
using System.IO;
using System.Collections.Generic;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.IO;
using CSharpTest.Net.Synchronization;
using System.Runtime.InteropServices;
using NUnit.Framework;
using CSharpTest.Net.Threading;
using System.Threading;
using CSharpTest.Net.Reflection;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class TestBackupAndRecovery
    {
        BPlusTree<Guid, TestInfo>.OptionsV2 GetOptions(TempFile temp)
        {
            BPlusTree<Guid, TestInfo>.OptionsV2 options = new BPlusTree<Guid, TestInfo>.OptionsV2(
                PrimitiveSerializer.Guid, new TestInfoSerializer());
            options.CalcBTreeOrder(Marshal.SizeOf(typeof(Guid)), Marshal.SizeOf(typeof(TestInfo)));
            options.CreateFile = CreatePolicy.IfNeeded;
            options.FileName = temp.TempPath;

            // The following three options allow for automatic commit/recovery:
            options.CallLevelLock = new ReaderWriterLocking();
            options.TransactionLogFileName = Path.ChangeExtension(options.FileName, ".tlog");
            return options;
        }

        static void Insert(BPlusTree<Guid, TestInfo> tree, IDictionary<Guid, TestInfo> testdata, int threads, int count, TimeSpan wait)
        {
            using (var work = new WorkQueue<IEnumerable<KeyValuePair<Guid, TestInfo>>>(tree.AddRange, threads))
            {
                foreach (var set in TestInfo.CreateSets(threads, count, testdata))
                    work.Enqueue(set);
                work.Complete(true, wait == TimeSpan.MaxValue ? Timeout.Infinite : (int)Math.Min(int.MaxValue, wait.TotalMilliseconds));
            }
        }

        [Test]
        public void TestRecoveryOnNewWithAsyncLog()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                options.TransactionLog = new TransactionLog<Guid, TestInfo>(
                    new TransactionLogOptions<Guid, TestInfo>(
                        options.TransactionLogFileName,
                        options.KeySerializer,
                        options.ValueSerializer
                        )
                    );
                TestRecoveryOnNew(options, 100, 0);
            }
        }

        [Test]
        public void TestRestoreLargeLog()
        {
            using (TempFile savelog = new TempFile())
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                options.FileBlockSize = 512;
                options.StoragePerformance = StoragePerformance.Fastest;
                options.CalcBTreeOrder(Marshal.SizeOf(typeof(Guid)), Marshal.SizeOf(typeof(TestInfo)));
                options.TransactionLog = new TransactionLog<Guid, TestInfo>(
                    new TransactionLogOptions<Guid, TestInfo>(
                        options.TransactionLogFileName,
                        options.KeySerializer,
                        options.ValueSerializer
                        )
                    );

                //Now recover...
                Dictionary<Guid, TestInfo> first = new Dictionary<Guid, TestInfo>();
                Dictionary<Guid, TestInfo> sample;

                using (var tree = new BPlusTree<Guid, TestInfo>(options))
                {
                    tree.EnableCount();
                    Insert(tree, first, 1, 100, TimeSpan.FromMinutes(1));
                    tree.Commit();

                    Assert.AreEqual(100, tree.Count);

                    sample = new Dictionary<Guid, TestInfo>(first);
                    Insert(tree, sample, 7, 5000, TimeSpan.FromMinutes(1));

                    Assert.AreEqual(35100, tree.Count);

                    for (int i = 0; i < 1; i++)
                    {
                        foreach (var rec in tree)
                        {
                            var value = rec.Value;
                            value.UpdateCount++;
                            value.ReadCount++;
                            tree[rec.Key] = value;
                        }
                    }
                    
                    File.Copy(options.TransactionLog.FileName, savelog.TempPath, true);
                    tree.Rollback();

                    TestInfo.AssertEquals(first, tree);
                }

                //file still has initial committed data
                TestInfo.AssertEquals(first, BPlusTree<Guid, TestInfo>.EnumerateFile(options));

                //restore the log and verify all data.
                File.Copy(savelog.TempPath, options.TransactionLog.FileName, true);
                using (var tree = new BPlusTree<Guid, TestInfo>(options))
                {
                    TestInfo.AssertEquals(sample, tree);
                }

                //file still has initial committed data
                TestInfo.AssertEquals(sample, BPlusTree<Guid, TestInfo>.EnumerateFile(options));
            }
        }

        [Test]
        public void TestRecoveryOnExistingWithAsyncLog()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                options.TransactionLog = new TransactionLog<Guid, TestInfo>(
                    new TransactionLogOptions<Guid, TestInfo>(
                        options.TransactionLogFileName,
                        options.KeySerializer,
                        options.ValueSerializer
                        ) { FileOptions = FileOptions.Asynchronous }
                    );
                TestRecoveryOnExisting(options, 100, 0);
            }
        }

        [Test]
        public void TestRecoveryOnNew()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                TestRecoveryOnNew(options, 10, 0);
            }
        }

        [Test]
        public void TestRecoveryOnExisting()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                TestRecoveryOnExisting(options, 10, 0);
            }
        }


        [Test]
        public void TestRecoveryOnNewLargeOrder()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                options.MaximumValueNodes = 255;
                options.MinimumValueNodes = 100;
                options.TransactionLog = new TransactionLog<Guid, TestInfo>(
                    new TransactionLogOptions<Guid, TestInfo>(
                        options.TransactionLogFileName,
                        options.KeySerializer,
                        options.ValueSerializer
                        ) { FileOptions = FileOptions.None } /* no-write through */
                    );
                TestRecoveryOnNew(options, 100, 10000);
            }
        }

        [Test]
        public void TestRecoveryOnExistingLargeOrder()
        {
            using (TempFile temp = new TempFile())
            {
                var options = GetOptions(temp);
                options.MaximumValueNodes = 255;
                options.MinimumValueNodes = 100;
                options.TransactionLog = new TransactionLog<Guid, TestInfo>(
                    new TransactionLogOptions<Guid, TestInfo>(
                        options.TransactionLogFileName,
                        options.KeySerializer,
                        options.ValueSerializer
                        ) { FileOptions = FileOptions.None } /* no-write through */
                    );
                TestRecoveryOnExisting(options, 100, ushort.MaxValue);
            }
        }

        void TestRecoveryOnNew(BPlusTree<Guid, TestInfo>.OptionsV2 options, int count, int added)
        {
            BPlusTree<Guid, TestInfo> tree = null;
            var temp = TempFile.Attach(options.FileName);
            Dictionary<Guid, TestInfo> data = new Dictionary<Guid, TestInfo>();
            try
            {
                Assert.IsNotNull(options.TransactionLog);
                temp.Delete();
                tree = new BPlusTree<Guid, TestInfo>(options);
                using (var log = options.TransactionLog)
                {
                    using ((IDisposable)new PropertyValue(tree, "_storage").Value)
                        Insert(tree, data, Environment.ProcessorCount, count, TimeSpan.MaxValue);
                    //Add extra data...
                    AppendToLog(log, TestInfo.Create(added, data));
                }
                tree = null;
                //No data... yet...
                using(TempFile testempty = TempFile.FromCopy(options.FileName))
                {
                    var testoptions = options.Clone();
                    testoptions.TransactionLogFileName = null;
                    testoptions.TransactionLog = null;
                    testoptions.FileName = testempty.TempPath;

                    using (var empty = new BPlusTree<Guid, TestInfo>(testoptions))
                    {
                        empty.EnableCount();
                        Assert.AreEqual(0, empty.Count);
                    }
                }

                //Now recover...
                using (var recovered = new BPlusTree<Guid, TestInfo>(options))
                {
                    TestInfo.AssertEquals(data, recovered);
                }
            }
            finally
            {
                temp.Dispose();
                if (tree != null)
                    tree.Dispose();
            }
        }

        void TestRecoveryOnExisting(BPlusTree<Guid, TestInfo>.OptionsV2 options, int count, int added)
        {
            BPlusTree<Guid, TestInfo> tree = null;
            var temp = TempFile.Attach(options.FileName);
            Dictionary<Guid, TestInfo> dataFirst, data = new Dictionary<Guid, TestInfo>();
            try
            {
                temp.Delete();
                Assert.IsNotNull(options.TransactionLog);

                using (tree = new BPlusTree<Guid, TestInfo>(options))
                {
                    Insert(tree, data, 1, 100, TimeSpan.MaxValue);
                    TestInfo.AssertEquals(data, tree);
                }
                tree = null;
                Assert.IsFalse(File.Exists(options.TransactionLogFileName));

                // All data commits to output file
                Assert.IsTrue(temp.Exists);
                TestInfo.AssertEquals(data, BPlusTree<Guid, TestInfo>.EnumerateFile(options));

                dataFirst = new Dictionary<Guid, TestInfo>(data);
                DateTime modified = temp.Info.LastWriteTimeUtc;

                tree = new BPlusTree<Guid, TestInfo>(options);
                using (var log = options.TransactionLog)
                {
                    using ((IDisposable) new PropertyValue(tree, "_storage").Value)
                        Insert(tree, data, Environment.ProcessorCount, count, TimeSpan.MaxValue);
                    //Add extra data...
                    AppendToLog(log, TestInfo.Create(added, data));
                }
                tree = null;

                //Still only contains original data
                Assert.AreEqual(modified, temp.Info.LastWriteTimeUtc);
                TestInfo.AssertEquals(dataFirst, BPlusTree<Guid, TestInfo>.EnumerateFile(options));

                //Now recover...
                using (var recovered = new BPlusTree<Guid, TestInfo>(options))
                {
                    TestInfo.AssertEquals(data, recovered);
                }
            }
            finally
            {
                temp.Dispose();
                if (tree != null)
                    tree.Dispose();
            }
        }

        private static void AppendToLog(ITransactionLog<Guid, TestInfo> log, IEnumerable<KeyValuePair<Guid, TestInfo>> keyValuePairs)
        {
            using(var items = keyValuePairs.GetEnumerator())
            {
                bool more = items.MoveNext();
                while (more)
                {
                    var tx = log.BeginTransaction();
                    int count = 1000;
                    do
                    {
                        log.AddValue(ref tx, items.Current.Key, items.Current.Value);
                        more = items.MoveNext();
                    } while (more && --count > 0);

                    log.CommitTransaction(ref tx);
                }
            }
        }
    }
}
