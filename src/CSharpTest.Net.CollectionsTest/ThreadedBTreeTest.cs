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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CSharpTest.Net.Collections.Test.Reflection;
using CSharpTest.Net.Collections.Test.SampleTypes;
using CSharpTest.Net.Collections.Test.Threading;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{

    public class ThreadedBTreeTest : IDisposable
    {
        protected static int RecordsCreated = 0;
        protected TempFile TempFile = new TempFile();

        private int StartAndAbortWriters(BPlusTreeOptions<KeyInfo, DataValue> options, TempFile copy)
        {
            RecordsCreated = 0;
            int minRecordCreated;
            BPlusTree<KeyInfo, DataValue> dictionary = new BPlusTree<KeyInfo, DataValue>(options);
            try
            {
                using (WorkQueue work = new WorkQueue(Environment.ProcessorCount))
                {
                    Exception lastError = null;
                    work.OnError += delegate (object o, ErrorEventArgs e) { lastError = e.GetException(); };

                    Thread.Sleep(1);
                    for (int i = 0; i < Environment.ProcessorCount; i++)
                        work.Enqueue(new ThreadedTest(dictionary, 10000000).Run);

                    while (RecordsCreated < 1000)
                        Thread.Sleep(1);

                    minRecordCreated = Interlocked.CompareExchange(ref RecordsCreated, 0, 0);
                    if (copy != null)
                        File.Copy(options.FileName, copy.TempPath); //just grab a copy any old time.
                    work.Complete(false, 0); //hard-abort all threads

                    //if(lastError != null)
                    //    Assert.Equal(typeof(InvalidDataException), lastError.GetType());
                }

                // force the file to close without disposing the btree
                IDisposable tmp = (IDisposable)new PropertyValue(dictionary, "_storage").Value;
                tmp.Dispose();
            }
            catch
            {
                dictionary.Dispose();
                throw;
            }
            return minRecordCreated;
        }

        private class ExpectedException : Exception
        {
        }

        private class KeyInfoEquality : IEqualityComparer<KeyInfo>
        {
            private static readonly KeyInfoComparer Comparer = new KeyInfoComparer();

            public bool Equals(KeyInfo x, KeyInfo y)
            {
                return Comparer.Compare(x, y) == 0;
            }

            public int GetHashCode(KeyInfo obj)
            {
                return obj.UID.GetHashCode();
            }
        }

        private class ThreadedTest
        {
            private readonly IDictionary<KeyInfo, DataValue> _data;
            private readonly int _recordCount;

            public ThreadedTest(IDictionary<KeyInfo, DataValue> data, int recordCount)
            {
                _data = data;
                _recordCount = recordCount;
            }

            public void Run()
            {
                byte[] bytes = new byte[255];
                Random rand = new Random();
                int create = 0, read = 0, write = 0, delete = 0;
                Stopwatch time = new Stopwatch();
                time.Start();

                try
                {
                    Dictionary<KeyInfo, DataValue> values = new Dictionary<KeyInfo, DataValue>(new KeyInfoEquality());
                    for (int i = 0; i < _recordCount; i++)
                    {
                        create++;
                        KeyInfo k = new KeyInfo();
                        rand.NextBytes(bytes);
                        DataValue v = new DataValue(k, bytes);

                        _data.Add(k, v);
                        Interlocked.Increment(ref RecordsCreated);
                        values.Add(k, v);
                    }

                    Dictionary<KeyInfo, DataValue> found = new Dictionary<KeyInfo, DataValue>(values, new KeyInfoEquality());
                    foreach (KeyValuePair<KeyInfo, DataValue> kv in _data)
                    {
                        read++;
                        found.Remove(kv.Key);
                    }

                    Assert.Equal(0, found.Count);

                    foreach (KeyValuePair<KeyInfo, DataValue> kv in values)
                    {
                        read++;
                        DataValue test;
                        Assert.True(_data.TryGetValue(kv.Key, out test));
                        Assert.Equal(kv.Key.UID, test.Key.UID);
                        Assert.Equal(kv.Value.Hash, test.Hash);
                        Assert.Equal(kv.Value.Bytes, test.Bytes);
                    }

                    foreach (KeyValuePair<KeyInfo, DataValue> kv in values)
                    {
                        bytes = kv.Value.Bytes;
                        Array.Reverse(bytes);

                        write++;
                        _data[kv.Key] = new DataValue(kv.Key, bytes);
                        read++;
                        Assert.Equal(bytes, _data[kv.Key].Bytes);
                    }

                    foreach (KeyInfo key in new List<KeyInfo>(values.Keys))
                    {
                        delete++;
                        Assert.True(_data.Remove(key));
                        values.Remove(key);
                    }
                }
                finally
                {
                    time.Stop();
                    Trace.TraceInformation("thread {0,3} complete ({1}c/{2}r/{3}w/{4}d) in {5:n0}ms",
                        Thread.CurrentThread.ManagedThreadId,
                        create, read, write, delete, time.ElapsedMilliseconds);
                }
            }
        }

        [Fact]
        [Trait("Category", "Benchmark")]
        public void LoopTestAbortWritersAndRecover()
        {
            for (int i = 0; i < 10; i++)
                TestAbortWritersAndRecover();
        }

        [Fact]
        public void TestAbortWritersAndRecover()
        {
            BPlusTree<KeyInfo, DataValue>.Options options = new BPlusTree<KeyInfo, DataValue>.Options(
                new KeyInfoSerializer(), new DataValueSerializer(), new KeyInfoComparer());
            options.CalcBTreeOrder(32, 300);
            options.FileName = TempFile.TempPath;
            options.CreateFile = CreatePolicy.Always;

            using (TempFile copy = new TempFile())
            {
                copy.Delete();
                int minRecordCreated = StartAndAbortWriters(options, copy);

                using (TempFile.Attach(copy.TempPath + ".recovered")) //used to create the new copy
                using (TempFile.Attach(copy.TempPath + ".deleted")) //renamed existing file
                {
                    options.CreateFile = CreatePolicy.Never;
                    int recoveredRecords = BPlusTree<KeyInfo, DataValue>.RecoverFile(options);
                    if (recoveredRecords < RecordsCreated)
                        Assert.True(false, $"Unable to recover records, recieved ({recoveredRecords} of {RecordsCreated}).");

                    options.FileName = copy.TempPath;
                    recoveredRecords = BPlusTree<KeyInfo, DataValue>.RecoverFile(options);
                    Assert.True(recoveredRecords >= minRecordCreated,
                        "Expected at least " + minRecordCreated + " found " + recoveredRecords);

                    using (BPlusTree<KeyInfo, DataValue> dictionary = new BPlusTree<KeyInfo, DataValue>(options))
                    {
                        dictionary.EnableCount();
                        Assert.Equal(recoveredRecords, dictionary.Count);

                        foreach (KeyValuePair<KeyInfo, DataValue> kv in dictionary)
                        {
                            Assert.Equal(kv.Key.UID, kv.Value.Key.UID);
                            dictionary.Remove(kv.Key);
                        }

                        Assert.Equal(0, dictionary.Count);
                    }
                }
            }
        }

        [Fact]
        public void TestAtomicUpdate()
        {
            int threads = Environment.ProcessorCount;
            const int updates = 10000;

            KeyValueUpdate<int, int> fnIncrement = delegate (int k, int i) { return i + 1; };
            Action<BPlusTree<int, int>> fnDoUpdates = delegate (BPlusTree<int, int> t)
            {
                for (int i = 0; i < updates; i++) t.TryUpdate(1, fnIncrement);
            };

            Stopwatch time = new Stopwatch();
            time.Start();
            using (BPlusTree<int, int> tree =
                new BPlusTree<int, int>(new BPlusTree<int, int>.OptionsV2(PrimitiveSerializer.Instance,
                    PrimitiveSerializer.Instance)))
            {
                tree[1] = 0;
                using (WorkQueue w = new WorkQueue(threads))
                {
                    for (int i = 0; i < threads; i++)
                        w.Enqueue(fnDoUpdates, tree);

                    Assert.True(w.Complete(true, 60000));
                }

                Assert.Equal(updates * threads, tree[1]);
            }

            Trace.TraceInformation("Updated {0} times on each of {1} threads in {2}ms", updates, threads,
                time.ElapsedMilliseconds);
        }

        [Fact]
        public void TestCallLevelLocking()
        {
            BPlusTree<int, int>.OptionsV2 options = new BPlusTree<int, int>.OptionsV2(
                new PrimitiveSerializer(), new PrimitiveSerializer());
            options.LockTimeout = 100;
            options.CallLevelLock = new ReaderWriterLocking();

            using (BPlusTree<int, int> dictionary = new BPlusTree<int, int>(options))
            {
                bool canwrite = false, canread = false;
                Action proc = () =>
                {
                    try
                    {
                        dictionary[1] = 1;
                        canwrite = true;
                    }
                    catch
                    {
                        canwrite = false;
                    }

                    try
                    {
                        int i;
                        dictionary.TryGetValue(1, out i);
                        canread = true;
                    }
                    catch
                    {
                        canread = false;
                    }
                };

                Assert.True(Task.Run(proc).Wait(1000));
                Assert.True(canwrite);
                Assert.True(canread);

                //now we lock the entire btree:
                using (dictionary.CallLevelLock.Write())
                {
                    //they can't read or write
                    Assert.True(Task.Run(proc).Wait(1000));
                    Assert.False(canwrite);
                    Assert.False(canread);
                    //but we can
                    proc();
                    Assert.True(canwrite);
                    Assert.True(canread);
                }

                //lock release all is well
                Assert.True(Task.Run(proc).Wait(1000));
                Assert.True(canwrite);
                Assert.True(canread);

                //We can also make sure noone else gains exclusive access with a read lock
                using (dictionary.CallLevelLock.Read())
                {
                    Assert.True(Task.Run(proc).Wait(1000));
                    Assert.True(canwrite);
                    Assert.True(canread);
                }
            }
        }

        [Fact]
        public void TestConcurrentCreateReadUpdateDelete8000()
        {
            BPlusTree<KeyInfo, DataValue>.OptionsV2 options = new BPlusTree<KeyInfo, DataValue>.OptionsV2(
                new KeyInfoSerializer(), new DataValueSerializer(), new KeyInfoComparer());

            const int keysize = 16 + 4;
            const int valuesize = keysize + 256 + 44;

            options.CalcBTreeOrder(keysize, valuesize);
            options.FileName = TempFile.TempPath;
            options.CreateFile = CreatePolicy.Always;
            options.FileBlockSize = 8192;
            options.StorageType = StorageType.Disk;

            options.CacheKeepAliveTimeout = 10000;
            options.CacheKeepAliveMinimumHistory = 0;
            options.CacheKeepAliveMaximumHistory = 200;

            options.CallLevelLock = new ReaderWriterLocking();
            options.LockingFactory = new LockFactory<SimpleReadWriteLocking>();
            options.LockTimeout = 10000;

            using (BPlusTree<KeyInfo, DataValue> dictionary = new BPlusTree<KeyInfo, DataValue>(options))
            using (WorkQueue work = new WorkQueue(Environment.ProcessorCount))
            {
                Exception lastError = null;
                work.OnError += delegate (object o, ErrorEventArgs e) { lastError = e.GetException(); };

                for (int i = 0; i < Environment.ProcessorCount; i++)
                    work.Enqueue(new ThreadedTest(dictionary, 1000).Run);

                Assert.True(work.Complete(true, 60000));
                Assert.True(lastError == null, $"Exception raised in worker: {lastError}");
            }
        }

        [Fact]
        public void TestErrorsOnInsertAndDelete()
        {
            const int CountPerThread = 100;

            BPlusTree<KeyInfo, DataValue>.OptionsV2 options = new BPlusTree<KeyInfo, DataValue>.OptionsV2(
                new KeyInfoSerializer(), new DataValueSerializer(), new KeyInfoComparer());
            options.CalcBTreeOrder(32, 300);
            options.FileName = TempFile.TempPath;
            options.CreateFile = CreatePolicy.Always;

            using (BPlusTree<KeyInfo, DataValue> dictionary = new BPlusTree<KeyInfo, DataValue>(options))
            using (WorkQueue work = new WorkQueue(Environment.ProcessorCount))
            {
                Exception lastError = null;
                work.OnError += delegate (object o, ErrorEventArgs e) { lastError = e.GetException(); };

                for (int i = 0; i < Environment.ProcessorCount; i++)
                    work.Enqueue(new ThreadedTest(dictionary, CountPerThread).Run);

                for (int i = 0; i < CountPerThread; i++)
                    if (i % 2 == 0)
                        try
                        {
                            dictionary.TryAdd(new KeyInfo(Guid.NewGuid(), i), k => { throw new ExpectedException(); });
                        }
                        catch
                        {
                        }
                    else
                        try
                        {
                            dictionary.TryRemove(dictionary.First().Key, (k, v) => { throw new ExpectedException(); });
                        }
                        catch
                        {
                        }

                Assert.True(work.Complete(true, 60000));
                Assert.True(lastError == null, $"Exception raised in worker: {lastError}");
            }
        }

        public void Dispose()
        {
            TempFile.Dispose();
        }
    }
}