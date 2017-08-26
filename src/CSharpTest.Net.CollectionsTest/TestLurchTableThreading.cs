﻿#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Threading;
using CSharpTest.Net.Collections;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestLurchTableThreading
    {
        private const int MAXTHREADS = 8;
        private const int COUNT = 1000;

        private static LurchTable<Guid, T> CreateMap<T>()
        {
            LurchTable<Guid, T> ht = new LurchTable<Guid, T>(COUNT, LurchTableOrder.Access);
            return ht;
        }

        private static void Parallel<T>(int loopCount, T[] args, Action<T> task)
        {
            Stopwatch timer = Stopwatch.StartNew();
            int[] ready = {0};
            ManualResetEvent start = new ManualResetEvent(false);
            int nthreads = Math.Min(MAXTHREADS, args.Length);
            Thread[] threads = new Thread[nthreads];
            for (int i = 0; i < threads.Length; i++)
                threads[i] = new Thread(ithread =>
                {
                    Interlocked.Increment(ref ready[0]);
                    start.WaitOne();
                    for (int loop = 0; loop < loopCount; loop++)
                    for (int ix = (int) ithread; ix < args.Length; ix += nthreads)
                        task(args[ix]);
                });

            int threadIx = 0;
            foreach (Thread t in threads)
                t.Start(threadIx++);

            while (Interlocked.CompareExchange(ref ready[0], 0, 0) < nthreads)
                Thread.Sleep(0);

            start.Set();

            foreach (Thread t in threads)
                t.Join();

            Trace.TraceInformation("Execution time: {0}", timer.Elapsed);
        }

        private struct TestValue
        {
            public Guid Id { get; set; }
            public int Count { get; set; }
        }

        private static readonly Random random = new Random();
        private static int iCounter = 0x01010101;

        public static Guid NextHashCollision(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // Modify bytes 8 & 9 with random number
            Array.Copy(
                BitConverter.GetBytes((short) random.Next()),
                0,
                bytes,
                8,
                2
            );

            // Increment bytes 11, 12, 13, & 14
            Array.Copy(
                BitConverter.GetBytes(
                    BitConverter.ToInt32(bytes, 11) +
                    Interlocked.Increment(ref iCounter)
                ),
                0,
                bytes,
                11,
                4
            );

            Guid result = new Guid(bytes);
            Assert.AreEqual(guid.GetHashCode(), result.GetHashCode());
            return result;
        }

        public static Guid[] CreateSample(Guid seed, int size, double collisions)
        {
            Guid[] sample = new Guid[size];
            int count = 0, collis = 0, uix = 0;
            for (int i = 0; i < size; i++)
                if (collis >= count * collisions)
                {
                    sample[uix = i] = Guid.NewGuid();
                    count++;
                }
                else
                {
                    sample[i] = NextHashCollision(sample[uix]);
                    collis++;
                }
            return sample;
        }

        [Test]
        public void CompareTest()
        {
            const int size = 1000000;
            int reps = 3;
            Stopwatch timer;

            IDictionary<Guid, TestValue> dict =
                new SynchronizedDictionary<Guid, TestValue>(new Dictionary<Guid, TestValue>(size));
            IDictionary<Guid, TestValue> test = new LurchTable<Guid, TestValue>(size);

            for (int rep = 0; rep < reps; rep++)
            {
                Guid[] sample = CreateSample(Guid.NewGuid(), size, 1);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => dict.Add(item, new TestValue {Id = item, Count = rep}));
                Trace.TraceInformation("Dict Add: {0}", timer.Elapsed);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => test.Add(item, new TestValue {Id = item, Count = rep}));
                Trace.TraceInformation("Test Add: {0}", timer.Elapsed);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => dict[item] = new TestValue {Id = item, Count = rep});
                Trace.TraceInformation("Dict Update: {0}", timer.Elapsed);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => test[item] = new TestValue {Id = item, Count = rep});
                Trace.TraceInformation("Test Update: {0}", timer.Elapsed);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => dict.Remove(item));
                Trace.TraceInformation("Dict Rem: {0}", timer.Elapsed);
                Assert.AreEqual(0, dict.Count);

                timer = Stopwatch.StartNew();
                Parallel(1, sample, item => test.Remove(item));
                Trace.TraceInformation("Test Rem: {0}", timer.Elapsed);

                test.Clear();
                dict.Clear();

                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
        }

        [Test]
        public void TestDelete()
        {
            LurchTable<Guid, bool> Map = CreateMap<bool>();
            Guid[] ids = CreateSample(Guid.NewGuid(), COUNT, 4);
            foreach (Guid id in ids)
                Assert.IsTrue(Map.TryAdd(id, true));

            bool test;
            Parallel(1, ids, id => { Assert.IsTrue(Map.Remove(id)); });

            Assert.AreEqual(0, Map.Count);
            foreach (Guid id in ids)
                Assert.IsTrue(!Map.TryGetValue(id, out test));
        }

        [Test]
        public void TestGuidHashCollision()
        {
            Guid id1 = Guid.NewGuid();
            Guid id2 = NextHashCollision(id1);

            Assert.AreNotEqual(id1, id2);
            Assert.AreEqual(id1.GetHashCode(), id2.GetHashCode());
        }

        [Test]
        public void TestInsert()
        {
            LurchTable<Guid, bool> Map = CreateMap<bool>();
            Guid[] ids = CreateSample(Guid.NewGuid(), COUNT, 4);

            bool test;
            Parallel(1, ids, id => { Assert.IsTrue(Map.TryAdd(id, true)); });

            Assert.AreEqual(ids.Length, Map.Count);
            foreach (Guid id in ids)
                Assert.IsTrue(Map.TryGetValue(id, out test) && test);
        }

        [Test]
        public void TestInsertDelete()
        {
            LurchTable<Guid, bool> Map = CreateMap<bool>();
            Guid[] ids = CreateSample(Guid.NewGuid(), COUNT, 4);

            bool test;
            Parallel(100, ids, id =>
            {
                Assert.IsTrue(Map.TryAdd(id, true));
                Assert.IsTrue(Map.Remove(id));
            });

            Assert.AreEqual(0, Map.Count);
            foreach (Guid id in ids)
                Assert.IsTrue(!Map.TryGetValue(id, out test));
        }

        [Test]
        public void TestInsertUpdateDelete()
        {
            LurchTable<Guid, bool> Map = CreateMap<bool>();
            Guid[] ids = CreateSample(Guid.NewGuid(), COUNT, 4);

            bool test;
            Parallel(100, ids, id =>
            {
                Assert.IsTrue(Map.TryAdd(id, true));
                Assert.IsTrue(Map.TryUpdate(id, false, true));
                Assert.IsTrue(Map.Remove(id));
            });

            Assert.AreEqual(0, Map.Count);
            foreach (Guid id in ids)
                Assert.IsTrue(!Map.TryGetValue(id, out test));
        }

        [Test]
        public void TestLimitedInsert()
        {
            LurchTable<Guid, bool> Map = new LurchTable<Guid, bool>(LurchTableOrder.Access, 1000);
            Guid[] ids = CreateSample(Guid.NewGuid(), 1000000, 0);

            Parallel(1, ids,
                id =>
                {
                    bool test;
                    Assert.IsTrue(Map.TryAdd(id, true));
                    Map.TryGetValue(id, out test);
                });

            Assert.AreEqual(1000, Map.Count);
        }
    }
}