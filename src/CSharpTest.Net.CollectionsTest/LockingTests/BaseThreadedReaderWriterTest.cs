#region Copyright 2011-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Threading;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Synchronization;
using CSharpTest.Net.Threading;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test.LockingTests
{
    public class BaseThreadedReaderWriterTest<TFactory> : BaseThreadedWriterTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        [Test]
        public void TestReaderBlocksWriter()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();

                using (new ThreadedReader(l))
                    Assert.IsFalse(l.TryWrite(0));

                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }
        [Test]
        public void TestWriterBlocksReader()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryRead(0));
                l.ReleaseRead();

                using (new ThreadedWriter(l))
                    Assert.IsFalse(l.TryRead(0));

                Assert.IsTrue(l.TryRead(0));
                l.ReleaseRead();
            }
        }
        [Test]
        public void TestWriterBlocksWriter()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();

                using (new ThreadedWriter(l))
                    Assert.IsFalse(l.TryWrite(0));

                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }
        [Test]
        public virtual void TestReaderAllowsReader()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (new ThreadedReader(l))
                    Assert.IsTrue(l.TryRead(0));

                l.ReleaseRead();
            }
        }
        [Test]
        public virtual void TestWriteToReadRecursion()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write())
                using (l.Read())
                using (l.Read())
                { }
            }
        }
        [Test]
        public virtual void TestWriteToWriteRecursion()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write())
                using (l.Write())
                using (l.Read())
                using (l.Read())
                using (l.Write())
                using (l.Read())
                using (l.Write())
                using (l.Write())
                using (l.Write())
                using (l.Read())
                { }
            }
        }
        [Test]
        public void FullContentionTest()
        {
            ManualResetEvent stop = new ManualResetEvent(false);
            SynchronizedDictionary<int, int> data = new SynchronizedDictionary<int, int>();
            int[] iterations = new int[1];
            int errors = 0;
            using (WorkQueue worker = new WorkQueue(10))
            {
                worker.OnError += delegate(object o, ErrorEventArgs e) { errors++; };
                for (int job = 0; job < 10; job++)
                {
                    worker.Enqueue(
                        delegate()
                            {
                                Random r = new Random();
                                while (!stop.WaitOne(0, false))
                                {
                                    Interlocked.Increment(ref iterations[0]);
                                    for (int i = 0; i < 100; i++)
                                    {
                                        int val;
                                        int tmp = r.Next(100);
                                        if (i%5 == 0)
                                            data[tmp] = tmp;
                                        else if (data.TryGetValue(tmp, out val))
                                            Assert.AreEqual(tmp, val);
                                    }
                                }
                            }
                        );
                }

                Thread.Sleep(100);
                stop.Set();
                worker.Complete(true, 100);
            }

            Assert.AreEqual(0, errors);
            System.Diagnostics.Trace.TraceInformation("Iterations completed: {0} * 100", iterations[0]);
        }
        [Test, ExpectedException(typeof(TimeoutException))]
        public void TestThreadedReadTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (new ThreadedWriter(l))
                using (l.Read(0))
                { }
            }
        }
        [Test, ExpectedException]
        public void TestExcessiveReleaseWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
                l.ReleaseWrite();
        }
        [Test, ExpectedException]
        public void TestExcessiveReleaseRead()
        {
            using (ILockStrategy l = LockFactory.Create())
                l.ReleaseRead();
        }
    }
}
