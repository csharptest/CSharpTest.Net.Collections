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
using System.Diagnostics;
using System.Threading;
using CSharpTest.Net.Collections.Test.Threading;
using CSharpTest.Net.Synchronization;
using Xunit;

namespace CSharpTest.Net.Collections.Test.LockingTests
{
    public abstract class BaseThreadedReaderWriterTest<TFactory> : BaseThreadedWriterTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        [Fact]
        public void TestReaderBlocksWriter()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();

                using (new ThreadedReader(l))
                {
                    Assert.False(l.TryWrite(0));
                }

                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Fact]
        public void TestWriterBlocksReader()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryRead(0));
                l.ReleaseRead();

                using (new ThreadedWriter(l))
                {
                    Assert.False(l.TryRead(0));
                }

                Assert.True(l.TryRead(0));
                l.ReleaseRead();
            }
        }

        [Fact]
        public void TestWriterBlocksWriter()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();

                using (new ThreadedWriter(l))
                {
                    Assert.False(l.TryWrite(0));
                }

                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Fact]
        public virtual void TestReaderAllowsReader()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (new ThreadedReader(l))
                {
                    Assert.True(l.TryRead(0));
                }

                l.ReleaseRead();
            }
        }

        [Fact]
        public virtual void TestWriteToReadRecursion()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write())
                using (l.Read())
                using (l.Read())
                {
                }
            }
        }

        [Fact]
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
                {
                }
            }
        }

        [Fact]
        public void FullContentionTest()
        {
            ManualResetEvent stop = new ManualResetEvent(false);
            SynchronizedDictionary<int, int> data = new SynchronizedDictionary<int, int>();
            int[] iterations = new int[1];
            int errors = 0;
            using (WorkQueue worker = new WorkQueue(10))
            {
                worker.OnError += delegate { errors++; };
                for (int job = 0; job < 10; job++)
                    worker.Enqueue(
                        delegate
                        {
                            Random r = new Random();
                            while (!stop.WaitOne(0))
                            {
                                Interlocked.Increment(ref iterations[0]);
                                for (int i = 0; i < 100; i++)
                                {
                                    int val;
                                    int tmp = r.Next(100);
                                    if (i % 5 == 0)
                                        data[tmp] = tmp;
                                    else if (data.TryGetValue(tmp, out val))
                                        Assert.Equal(tmp, val);
                                }
                            }
                        }
                    );

                Thread.Sleep(100);
                stop.Set();
                worker.Complete(true, 100);
            }

            Assert.Equal(0, errors);
            Trace.TraceInformation("Iterations completed: {0} * 100", iterations[0]);
        }

        [Fact]
        public void TestThreadedReadTimeout()
        {
            Assert.Throws<TimeoutException>(() =>
            {
                using (ILockStrategy l = LockFactory.Create())
                {
                    using (new ThreadedWriter(l))
                    using (l.Read(0))
                    {
                    }
                }
            });
        }

        [Fact]
        public void TestExcessiveReleaseWrite()
        {
            Assert.Throws<SynchronizationLockException>(() =>
            {
                using (ILockStrategy l = LockFactory.Create())
                {
                    l.ReleaseWrite();
                }
            });
        }

        [Fact]
        public void TestExcessiveReleaseRead()
        {
            Assert.Throws<SynchronizationLockException>(() =>
            {
            using (ILockStrategy l = LockFactory.Create())
            {
                l.ReleaseRead();
            }
            });
        }
    }
}