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
using System.IO;
using System.Threading;
using CSharpTest.Net.Threading;
using NUnit.Framework;
#if NET20
using Action = System.Threading.ThreadStart;
#endif

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestWorkQueue
    {
        [Test]
        public void TestSingleAction()
        {
            using (ManualResetEvent finished = new ManualResetEvent(false))
            using (WorkQueue worker = new WorkQueue(2))
            {
                worker.Enqueue(delegate() { finished.Set(); });
                Assert.IsTrue(finished.WaitOne(100, false));
            }
        }
        [Test]
        public void TestMultipleActionsComplete()
        {
            int[] count = new int[1];
            using (WorkQueue worker = new WorkQueue(Math.Max(2, Environment.ProcessorCount)))
            {
                for( int i=0; i < 1000; i++)
                worker.Enqueue(delegate() { Interlocked.Increment(ref count[0]); Thread.Sleep(1); });
                worker.Complete(true, -1);
            }
            Assert.AreEqual(1000, count[0]);
        }
        [Test]
        public void TestMultipleActionsIncomplete()
        {
            int[] count = new int[1];
            using (WorkQueue worker = new WorkQueue(Math.Max(2, Environment.ProcessorCount)))
            {
                ThreadStart a = delegate() { Interlocked.Increment(ref count[0]); Thread.Sleep(1); };
                for (int i = 0; i < 10000; i++)
                    worker.Enqueue(a);
            }
            Assert.AreNotEqual(0, count[0]);
            Assert.AreNotEqual(10000, count[0]);
        }
        [Test]
        public void TestSingleActionT()
        {
            using (ManualResetEvent finished = new ManualResetEvent(false))
            using (WorkQueue worker = new WorkQueue(2))
            {
                worker.Enqueue(delegate() { finished.Set(); });
                Assert.IsTrue(finished.WaitOne(100, false));
            }
        }
        [Test]
        public void TestMultipleActionTComplete()
        {
            int[] counters = new int[10];
            using (WorkQueue worker = new WorkQueue(Math.Max(2, Environment.ProcessorCount)))
            {
                for (int i = 0; i < 1000; i++)
                    worker.Enqueue(delegate(int offset) { Interlocked.Increment(ref counters[offset]); }, i % 10);
                worker.Complete(true, -1);
            }
            foreach (int counter in counters)
                Assert.AreEqual(100, counter);

        }
        [Test]
        public void TestExceptionHandled()
        {
            Exception error = null;
            using (WorkQueue worker = new WorkQueue(1))
            {
                worker.OnError += delegate(object o, ErrorEventArgs e) { error = e.GetException(); };
                worker.Enqueue(delegate() { throw new ArgumentException("Handled?"); });
                worker.Complete(true, -1);
            }
            Assert.IsTrue(error is ArgumentException);
            Assert.AreEqual("Handled?", error.Message);
        }

        [Test]
        public void TestThreadAborts()
        {
            int[] counter = new int[1];
            using (WorkQueue<int[]> worker = new WorkQueue<int[]>(ProcessOne, Math.Max(2, Environment.ProcessorCount)))
            {
                for (int i = 0; i < 1000; i++)
                    worker.Enqueue(counter);
                worker.Complete(false, 10);
            }
            Assert.AreNotEqual(0, counter[0]);
        }

        private static void ProcessOne(int[] counters)
        {
            Interlocked.Increment(ref counters[0]);
            Thread.Sleep(100);
            Interlocked.Decrement(ref counters[0]);
        }

        [Test, ExpectedException(typeof(ObjectDisposedException))]
        public void TestEnqueueAfterDispose()
        {
            int counter = 0;
            WorkQueue worker = new WorkQueue(1);
            worker.Complete(false, 100);
            worker.Enqueue(delegate() { counter++; });
            Assert.Fail("Enqueue after Dispose()", counter);
        }
    }
}
