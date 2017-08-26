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
using System.Threading;
using CSharpTest.Net.Synchronization;
using Xunit;

namespace CSharpTest.Net.Collections.Test.LockingTests
{

    public class TestSimpleReadWriteLocking : BaseThreadedReaderWriterTest<LockFactory<SimpleReadWriteLocking>>
    {
        [Fact]
        public void DisposedWithReaders()
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                ILockStrategy l = LockFactory.Create();
                ThreadedReader thread = new ThreadedReader(l);
                try
                {
                    l.Dispose();
                }
                finally
                {
                    try
                    {
                        thread.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            });
        }

        [Fact]
        public void DisposedWithWriters()
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                ILockStrategy l = LockFactory.Create();
                ThreadedWriter thread = new ThreadedWriter(l);
                try
                {
                    l.Dispose();
                }
                finally
                {
                    try
                    {
                        thread.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            });
        }

        [Fact]
        public void ExplicitSyncObject()
        {
            object obj = new object();
            ILockStrategy l = new SimpleReadWriteLocking(obj);
            using (new ThreadedWriter(l))
            {
                Assert.False(Monitor.TryEnter(obj, 0));
            }
            l.Dispose();
        }

        [Fact]
        public void ReadToWriteFails()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Read())
            {
                Assert.False(l.TryWrite(10));
            }
        }
    }
}