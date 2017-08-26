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
using CSharpTest.Net.Synchronization;
using Xunit;

namespace CSharpTest.Net.Collections.Test.LockingTests
{
    public abstract class BaseThreadedWriterTest<TFactory> : BaseLockTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        [Fact]
        public void TestThreadedTryWrite()
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
        public virtual void TestWriteCounter()
        {
            ILockStrategy l = LockFactory.Create();

            int count = l.WriteVersion;

            Assert.True(l.TryRead(0));
            l.ReleaseRead();
            Assert.Equal(count, l.WriteVersion);

            Assert.True(l.TryWrite(0));
            l.ReleaseWrite();
            Assert.NotEqual(count, l.WriteVersion);
            count = l.WriteVersion;

            Assert.True(l.TryWrite(0));
            l.ReleaseWrite();
            Assert.NotEqual(count, l.WriteVersion);
        }

        [Fact]
        public void TestWriteRecursion()
        {
            ILockStrategy l = LockFactory.Create();
            using (l.Write())
            using (l.Write())
            using (l.Write())
            {
            }
        }


        [Fact]
        public void TestThreadedWriteTimeout()
        {
            Assert.Throws<TimeoutException>(() =>
            {
                using (ILockStrategy l = LockFactory.Create())
                using (new ThreadedWriter(l))
                using (l.Write(0))
                {
                }
            });
        }

        #region ThreadedReader/ThreadedWriter

        #endregion
    }
}