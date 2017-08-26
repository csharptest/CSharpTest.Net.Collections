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

    public class TestLockingStructs
    {
        protected readonly ILockFactory LockFactory = new LockFactory<SimpleReadWriteLocking>();

        [Fact]
        public void TestIdiotReaderUsesDispose()
        {
            Assert.Throws<SynchronizationLockException>(() =>
            {
                using (ILockStrategy l = LockFactory.Create())
                using (ReadLock r = new ReadLock(l, 0))
                {
                    Assert.True(r.HasReadLock);
                    ((IDisposable)r)
                        .Dispose(); //You cannot do this, the using statement has a 'copy' of ReadLock, don't call dispose.
                }
            });
        }

        [Fact]
        public void TestIdiotUsesSafeLockDispose()
        {
            Assert.Throws<SynchronizationLockException>(() =>
            {
                object instance = new object();
                using (SafeLock safeLock = new SafeLock(instance, 0))
                {
                    ((IDisposable)safeLock)
                        .Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
                }
            });
        }

        [Fact]
        public void TestIdiotWriterUsesDispose()
        {
            Assert.Throws<SynchronizationLockException>(() =>
            {
                using (ILockStrategy l = LockFactory.Create())
                using (WriteLock w = new WriteLock(l, 0))
                {
                    Assert.True(w.HasWriteLock);
                    ((IDisposable)w).Dispose(); //You cannot do this, the using statement has a 'copy' of ReadLock, don't call dispose.
                }
            });
        }

        [Fact]
        public void TestReadLockSuccess()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (ReadLock r = new ReadLock(l, 0))
            {
                Assert.True(r.HasReadLock);
            }
        }

        [Fact]
        public void TestReadLockTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (new ThreadedWriter(l))
            using (ReadLock r = new ReadLock(l, 0))
            {
                Assert.False(r.HasReadLock);
            }
        }

        [Fact]
        public void TestSafeLockSuccess()
        {
            object instance = new object();
            using (new SafeLock(instance))
            {
            }
        }

        [Fact]
        public void TestSafeLockSuccessWithTException()
        {
            object instance = new object();
            using (new SafeLock<InvalidOperationException>(instance))
            {
            }
        }

        [Fact]
        public void TestSafeLockTimeout()
        {
            Assert.Throws<TimeoutException>(() =>
            {
                object instance = new object();
                using (new ThreadedWriter(new SimpleReadWriteLocking(instance)))
                using (new SafeLock(instance, 0))
                {
                    Assert.True(false);
                }
            });
        }

        [Fact]
        public void TestSafeLockTimeoutWithTException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {

                object instance = new object();
                using (new ThreadedWriter(new SimpleReadWriteLocking(instance)))
                using (new SafeLock<ArgumentOutOfRangeException>(instance, 0))
                {
                    Assert.True(false);
                }
            });
        }

        [Fact]
        public void TestWriteLockSuccess()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (WriteLock w = new WriteLock(l, 0))
            {
                Assert.True(w.HasWriteLock);
            }
        }

        [Fact]
        public void TestWriteLockTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (new ThreadedWriter(l))
            using (WriteLock w = new WriteLock(l, 0))
            {
                Assert.False(w.HasWriteLock);
            }
        }

        [Fact]
        public void TestYouCanDisposeReadLock()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (IDisposable r = ReadLock.Acquire(l, 0))
            {
                r.Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
            }
        }

        [Fact]
        public void TestYouCanDisposeSafeLock()
        {
            object instance = new object();
            using (IDisposable safeLock = new SafeLock(instance, 0))
            {
                safeLock.Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
            }
        }

        [Fact]
        public void TestYouCanDisposeWriteLock()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (IDisposable w = l.Write())
            {
                w.Dispose(); //since the using statement has the same boxed pointer to w, we are allowed to dispose
            }
        }
    }
}