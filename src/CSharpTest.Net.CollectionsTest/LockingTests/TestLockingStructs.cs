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
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test.LockingTests
{
    [TestFixture]
    public class TestLockingStructs
    {
        protected readonly ILockFactory LockFactory = new LockFactory<SimpleReadWriteLocking>();

        [Test]
        [ExpectedException(typeof(SynchronizationLockException))]
        public void TestIdiotReaderUsesDispose()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (ReadLock r = new ReadLock(l, 0))
            {
                Assert.IsTrue(r.HasReadLock);
                ((IDisposable) r)
                    .Dispose(); //You cannot do this, the using statement has a 'copy' of ReadLock, don't call dispose.
            }
        }

        [Test]
        [ExpectedException(typeof(SynchronizationLockException))]
        public void TestIdiotUsesSafeLockDispose()
        {
            object instance = new object();
            using (SafeLock safeLock = new SafeLock(instance, 0))
            {
                ((IDisposable) safeLock)
                    .Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
            }
        }

        [Test]
        [ExpectedException(typeof(SynchronizationLockException))]
        public void TestIdiotWriterUsesDispose()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (WriteLock w = new WriteLock(l, 0))
            {
                Assert.IsTrue(w.HasWriteLock);
                ((IDisposable) w)
                    .Dispose(); //You cannot do this, the using statement has a 'copy' of ReadLock, don't call dispose.
            }
        }

        [Test]
        public void TestReadLockSuccess()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (ReadLock r = new ReadLock(l, 0))
            {
                Assert.IsTrue(r.HasReadLock);
            }
        }

        [Test]
        public void TestReadLockTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (new ThreadedWriter(l))
            using (ReadLock r = new ReadLock(l, 0))
            {
                Assert.IsFalse(r.HasReadLock);
            }
        }

        [Test]
        public void TestSafeLockSuccess()
        {
            object instance = new object();
            using (new SafeLock(instance))
            {
            }
        }

        [Test]
        public void TestSafeLockSuccessWithTException()
        {
            object instance = new object();
            using (new SafeLock<InvalidOperationException>(instance))
            {
            }
        }

        [Test]
        [ExpectedException(typeof(TimeoutException))]
        public void TestSafeLockTimeout()
        {
            object instance = new object();
            using (new ThreadedWriter(new SimpleReadWriteLocking(instance)))
            using (new SafeLock(instance, 0))
            {
                Assert.Fail();
            }
        }

        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestSafeLockTimeoutWithTException()
        {
            object instance = new object();
            using (new ThreadedWriter(new SimpleReadWriteLocking(instance)))
            using (new SafeLock<ArgumentOutOfRangeException>(instance, 0))
            {
                Assert.Fail();
            }
        }

        [Test]
        public void TestWriteLockSuccess()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (WriteLock w = new WriteLock(l, 0))
            {
                Assert.IsTrue(w.HasWriteLock);
            }
        }

        [Test]
        public void TestWriteLockTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (new ThreadedWriter(l))
            using (WriteLock w = new WriteLock(l, 0))
            {
                Assert.IsFalse(w.HasWriteLock);
            }
        }

        [Test]
        public void TestYouCanDisposeReadLock()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (IDisposable r = ReadLock.Acquire(l, 0))
            {
                r.Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
            }
        }

        [Test]
        public void TestYouCanDisposeSafeLock()
        {
            object instance = new object();
            using (IDisposable safeLock = new SafeLock(instance, 0))
            {
                safeLock.Dispose(); //since the using statement has the same boxed pointer to r, we are allowed to dispose
            }
        }

        [Test]
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