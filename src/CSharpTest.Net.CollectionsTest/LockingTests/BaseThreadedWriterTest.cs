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
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test.LockingTests
{
    public class BaseThreadedWriterTest<TFactory> : BaseLockTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        #region ThreadedReader/ThreadedWriter
        #endregion

        [Test]
        public void TestThreadedTryWrite()
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
        public virtual void TestWriteCounter()
        {
            ILockStrategy l = LockFactory.Create();

            int count = l.WriteVersion;

            Assert.IsTrue(l.TryRead(0));
            l.ReleaseRead();
            Assert.AreEqual(count, l.WriteVersion);

            Assert.IsTrue(l.TryWrite(0));
            l.ReleaseWrite();
            Assert.AreNotEqual(count, l.WriteVersion);
            count = l.WriteVersion;

            Assert.IsTrue(l.TryWrite(0));
            l.ReleaseWrite();
            Assert.AreNotEqual(count, l.WriteVersion);
        }

        [Test]
        public void TestWriteRecursion()
        {
            ILockStrategy l = LockFactory.Create();
            using (l.Write())
            using (l.Write())
            using (l.Write())
            { }
        }


        [Test, ExpectedException(typeof(TimeoutException))]
        public void TestThreadedWriteTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (new ThreadedWriter(l))
                using (l.Write(0)) { }
        }
    }
}
