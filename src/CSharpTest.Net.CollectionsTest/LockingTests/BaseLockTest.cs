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
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test.LockingTests
{
    public class BaseLockTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        protected readonly TFactory LockFactory = new TFactory();

        [Test]
        public void TestTryRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryRead(0));
                l.ReleaseRead();
            }
        }

        [Test]
        public void TestTryWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Test]
        public void TestTryReadThenTryWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryRead(0));
                l.ReleaseRead();

                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Test]
        public void TestTryWriteThenTryRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.IsTrue(l.TryRead(0));
                l.ReleaseRead();

                Assert.IsTrue(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Test]
        public void TestRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Read())
            { }
        }

        [Test]
        public void TestWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Write())
            { }
        }

        [Test]
        public void TestReadThenWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Read())
                { }
                using (l.Write())
                { }
            }
        }

        [Test]
        public void TestWriteThenRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write())
                { }
                using (l.Read())
                { }
            }
        }

        [Test]
        public void TestReadWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Read(0))
            { }
        }

        [Test]
        public void TestWriteWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Write(0))
            { }
        }

        [Test]
        public void TestReadThenWriteWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Read(0))
                { }
                using (l.Write(0))
                { }
            }
        }

        [Test]
        public void TestWriteThenReadWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write(0))
                { }
                using (l.Read(0))
                { }
            }
        }
    }
}
