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
using Xunit;

namespace CSharpTest.Net.Collections.Test.LockingTests
{
    public abstract class BaseLockTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        protected readonly TFactory LockFactory = new TFactory();

        [Fact]
        public void TestTryRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryRead(0));
                l.ReleaseRead();
            }
        }

        [Fact]
        public void TestTryWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Fact]
        public void TestTryReadThenTryWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryRead(0));
                l.ReleaseRead();

                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Fact]
        public void TestTryWriteThenTryRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.True(l.TryRead(0));
                l.ReleaseRead();

                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
            }
        }

        [Fact]
        public void TestRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Read())
            {
            }
        }

        [Fact]
        public void TestWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Write())
            {
            }
        }

        [Fact]
        public void TestReadThenWrite()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Read())
                {
                }
                using (l.Write())
                {
                }
            }
        }

        [Fact]
        public void TestWriteThenRead()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write())
                {
                }
                using (l.Read())
                {
                }
            }
        }

        [Fact]
        public void TestReadWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Read(0))
            {
            }
        }

        [Fact]
        public void TestWriteWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            using (l.Write(0))
            {
            }
        }

        [Fact]
        public void TestReadThenWriteWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Read(0))
                {
                }
                using (l.Write(0))
                {
                }
            }
        }

        [Fact]
        public void TestWriteThenReadWithTimeout()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                using (l.Write(0))
                {
                }
                using (l.Read(0))
                {
                }
            }
        }
    }
}