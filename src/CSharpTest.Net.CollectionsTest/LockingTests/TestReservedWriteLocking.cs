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
    public class TestReservedWriteLocking<TFactory> : BaseThreadedWriterTest<TFactory>
        where TFactory : ILockFactory, new()
    {
        [Fact]
        public override void TestWriteCounter()
        {
            using (ILockStrategy l = LockFactory.Create())
            {
                Assert.Equal(0, l.WriteVersion);

                Assert.True(l.TryRead(0));
                l.ReleaseRead();
                Assert.Equal(0, l.WriteVersion);

                Assert.True(l.TryWrite(0));
                Assert.Equal(0, l.WriteVersion);
                l.ReleaseWrite();
                Assert.Equal(0, l.WriteVersion);

                using (l.Write())
                {
                    Assert.Equal(0, l.WriteVersion);
                    Assert.True(l.TryWrite(0));
                    // Once a nested write lock is acquired the real lock is obtained.
                    Assert.Equal(1, l.WriteVersion);
                    l.ReleaseWrite();
                    Assert.Equal(1, l.WriteVersion);
                }

                Assert.True(l.TryWrite(0));
                l.ReleaseWrite();
                Assert.Equal(1, l.WriteVersion);
            }
        }
    }
}