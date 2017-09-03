#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using CSharpTest.Net.Collections.Test.Bases;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    
    public class TestDisposable
    {
        private class MyDisposable : Disposable
        {
            public int _disposedCount;

            protected override void Dispose(bool disposing)
            {
                _disposedCount++;
            }

            public void TestAssert()
            {
                Assert();
            }
        }

        [Fact]
        public void TestAssertBeforeDispose()
        {
            MyDisposable o = new MyDisposable();
            o.TestAssert();
        }

        [Fact]
        public void TestAssertWhenDisposed()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                MyDisposable o = new MyDisposable();
                o.Dispose();
                o.TestAssert();
            });
        }

        [Fact]
        public void TestDisposedEvent()
        {
            MyDisposable o = new MyDisposable();
            bool disposed = false;
            o.Disposed += delegate { disposed = true; };
            o.Dispose();
            Assert.True(disposed, "Disposed event failed.");
        }

        [Fact]
        public void TestDisposedOnce()
        {
            MyDisposable o = new MyDisposable();
            using (o)
            {
                Assert.Equal(0, o._disposedCount);
                o.Dispose();
                Assert.Equal(1, o._disposedCount);
                o.Dispose();
                Assert.Equal(1, o._disposedCount);
            }
            Assert.Equal(1, o._disposedCount);
        }

        [Fact]
        public void TestDisposeOnFinalize()
        {
            MyDisposable o = new MyDisposable();
            bool disposed = false;
            o.Disposed += delegate { disposed = true; };

            o = null;
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.True(disposed, "Disposed event failed.");
        }

        [Fact]
        public void TestRemoveDisposedEvent()
        {
            MyDisposable o = new MyDisposable();
            bool disposed = false;
            EventHandler handler = delegate { disposed = true; };
            o.Disposed += handler;
            o.Disposed -= handler;
            o.Dispose();
            Assert.False(disposed, "Disposed fired?");
        }
    }
}