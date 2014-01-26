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
using CSharpTest.Net.Utils;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestObjectKeepAlive
    {
        static bool _destroyed;
        class MyObject
        {
            ~MyObject()
            {
                _destroyed = true;
            }
        }

        [Test]
        public void TestKeepAliveMax()
        {
            WeakReference r;
            ObjectKeepAlive keep = new ObjectKeepAlive(25, 50, TimeSpan.FromSeconds(1));

            if(true)
            {
                object target = new MyObject();
                r = new WeakReference(target);
                keep.Add(target);
                target = null;
            }

            _destroyed = false;
            Assert.IsTrue(r.IsAlive);

            for (int i = 0; i < 49; i++)
                keep.Add(i);

            Assert.IsTrue(r.IsAlive);

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsTrue(r.IsAlive);
            Assert.IsFalse(_destroyed);

            keep.Add(new object());
            keep.Add(new object());

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsFalse(r.IsAlive);
            Assert.IsTrue(_destroyed);
        }

        [Test]
        public void TestKeepAliveMin()
        {
            WeakReference r;
            ObjectKeepAlive keep = new ObjectKeepAlive(1, 10, TimeSpan.FromTicks(1), true);

            for (int i = 0; i < 35; i++)
                keep.Add(i);

            if (true)
            {
                object target = new MyObject();
                r = new WeakReference(target);
                keep.Add(target);
                target = null;
            }

            _destroyed = false;
            Assert.IsTrue(r.IsAlive);

            for (int i = 0; i < 100; i++)
                keep.Tick();

            Assert.IsTrue(r.IsAlive);

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsTrue(r.IsAlive);
            Assert.IsFalse(_destroyed);

            System.Threading.Thread.Sleep(1);

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsTrue(r.IsAlive);
            Assert.IsFalse(_destroyed);

            keep.Clear();

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsFalse(r.IsAlive);
            Assert.IsTrue(_destroyed);
        }

        [Test]
        public void TestKeepAliveTime()
        {
            WeakReference r;
            TimeSpan timeout = TimeSpan.FromMilliseconds(100);
            ObjectKeepAlive keep = new ObjectKeepAlive(1, 10, timeout, false);

            if (true)
            {
                object target = new MyObject();
                r = new WeakReference(target);
                keep.Add(target);
                target = null;
            }

            _destroyed = false;
            Assert.IsTrue(r.IsAlive);

            for (int i = 0; i < 5; i++)
                keep.Add(i);

            Assert.IsTrue(r.IsAlive);

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsTrue(r.IsAlive);
            Assert.IsFalse(_destroyed);

            long start = DateTime.UtcNow.Ticks;
            while ((start + timeout.Ticks) > DateTime.UtcNow.Ticks)
                System.Threading.Thread.SpinWait(100);

            //Time has elapsed, yet it nothing is added, and Tick() is not called, it remains in memory
            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsTrue(r.IsAlive);
            Assert.IsFalse(_destroyed);

            //Once the collection is touched with either a call to Add or Tick, timeout will expire
            keep.Add(new object());

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();
            Assert.IsFalse(r.IsAlive);
            Assert.IsTrue(_destroyed);
        }
        [Test]
        public void TestTruncateLarge()
        {
            TimeSpan timeout = TimeSpan.FromMilliseconds(25);
            ObjectKeepAlive keep = new ObjectKeepAlive(100, 10000, timeout);

            for (int i = 0; i < 100000; i++)
                keep.Add(i);

            System.Threading.Thread.Sleep(timeout);

            for (int i = 0; i < 10000; i++)
                keep.Add(i);
        }
    }
}
