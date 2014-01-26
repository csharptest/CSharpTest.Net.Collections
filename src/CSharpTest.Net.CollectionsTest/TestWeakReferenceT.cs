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
    public class TestWeakReferenceT
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
        public void TestDestoryed()
        {
            Utils.WeakReference<MyObject> r;
            if (true)
            {
                MyObject obj = new MyObject();

                r = new Utils.WeakReference<MyObject>(obj);
                Assert.IsTrue(r.IsAlive);
                Assert.IsNotNull(r.Target);
                MyObject test;
                Assert.IsTrue(r.TryGetTarget(out test));
                Assert.IsTrue(ReferenceEquals(obj, test));
                test = null;
                _destroyed = false;

                GC.KeepAlive(obj);
                obj = null;
            }

            GC.GetTotalMemory(true);
            GC.WaitForPendingFinalizers();

            Assert.IsTrue(_destroyed);

            MyObject tmp;
            Assert.IsFalse(r.IsAlive);
            Assert.IsNull(r.Target);
            Assert.IsFalse(r.TryGetTarget(out tmp));
        }

        [Test]
        public void TestReplaceTarget()
        {
            string value1 = "Testing Value - 1";
            string value2 = "Testing Value - 2";
            Utils.WeakReference<string> r = new Utils.WeakReference<string>(value1);

            string tmp;
            Assert.IsTrue(r.TryGetTarget(out tmp) && tmp == value1);

            r.Target = value2;
            Assert.IsTrue(r.TryGetTarget(out tmp) && tmp == value2);
        }

        [Test]
        public void TestReplaceBadTypeTarget()
        {
            string value1 = "Testing Value - 1";
            object value2 = new MyObject();
            Utils.WeakReference<string> r = new Utils.WeakReference<string>(value1);

            string tmp;
            Assert.IsTrue(r.TryGetTarget(out tmp) && tmp == value1);

            ((WeakReference)r).Target = value2; //incorrect type...
            Assert.IsFalse(r.IsAlive);
            Assert.IsNull(r.Target);
            Assert.IsFalse(r.TryGetTarget(out tmp));

            Assert.IsTrue(ReferenceEquals(value2, ((WeakReference)r).Target));
        }
    }
}
