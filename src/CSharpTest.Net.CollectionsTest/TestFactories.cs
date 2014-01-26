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
using NUnit.Framework;
using CSharpTest.Net.Interfaces;

#pragma warning disable 1591
namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public partial class TestFactories
    {
        private static int Loaded = 0;

        private class TestObject { }
        private class BadObject { public BadObject() { throw new Exception("BadObject"); } }
        private class TestLazy1
        {
            static TestLazy1() { Loaded |= 1; }
        }

        private class TestLazy2
        {
            static TestLazy2() { Loaded |= 2; }
        }

        [Test]
        public void TestSingletonIsLazyInstance()
        {
            Assert.AreEqual(0, Loaded & 1);
            if(true)
            {
                Action<int> Load1 = x => GC.KeepAlive(Singleton<TestLazy1>.Instance);
                Assert.AreEqual(0, Loaded & 1);
                Load1(0);
                Assert.AreEqual(1, Loaded & 1);
            }
        }

        [Test]
        public void TestSingletonIsLazyFactory()
        {
            Assert.AreEqual(0, Loaded & 2);
            if (true)
            {
                IFactory<TestLazy2> factory = Singleton<TestLazy2>.Factory;
                Assert.AreEqual(0, Loaded & 2);
                factory.Create();
                Assert.AreEqual(2, Loaded & 2);
            }
        }

        [Test]
        public void TestSingletonThatThrows()
        {
            try
            {
                GC.KeepAlive(Singleton<BadObject>.Instance);
                Assert.Fail();
            }
            catch(ApplicationException ae)
            {
                Assert.AreEqual("BadObject", ae.GetBaseException().Message);
            }
            try
            {
                Singleton<BadObject>.Factory.Create();
                Assert.Fail();
            }
            catch (ApplicationException ae)
            {
                Assert.AreEqual("BadObject", ae.GetBaseException().Message);
            }
        }

        [Test]
        public void TestNewFactory()
        {
            IFactory<TestObject> factory = new NewFactory<TestObject>();
            Assert.IsFalse(ReferenceEquals(factory.Create(), factory.Create()));
        }

        [Test]
        public void TestDelegateFactory()
        {
            TestObject obj = new TestObject();
            IFactory<TestObject> factory = new DelegateFactory<TestObject>(() => obj);
            Assert.IsTrue(ReferenceEquals(factory.Create(), factory.Create()));

            factory = new DelegateFactory<TestObject>(() => new TestObject());
            Assert.IsFalse(ReferenceEquals(factory.Create(), factory.Create()));
        }

        [Test]
        public void TestInstanceFactory()
        {
            TestObject obj = new TestObject();
            IFactory<TestObject> factory = new InstanceFactory<TestObject>(obj);
            Assert.IsTrue(ReferenceEquals(obj, factory.Create()));
            Assert.IsTrue(ReferenceEquals(factory.Create(), factory.Create()));
        }
    }
}
