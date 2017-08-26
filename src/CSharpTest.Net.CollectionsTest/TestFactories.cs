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
using CSharpTest.Net.Interfaces;
using Xunit;

#pragma warning disable 1591
namespace CSharpTest.Net.Collections.Test
{
    
    public class TestFactories
    {
        private static int Loaded;

        private class TestObject
        {
        }

        private class BadObject
        {
            public BadObject()
            {
                throw new Exception("BadObject");
            }
        }

        private class TestLazy1
        {
            static TestLazy1()
            {
                Loaded |= 1;
            }
        }

        private class TestLazy2
        {
            static TestLazy2()
            {
                Loaded |= 2;
            }
        }

        [Fact]
        public void TestDelegateFactory()
        {
            TestObject obj = new TestObject();
            IFactory<TestObject> factory = new DelegateFactory<TestObject>(() => obj);
            Assert.True(ReferenceEquals(factory.Create(), factory.Create()));

            factory = new DelegateFactory<TestObject>(() => new TestObject());
            Assert.False(ReferenceEquals(factory.Create(), factory.Create()));
        }

        [Fact]
        public void TestInstanceFactory()
        {
            TestObject obj = new TestObject();
            IFactory<TestObject> factory = new InstanceFactory<TestObject>(obj);
            Assert.True(ReferenceEquals(obj, factory.Create()));
            Assert.True(ReferenceEquals(factory.Create(), factory.Create()));
        }

        [Fact]
        public void TestNewFactory()
        {
            IFactory<TestObject> factory = new NewFactory<TestObject>();
            Assert.False(ReferenceEquals(factory.Create(), factory.Create()));
        }

        [Fact]
        public void TestSingletonIsLazyFactory()
        {
            Assert.Equal(0, Loaded & 2);
            if (true)
            {
                IFactory<TestLazy2> factory = Singleton<TestLazy2>.Factory;
                Assert.Equal(0, Loaded & 2);
                factory.Create();
                Assert.Equal(2, Loaded & 2);
            }
        }

        [Fact]
        public void TestSingletonIsLazyInstance()
        {
            Assert.Equal(0, Loaded & 1);
            if (true)
            {
                Action<int> Load1 = x => GC.KeepAlive(Singleton<TestLazy1>.Instance);
                Assert.Equal(0, Loaded & 1);
                Load1(0);
                Assert.Equal(1, Loaded & 1);
            }
        }

        [Fact]
        public void TestSingletonThatThrows()
        {
            try
            {
                GC.KeepAlive(Singleton<BadObject>.Instance);
                Assert.True(false);
            }
            catch (Exception ae)
            {
                string message = ae.GetBaseException().Message;
                Assert.Equal("BadObject", message);
            }
            try
            {
                Singleton<BadObject>.Factory.Create();
                Assert.True(false);
            }
            catch (Exception ae)
            {
                Assert.Equal("BadObject", ae.GetBaseException().Message);
            }
        }
    }
}