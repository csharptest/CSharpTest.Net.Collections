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
using System.Collections.Generic;
using NUnit.Framework;
using CSharpTest.Net.Collections;

#pragma warning disable 1591
namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public partial class TestDisposingList
    {
        static readonly List<IDisposable> disposeOrder = new List<IDisposable>();
        class DisposeInOrder : IDisposable
        {
            public void Dispose()
            {
                disposeOrder.Add(this);
            }
        }

        [Test]
        public void TestNonGeneric()
        {
            disposeOrder.Clear();
            DisposingList list = new DisposingList();

            DisposeInOrder a = new DisposeInOrder();
            DisposeInOrder b = new DisposeInOrder();

            list.Add(a);
            list.Add(b);
            list.Add(null);
            list.Dispose();

            //Removed from list?
            Assert.AreEqual(0, list.Count);
            //All were disposed?
            Assert.AreEqual(2, disposeOrder.Count);
            //Disposed in reverse order of creation?
            Assert.IsTrue(object.ReferenceEquals(b, disposeOrder[0]));
            Assert.IsTrue(object.ReferenceEquals(a, disposeOrder[1]));

            Assert.AreEqual(2, new DisposingList(new IDisposable[] { a, b }).Count);
            Assert.AreEqual(0, new DisposingList(5).Count);
        }

        [Test]
        public void TestGeneric()
        {
            disposeOrder.Clear();
            DisposingList<DisposeInOrder> list = new DisposingList<DisposeInOrder>();

            DisposeInOrder a = new DisposeInOrder();
            DisposeInOrder b = new DisposeInOrder();

            list.Add(a);
            list.Add(b);
            list.Add(null);
            list.Dispose();

            //Removed from list?
            Assert.AreEqual(0, list.Count);
            //All were disposed?
            Assert.AreEqual(2, disposeOrder.Count);
            //Disposed in reverse order of creation?
            Assert.IsTrue(object.ReferenceEquals(b, disposeOrder[0]));
            Assert.IsTrue(object.ReferenceEquals(a, disposeOrder[1]));

            Assert.AreEqual(2, new DisposingList<DisposeInOrder>(new DisposeInOrder[] { a, b }).Count);
            Assert.AreEqual(0, new DisposingList<DisposeInOrder>(5).Count);
        }
    }
}
