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
using Xunit;

#pragma warning disable 1591
namespace CSharpTest.Net.Collections.Test
{
    
    public class TestDisposingList
    {
        private static readonly List<IDisposable> disposeOrder = new List<IDisposable>();

        private class DisposeInOrder : IDisposable
        {
            public void Dispose()
            {
                disposeOrder.Add(this);
            }
        }

        [Fact]
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
            Assert.Equal(0, list.Count);
            //All were disposed?
            Assert.Equal(2, disposeOrder.Count);
            //Disposed in reverse order of creation?
            Assert.True(ReferenceEquals(b, disposeOrder[0]));
            Assert.True(ReferenceEquals(a, disposeOrder[1]));

            Assert.Equal(2, new DisposingList<DisposeInOrder>(new[] {a, b}).Count);
            Assert.Equal(0, new DisposingList<DisposeInOrder>(5).Count);
        }

        [Fact]
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
            Assert.Equal(0, list.Count);
            //All were disposed?
            Assert.Equal(2, disposeOrder.Count);
            //Disposed in reverse order of creation?
            Assert.True(ReferenceEquals(b, disposeOrder[0]));
            Assert.True(ReferenceEquals(a, disposeOrder[1]));

            Assert.Equal(2, new DisposingList(new IDisposable[] {a, b}).Count);
            Assert.Equal(0, new DisposingList(5).Count);
        }
    }
}