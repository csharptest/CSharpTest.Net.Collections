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
using System.Collections;
using System.Collections.Generic;
using CSharpTest.Net.Collections;
using NUnit.Framework;

#pragma warning disable 1591

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestOrdinalList
    {
        [TestFixtureSetUp]
        public virtual void Setup()
        {
        }

        [TestFixtureTearDown]
        public virtual void Teardown()
        {
        }

        [Test]
        public void TestBasics()
        {
            OrdinalList list = new OrdinalList();
            Assert.IsFalse(list.IsReadOnly);
            list.Ceiling = 0;

            for (int i = 512; i >= 0; i--)
                list.Add(i);

            int offset = 0;
            foreach (int item in list)
                Assert.AreEqual(offset++, item);

            Assert.AreEqual(513, offset);
            Assert.AreEqual(513, list.Count);
            Assert.AreEqual(519, list.Ceiling);

            list.Clear();
            list.AddRange(new[] {5, 10, 20});
            list.AddRange(new int[] { });

            Assert.AreEqual(3, list.Count);
            Assert.AreEqual(23, list.Ceiling);

            Assert.IsTrue(list.Contains(20));
            Assert.IsTrue(list.Remove(20));

            Assert.IsFalse(list.Contains(20));
            Assert.IsFalse(list.Remove(20));

            Assert.AreEqual(2, list.Count);

            int[] items = new int[2];
            list.CopyTo(items, 0);
            Assert.AreEqual(5, items[0]);
            Assert.AreEqual(10, items[1]);

            items = list.ToArray();
            Assert.AreEqual(5, items[0]);
            Assert.AreEqual(10, items[1]);

            byte[] bits = list.ToByteArray();
            Assert.AreEqual(3, bits.Length);
            Assert.AreEqual(2, new OrdinalList(bits).Count);

            List<int> tmp = new List<int>();
            foreach (int i in list)
                tmp.Add(i);
            Assert.AreEqual(2, tmp.Count);
            Assert.AreEqual(5, tmp[0]);
            Assert.AreEqual(10, tmp[1]);
        }

        [Test]
        public void TestClone()
        {
            OrdinalList lista = new OrdinalList(new[] {0});
            OrdinalList listb = (OrdinalList) ((ICloneable) lista).Clone();
            Assert.IsFalse(ReferenceEquals(lista, listb));
            Assert.AreEqual(lista.Count, listb.Count);
            Assert.IsTrue(listb.Contains(0));

            listb.Add(1);
            Assert.IsTrue(listb.Contains(1));
            Assert.IsFalse(lista.Contains(1));
        }

        [Test]
        public void TestICollection()
        {
            OrdinalList list = new OrdinalList();
            list.AddRange(new[] {5, 10, 20});

            ICollection coll = list;
            Assert.IsFalse(coll.IsSynchronized);
            Assert.IsTrue(ReferenceEquals(coll, coll.SyncRoot));

            int[] copy = new int[3];
            coll.CopyTo(copy, 0);
            Assert.AreEqual(5, copy[0]);
            Assert.AreEqual(10, copy[1]);
            Assert.AreEqual(20, copy[2]);

            byte[] bits = new byte[3];
            coll.CopyTo(bits, 0);
            Assert.AreEqual(32, bits[0]);
            Assert.AreEqual(4, bits[1]);
            Assert.AreEqual(16, bits[2]);

            List<int> tmp = new List<int>();
            foreach (int i in coll)
                tmp.Add(i);
            Assert.AreEqual(3, tmp.Count);
            Assert.AreEqual(5, tmp[0]);
            Assert.AreEqual(10, tmp[1]);
            Assert.AreEqual(20, tmp[2]);
        }

        [Test]
        public void TestIntersectUnion()
        {
            OrdinalList lista = new OrdinalList(new[] {5, 10, 99});
            OrdinalList listb = new OrdinalList(new[] {2, 4, 6, 8, 10});

            OrdinalList union = lista.UnionWith(listb);
            Assert.AreEqual(7, union.Count);
            foreach (int i in union)
                Assert.IsTrue(lista.Contains(i) || listb.Contains(i));

            OrdinalList inter = lista.IntersectWith(listb);
            Assert.AreEqual(1, inter.Count);
            foreach (int i in inter)
                Assert.AreEqual(10, i);
        }

        [Test]
        public void TestIntersectUnionSameLength()
        {
            OrdinalList lista = new OrdinalList(new[] {1, 4, 5});
            OrdinalList listb = new OrdinalList(new[] {2, 4, 6});

            OrdinalList union = lista.UnionWith(listb);
            Assert.AreEqual(5, union.Count);
            foreach (int i in union)
                Assert.IsTrue(lista.Contains(i) || listb.Contains(i));

            OrdinalList inter = lista.IntersectWith(listb);
            Assert.AreEqual(1, inter.Count);
            foreach (int i in inter)
                Assert.AreEqual(4, i);
        }

        [Test]
        public void TestInvert()
        {
            OrdinalList lista = new OrdinalList(new[] {0, 2, 4, 6, 8, 10, 12});
            OrdinalList listb = new OrdinalList(new[] {1, 3, 5, 7, 9, 11, 13});

            OrdinalList invta = lista.Invert(13);
            string invtatext = "", listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.AreEqual(listbtext, invtatext);

            lista = new OrdinalList(new[] {0});
            listb = new OrdinalList(new[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13});
            invta = lista.Invert(13);
            invtatext = "";
            listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.AreEqual(listbtext, invtatext);

            lista = new OrdinalList(new[] {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13});
            listb = new OrdinalList(new[] {3});
            invta = lista.Invert(4);
            invtatext = "";
            listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.AreEqual(listbtext, invtatext);
        }

        /// <summary>
        ///     Previously it was thought to be invalid to set a negative ceiling; however, since ceiling
        ///     is an 'inclusive' value, the value of '0' actually requires 1 byte to be allocated.  In
        ///     order to allow explicitly clearing the array length, a -1 must be allowed;
        /// </summary>
        [Test]
        public void TestNegativeCeiling()
        {
            OrdinalList list = new OrdinalList();
            list.Ceiling = -1;
            Assert.AreEqual(-1, list.Ceiling);
        }
    }

    [TestFixture]
    public class TestOrdinalListNegative
    {
        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBadArrayType()
        {
            ICollection list = new OrdinalList();
            list.CopyTo(new ulong[3], 0);
        }

        /* '-1' is now allowed * see comments on TestNegativeCeiling */
        [Test]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestBadCeiling()
        {
            OrdinalList list = new OrdinalList();
            list.Ceiling = -2;
        }
    }
}