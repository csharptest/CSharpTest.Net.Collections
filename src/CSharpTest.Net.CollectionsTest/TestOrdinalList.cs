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
using CSharpTest.Net.Interfaces;
using Xunit;

#pragma warning disable 1591

namespace CSharpTest.Net.Collections.Test
{

    public class TestOrdinalList
    {
        [Fact]
        public void TestBasics()
        {
            OrdinalList list = new OrdinalList();
            Assert.False(list.IsReadOnly);
            list.Ceiling = 0;

            for (int i = 512; i >= 0; i--)
                list.Add(i);

            int offset = 0;
            foreach (int item in list)
                Assert.Equal(offset++, item);

            Assert.Equal(513, offset);
            Assert.Equal(513, list.Count);
            Assert.Equal(519, list.Ceiling);

            list.Clear();
            list.AddRange(new[] { 5, 10, 20 });
            list.AddRange(new int[] { });

            Assert.Equal(3, list.Count);
            Assert.Equal(23, list.Ceiling);

            Assert.True(list.Contains(20));
            Assert.True(list.Remove(20));

            Assert.False(list.Contains(20));
            Assert.False(list.Remove(20));

            Assert.Equal(2, list.Count);

            int[] items = new int[2];
            list.CopyTo(items, 0);
            Assert.Equal(5, items[0]);
            Assert.Equal(10, items[1]);

            items = list.ToArray();
            Assert.Equal(5, items[0]);
            Assert.Equal(10, items[1]);

            byte[] bits = list.ToByteArray();
            Assert.Equal(3, bits.Length);
            Assert.Equal(2, new OrdinalList(bits).Count);

            List<int> tmp = new List<int>();
            foreach (int i in list)
                tmp.Add(i);
            Assert.Equal(2, tmp.Count);
            Assert.Equal(5, tmp[0]);
            Assert.Equal(10, tmp[1]);
        }

        [Fact]
        public void TestClone()
        {
            OrdinalList lista = new OrdinalList(new[] { 0 });
            OrdinalList listb = ((ICloneable<OrdinalList>)lista).Clone();
            Assert.False(ReferenceEquals(lista, listb));
            Assert.Equal(lista.Count, listb.Count);
            Assert.True(listb.Contains(0));

            listb.Add(1);
            Assert.True(listb.Contains(1));
            Assert.False(lista.Contains(1));
        }

        [Fact]
        public void TestICollection()
        {
            OrdinalList list = new OrdinalList();
            list.AddRange(new[] { 5, 10, 20 });

            ICollection coll = list;
            Assert.False(coll.IsSynchronized);
            Assert.True(ReferenceEquals(coll, coll.SyncRoot));

            int[] copy = new int[3];
            coll.CopyTo(copy, 0);
            Assert.Equal(5, copy[0]);
            Assert.Equal(10, copy[1]);
            Assert.Equal(20, copy[2]);

            byte[] bits = new byte[3];
            coll.CopyTo(bits, 0);
            Assert.Equal(32, bits[0]);
            Assert.Equal(4, bits[1]);
            Assert.Equal(16, bits[2]);

            List<int> tmp = new List<int>();
            foreach (int i in coll)
                tmp.Add(i);
            Assert.Equal(3, tmp.Count);
            Assert.Equal(5, tmp[0]);
            Assert.Equal(10, tmp[1]);
            Assert.Equal(20, tmp[2]);
        }

        [Fact]
        public void TestIntersectUnion()
        {
            OrdinalList lista = new OrdinalList(new[] { 5, 10, 99 });
            OrdinalList listb = new OrdinalList(new[] { 2, 4, 6, 8, 10 });

            OrdinalList union = lista.UnionWith(listb);
            Assert.Equal(7, union.Count);
            foreach (int i in union)
                Assert.True(lista.Contains(i) || listb.Contains(i));

            OrdinalList inter = lista.IntersectWith(listb);
            Assert.Equal(1, inter.Count);
            foreach (int i in inter)
                Assert.Equal(10, i);
        }

        [Fact]
        public void TestIntersectUnionSameLength()
        {
            OrdinalList lista = new OrdinalList(new[] { 1, 4, 5 });
            OrdinalList listb = new OrdinalList(new[] { 2, 4, 6 });

            OrdinalList union = lista.UnionWith(listb);
            Assert.Equal(5, union.Count);
            foreach (int i in union)
                Assert.True(lista.Contains(i) || listb.Contains(i));

            OrdinalList inter = lista.IntersectWith(listb);
            Assert.Equal(1, inter.Count);
            foreach (int i in inter)
                Assert.Equal(4, i);
        }

        [Fact]
        public void TestInvert()
        {
            OrdinalList lista = new OrdinalList(new[] { 0, 2, 4, 6, 8, 10, 12 });
            OrdinalList listb = new OrdinalList(new[] { 1, 3, 5, 7, 9, 11, 13 });

            OrdinalList invta = lista.Invert(13);
            string invtatext = "", listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.Equal(listbtext, invtatext);

            lista = new OrdinalList(new[] { 0 });
            listb = new OrdinalList(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 });
            invta = lista.Invert(13);
            invtatext = "";
            listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.Equal(listbtext, invtatext);

            lista = new OrdinalList(new[] { 0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13 });
            listb = new OrdinalList(new[] { 3 });
            invta = lista.Invert(4);
            invtatext = "";
            listbtext = "";
            foreach (int i in invta)
                invtatext += "," + i;
            foreach (int i in listb)
                listbtext += "," + i;
            Assert.Equal(listbtext, invtatext);
        }

        /// <summary>
        ///     Previously it was thought to be invalid to set a negative ceiling; however, since ceiling
        ///     is an 'inclusive' value, the value of '0' actually requires 1 byte to be allocated.  In
        ///     order to allow explicitly clearing the array length, a -1 must be allowed;
        /// </summary>
        [Fact]
        public void TestNegativeCeiling()
        {
            OrdinalList list = new OrdinalList();
            list.Ceiling = -1;
            Assert.Equal(-1, list.Ceiling);
        }
    }


    public class TestOrdinalListNegative
    {
        [Fact]
        public void TestBadArrayType()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                ICollection list = new OrdinalList();
                list.CopyTo(new ulong[3], 0);
            });
        }

        /* '-1' is now allowed * see comments on TestNegativeCeiling */
        [Fact]
        public void TestBadCeiling()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                OrdinalList list = new OrdinalList();
                list.Ceiling = -2;
            });
        }
    }
}