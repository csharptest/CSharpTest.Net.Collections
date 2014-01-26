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
using CSharpTest.Net.IO;
using NUnit.Framework;

#pragma warning disable 1591
namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    [Category("TestCloning")]
    public partial class TestBinaryComparer
    {
        [Test]
        public void TestEquals()
        {
            Assert.IsTrue(BinaryComparer.Equals(null, null));
            Assert.IsTrue(BinaryComparer.Equals(new byte[] { }, new byte[] { }));
            Assert.IsTrue(BinaryComparer.Equals(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }));

            Assert.IsFalse(BinaryComparer.Equals(null, new byte[] { 1, 2, 3 }));
            Assert.IsFalse(BinaryComparer.Equals(new byte[] { 1, 2, 3 }, null));
            Assert.IsFalse(BinaryComparer.Equals(new byte[] { 1, 2 }, new byte[] { 1, 2, 3 }));
            Assert.IsFalse(BinaryComparer.Equals(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }));
        }
        [Test]
        public void TestHashCode()
        {
            Assert.AreEqual(0, BinaryComparer.GetHashCode(null));
            Assert.AreEqual(0, BinaryComparer.GetHashCode(new byte[] { }));
            Assert.AreEqual(BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }), BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }));

            Assert.AreNotEqual(BinaryComparer.GetHashCode(null), BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }));
            Assert.AreNotEqual(BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }), BinaryComparer.GetHashCode(null));
            Assert.AreNotEqual(BinaryComparer.GetHashCode(new byte[] { 1, 2 }), BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }));
            Assert.AreNotEqual(BinaryComparer.GetHashCode(new byte[] { 1, 2, 3 }), BinaryComparer.GetHashCode(new byte[] { 1, 2 }));
        }

        [Test]
        public void TestCompare()
        {
            Assert.AreEqual(0, BinaryComparer.Compare(null, null));
            Assert.AreEqual(0, BinaryComparer.Compare(new byte[] { }, new byte[] { }));
            Assert.AreEqual(0, BinaryComparer.Compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 }));

            Assert.AreEqual(-1, BinaryComparer.Compare(null, new byte[] { 1, 2, 3 }));
            Assert.AreEqual(1, BinaryComparer.Compare(new byte[] { 1, 2, 3 }, null));
            Assert.AreEqual(-1, BinaryComparer.Compare(new byte[] { 1, 2 }, new byte[] { 1, 2, 3 }));
            Assert.AreEqual(1, BinaryComparer.Compare(new byte[] { 1, 2, 3 }, new byte[] { 1, 2 }));
        }

        [Test]
        public void TestHashable()
        {
            List<Guid> all = new List<Guid>();
            Dictionary<byte[], Guid> data = new Dictionary<byte[], Guid>(new BinaryComparer());

            for (int i = 0; i < 1000; i++)
            {
                Guid guid = Guid.NewGuid();
                all.Add(guid);
                data.Add(guid.ToByteArray(), guid);
            }

            foreach (Guid g in all)
                Assert.AreEqual(g, data[(byte[])g.ToByteArray().Clone()]);
        }

        [Test]
        public void TestSortable()
        {
            List<byte[]> all = new List<byte[]>();

            all.Add(null);
            all.Add(Guid.Empty.ToByteArray());
            for (int i = 0; i < 1000; i++)
                all.Add(Guid.NewGuid().ToByteArray());

            all.Sort(new BinaryComparer());

            byte[] last = null;
            Assert.IsNull(all[0]);
            all.RemoveAt(0);

            foreach (byte[] entry in all)
            {
                if(last != null)
                    Assert.IsTrue(StringComparer.Ordinal.Compare(Convert.ToBase64String(last), Convert.ToBase64String(entry)) < 0);
                Assert.IsTrue(BinaryComparer.Compare(last, entry) < 0);
            }
        }
    }
}
