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
using CSharpTest.Net.IO;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestCrc32
    {
        private static void AssertCrc32(string input, int expected)
        {
            //Can not use ASCII encoding since it does not support 8-bit characters: resumé
            byte[] bytes = new byte[input.Length];
            for (int i = 0; i < input.Length; i++)
                bytes[i] = unchecked((byte)input[i]);
            AssertCrc32(bytes, expected);
        }
        private static void AssertCrc32(byte[] input, int expected)
        {
            Crc32 crc = new Crc32(input);
            Assert.AreEqual(expected, crc.Value);

            crc = new Crc32(0);
            crc.Add(input);
            Assert.AreEqual(expected, crc.Value);

            crc = new Crc32();
            crc.Add(input, 0, input.Length);
            Assert.AreEqual(expected, crc.Value);

            crc = new Crc32();
            foreach (byte b in input)
                crc.Add(b);
            Assert.AreEqual(expected, crc.Value);
        }

        [Test]
        public void TestKnown123456789()
        { AssertCrc32("123456789", unchecked((int)0xCBF43926)); }

        [Test]
        public void TestKnownResume1()
        { AssertCrc32("resume", 0x60c1d0a0); }

        [Test]
        public void TestKnownResume2()
        {
            //the string is "resumé" with the accented 'é'; however, I did not want to depend on proper text encoding.
            AssertCrc32("resum" + (char)233, unchecked((int)0x84cf1fab)); 
        }

        [Test]
        public void TestKnownBytesLeadingZero()
        { AssertCrc32(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x86, 0x4d, 0x7f, 0x99 }, unchecked((int)0x923D6EFD)); }

        [Test]
        public void TestKnownBytesLeading0Xff()
        { AssertCrc32(new byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x28, 0xc5, 0x5e, 0x45, 0x7a }, 0x49a04d82); }

        [Test]
        public void TestKnownBytesSequence()
        { AssertCrc32(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x00, 0x00, 0x00, 0x28, 0xbf, 0x67, 0x1e, 0xd0 }, 0x688b3bfa); }

        [Test]
        public void TestOperatorEquality()
        {
            Crc32 empty = new Crc32();
            Crc32 value = new Crc32("Hello");
            Crc32 copy = new Crc32(value.Value);

            Assert.IsTrue(value == copy);
            Assert.IsFalse(value == empty);
            Assert.IsTrue(value == copy.Value);
            Assert.IsFalse(value == empty.Value);

            Assert.IsFalse(value != copy);
            Assert.IsTrue(value != empty);
            Assert.IsFalse(value != copy.Value);
            Assert.IsTrue(value != empty.Value);
        }
        [Test]
        public void TestCrc32Equals()
        {
            Crc32 empty = new Crc32();
            Crc32 value = new Crc32("Hello");
            Crc32 copy = new Crc32(value.Value);

            Assert.IsTrue(value.Equals(copy));
            Assert.IsFalse(value.Equals(empty));
            Assert.IsTrue(value.Equals(copy.Value));
            Assert.IsFalse(value.Equals(empty.Value));

            Assert.IsTrue(value.Equals((object)copy));
            Assert.IsFalse(value.Equals((object)empty));
            Assert.IsTrue(value.Equals((object)copy.Value));
            Assert.IsFalse(value.Equals((object)empty.Value));
        }

        [Test]
        public void TestHashValue()
        {
            Crc32 crc = new Crc32();
            Assert.AreEqual(0, crc.Value);
            Assert.AreEqual(0, crc.GetHashCode());

            crc.Add(0x1b);

            Assert.AreNotEqual(0, crc.Value);
            Assert.AreEqual(crc.Value, crc.GetHashCode());
        }

        [Test]
        public void TestOperatorPlusBytes()
        {
            Crc32 all = new Crc32(new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8 });
            Crc32 crc = new Crc32();
            Assert.AreEqual(0, crc.Value);
            crc += new byte[] { 0x1, 0x2, 0x3, 0x4 };
            crc += 0x5;
            crc += 0x6;
            crc += 0x7;
            crc += 0x8;
            Assert.AreEqual(all.Value, crc.Value);
        }

        [Test]
        public void TestOperatorPlusString()
        {
            Crc32 all = new Crc32("hello there world");
            Crc32 crc = new Crc32();
            Assert.AreEqual(0, crc.Value);
            crc += "hello ";
            crc += "there ";
            crc += "world";
            Assert.AreEqual(all.Value, crc.Value);
        }

        [Test]
        public void TestAddByteRange()
        {
            Crc32 all = new Crc32(new byte[] { 0x2, 0x3, 0x4, 0x5, 0x6 });
            Crc32 crc = new Crc32();
            Assert.AreEqual(0, crc.Value);
            crc.Add(new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8 }, 1, 5);
            Assert.AreEqual(all.Value, crc.Value);
        }

        [Test]
        public void TestToString()
        {
            Assert.AreEqual("00000000", new Crc32().ToString());
            Assert.AreEqual("00100100", new Crc32(0x00100100).ToString());
            Assert.AreEqual("F0100100", new Crc32(unchecked((int)0xF0100100)).ToString());
        }
    }
}
