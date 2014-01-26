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
using System.IO;
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestVariantSerializer
    {
        private readonly Random _random = new Random();
        private readonly VariantNumberSerializer _serializer = new VariantNumberSerializer();

        private void ReadWrite<T>(T value)
        {
            ISerializer<T> ser = (ISerializer<T>)_serializer;
            using (MemoryStream ms = new MemoryStream())
            {
                ser.WriteTo(value, ms);
                // add random bytes, every value should know it's length and not rely on EOF.
                byte[] bytes = new byte[256 - ms.Position];
                _random.NextBytes(bytes);
                ms.Write(bytes, 0, bytes.Length);
                // seek begin and read.
                ms.Position = 0;
                Assert.AreEqual(value, ser.ReadFrom(ms));
            }
        }

        [Test]
        public void TestSerializeInt()
        {
            ReadWrite(-1);
            ReadWrite(int.MinValue);
            ReadWrite(int.MaxValue);
            for (int i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeUInt()
        {
            ReadWrite(uint.MinValue);
            ReadWrite(uint.MaxValue);
            for (uint i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeLong()
        {
            ReadWrite(-1L);
            ReadWrite(long.MinValue);
            ReadWrite(long.MaxValue);
            for (long i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeULong()
        {
            ReadWrite(ulong.MinValue);
            ReadWrite(ulong.MaxValue);
            for (ulong i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

    }
}
