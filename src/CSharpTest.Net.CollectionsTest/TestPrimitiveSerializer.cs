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
using System.Collections.Generic;
using System.IO;
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestPrimitiveSerializer
    {
        private readonly Random _random = new Random();
        private readonly PrimitiveSerializer _serializer = new PrimitiveSerializer();

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
        public void TestBytesSerializer()
        {
            byte[] bytes = new byte[256];
            _random.NextBytes(bytes);

            using (MemoryStream ms = new MemoryStream())
            {
                BytesSerializer.RawBytes.WriteTo(bytes, ms);
                ms.Position = 0;
                Assert.AreEqual(bytes, BytesSerializer.RawBytes.ReadFrom(ms));
            }
        }

        [Test]
        public void TestSerializeString()
        {
            ReadWrite<string>(null);
            ReadWrite(String.Empty);
            ReadWrite(GetType().ToString());
        }

        [Test]
        public void TestSerializeBool()
        {
            ReadWrite(true);
            ReadWrite(false);
        }

        [Test]
        public void TestSerializeByte()
        {
            ReadWrite(byte.MinValue);
            ReadWrite(byte.MaxValue);
            for (byte i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeSByte()
        {
            ReadWrite(sbyte.MinValue);
            ReadWrite(sbyte.MaxValue);
            for (sbyte i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeByteArray()
        {
            ReadWrite<byte[]>(null);
            ReadWrite(new byte[0]);
            ReadWrite(new byte[] { 0, 123, byte.MaxValue });
        }

        [Test]
        public void TestSerializeChar()
        {
            ReadWrite(char.MinValue);
            ReadWrite(char.MaxValue);
            for (short i = 1; i != 0; i *= 2)
                ReadWrite((char)i);
            for (char ch = (char)0; ch < 256; ch++)
                ReadWrite(ch);
        }

        [Test]
        public void TestSerializeDateTime()
        {
            ReadWrite(DateTime.MinValue);
            ReadWrite(DateTime.MaxValue);
            ReadWrite(DateTime.Now);
            ReadWrite(DateTime.UtcNow);
            ReadWrite(DateTime.Today);
        }

        [Test]
        public void TestSerializeTimeSpan()
        {
            ReadWrite(TimeSpan.MinValue);
            ReadWrite(TimeSpan.MaxValue);
            ReadWrite(TimeSpan.Zero);
            ReadWrite(TimeSpan.FromTicks(DateTime.UtcNow.Ticks));
            ReadWrite(TimeSpan.FromTicks(1));
            ReadWrite(TimeSpan.FromTicks(long.MaxValue));
        }

        [Test]
        public void TestSerializeShort()
        {
            ReadWrite(short.MinValue);
            ReadWrite(short.MaxValue);
            for (short i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeUShort()
        {
            ReadWrite(ushort.MinValue);
            ReadWrite(ushort.MaxValue);
            for (ushort i = 1; i != 0; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeInt()
        {
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

        [Test]
        public void TestSerializeDouble()
        {
            ReadWrite(double.MinValue);
            ReadWrite(double.MaxValue);
            ReadWrite(double.PositiveInfinity);
            ReadWrite(double.NegativeInfinity);
            ReadWrite(double.NaN);
            ReadWrite(double.Epsilon);
            ReadWrite(1.123);
            ReadWrite(0.001);
            for (double i = double.Epsilon; i < double.MaxValue; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeFloat()
        {
            ReadWrite(float.MinValue);
            ReadWrite(float.MaxValue);
            ReadWrite(float.PositiveInfinity);
            ReadWrite(float.NegativeInfinity);
            ReadWrite(float.NaN);
            ReadWrite(float.Epsilon);
            ReadWrite(1.123);
            ReadWrite(0.001);
            for (float i = float.Epsilon; i < float.MaxValue; i *= 2)
                ReadWrite(i);
        }

        [Test]
        public void TestSerializeGuid()
        {
            ReadWrite(Guid.Empty);
            ReadWrite(Guid.NewGuid());
        }

        [Test]
        public void TestSerializeIntPtr()
        {
            ReadWrite(IntPtr.Zero);

            if (IntPtr.Size == 4)
            {
                for (int i = 1; i != 0; i *= 2)
                    ReadWrite(new IntPtr(i));
            }
            else
            {
                for (long i = 1; i != 0; i *= 2)
                    ReadWrite(new IntPtr(i));
            }
        }

        [Test]
        public void TestSerializeUIntPtr()
        {
            ReadWrite(UIntPtr.Zero);

            if (UIntPtr.Size == 4)
            {
                for (uint i = 1; i != 0; i *= 2)
                    ReadWrite(new UIntPtr(i));
            }
            else
            {
                for (ulong i = 1; i != 0; i *= 2)
                    ReadWrite(new UIntPtr(i));
            }
        }

        [Test]
        public void TestSerializeKeyValuePair()
        {
            ISerializer<KeyValuePair<int, Guid>> ser = new KeyValueSerializer<int, Guid>(PrimitiveSerializer.Int32, PrimitiveSerializer.Guid);
            Dictionary<int, Guid> values = new Dictionary<int, Guid>
            {
                { -1, Guid.NewGuid() },
                { 0, Guid.NewGuid() },
                { 1, Guid.NewGuid() },
                { int.MaxValue, Guid.NewGuid() },
            };

            foreach (KeyValuePair<int, Guid> value in values)
            {
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
        }
    }
}
