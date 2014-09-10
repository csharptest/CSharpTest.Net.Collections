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

namespace CSharpTest.Net.IO
{
    /// <summary> Provides a simple CRC32 checksum for a set of bytes </summary>
    [System.Diagnostics.DebuggerDisplay("{Value:X8}")]
    public struct Crc32 : IEquatable<Crc32>, IEquatable<int>
    {
        static readonly int[] Table;
        static Crc32()
        {
            Table = new int[256];
            for (uint i = 0; i < 256; i++)
            {
                uint crc = i;
                for (uint j = 0; j < 8; j++)
                    crc = (crc >> 1) ^ (((crc & 1) == 1) ? 0xEDB88320u : 0);
                //Note: table ordinal and value inverted to omit -1 start and end operation
                Table[~i & 0x0ff] = ~unchecked((int)crc);
            }
        }

        int _crc32;

        /// <summary> Resumes the computation of a CRC32 value </summary>
        public Crc32(int crc)
        { _crc32 = crc; }

        /// <summary> Initailizes the Crc32 value to the checksum of the string as a series of 16-bit values </summary>
        public Crc32(string text)
        { _crc32 = 0; Add(text); }

        /// <summary> Initailizes the Crc32 value to the checksum of the bytes provided </summary>
        public Crc32(byte[] bytes)
        { _crc32 = 0; Add(bytes, 0, bytes.Length); }

        /// <summary> Returns the computed CRC32 value as a Hex string </summary>
        public override string ToString() { return String.Format("{0:X8}", Value); }

        /// <summary> Returns the computed CRC32 value </summary>
        public int Value { get { return _crc32; } }

        /// <summary> Adds a byte to the checksum </summary>
        public void Add(byte b)
        {
            _crc32 = (((~_crc32 >> 8) & 0x00FFFFFF) ^ Table[(_crc32 ^ b) & 0x0ff]);
        }

        /// <summary> Adds a byte to the checksum </summary>
        public static Crc32 operator +(Crc32 chksum, byte b)
        {
            chksum.Add(b);
            return chksum;
        }

        /// <summary> Adds an entire array of bytes to the checksum </summary>
        public void Add(byte[] bytes)
        { Add(bytes, 0, Check.NotNull(bytes).Length); }

        /// <summary> Adds a range from an array of bytes to the checksum </summary>
        public void Add(byte[] bytes, int start, int length)
        {
            Check.NotNull(bytes);
            int end = start + length;
            var temp = this._crc32;

            for (int i = start; i < end; i++)
                temp = (((~temp >> 8) & 0x00FFFFFF) ^ Table[(temp ^ bytes[i]) & 0x0ff]);

            _crc32 = temp;
        }

        /// <summary> Adds an entire array of bytes to the checksum </summary>
        public static Crc32 operator +(Crc32 chksum, byte[] bytes)
        {
            chksum.Add(bytes, 0, bytes.Length);
            return chksum;
        }

        /// <summary> Adds a string to the checksum as a series of 16-bit values (big endian) </summary>
        public void Add(string text)
        {
            var temp = this._crc32;

            foreach (char ch in Check.NotNull(text))
            {
                temp = (((~temp >> 8) & 0x00FFFFFF) ^ Table[(temp ^ ((byte)ch >> 8)) & 0x0ff]);
                temp = (((~temp >> 8) & 0x00FFFFFF) ^ Table[(temp ^ ((byte)ch)) & 0x0ff]);
            }

            _crc32 = temp;
        }

        /// <summary> Adds a string to the checksum as a series of 16-bit values </summary>
        public static Crc32 operator +(Crc32 chksum, string text)
        {
            chksum.Add(text);
            return chksum;
        }

        /// <summary> Extracts the correct hash code </summary>
        public override int GetHashCode() { return Value; }

        /// <summary> Returns true if the other object is equal to this one </summary>
        public override bool Equals(object obj)
        {
            return 
                (obj is Crc32 && _crc32 == ((Crc32)obj)._crc32) ||
                (obj is int && Value == ((int)obj));
        }

        /// <summary> Returns true if the other object is equal to this one </summary>
        public bool Equals(Crc32 other) { return _crc32 == other._crc32; }
        /// <summary> Returns true if the CRC32 provided is equal to this one </summary>
        public bool Equals(int crc32) { return Value == crc32; }

        /// <summary> Compares the two objects for equality </summary>
        public static bool operator ==(Crc32 x, Crc32 y) { return x._crc32 == y._crc32; }
        /// <summary> Compares the two objects for equality </summary>
        public static bool operator !=(Crc32 x, Crc32 y) { return x._crc32 != y._crc32; }

        /// <summary> Compares the two objects for equality </summary>
        public static bool operator ==(Crc32 x, int y) { return x.Value == y; }
        /// <summary> Compares the two objects for equality </summary>
        public static bool operator !=(Crc32 x, int y) { return x.Value != y; }

    }
}