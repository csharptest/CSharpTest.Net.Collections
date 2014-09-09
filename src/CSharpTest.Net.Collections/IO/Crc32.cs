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
        #region /* polynomial on 0xedb88320 */
        static readonly UInt32[] Table = { /* CRC polynomial 0xedb88320 */
0xd2fd1072, 0xa5fa20e4, 0x3cf3715e, 0x4bf441c8, 0xd590d46b, 0xa297e4fd, 
0x3b9eb547, 0x4c9985d1, 0xdc269840, 0xab21a8d6, 0x3228f96c, 0x452fc9fa, 
0xdb4b5c59, 0xac4c6ccf, 0x35453d75, 0x42420de3, 0xcf4a0016, 0xb84d3080, 
0x2144613a, 0x564351ac, 0xc827c40f, 0xbf20f499, 0x2629a523, 0x512e95b5, 
0xc1918824, 0xb696b8b2, 0x2f9fe908, 0x5898d99e, 0xc6fc4c3d, 0xb1fb7cab, 
0x28f22d11, 0x5ff51d87, 0xe99330ba, 0x9e94002c, 0x79d5196, 0x709a6100, 
0xeefef4a3, 0x99f9c435, 0xf0958f, 0x77f7a519, 0xe748b888, 0x904f881e, 
0x946d9a4, 0x7e41e932, 0xe0257c91, 0x97224c07, 0xe2b1dbd, 0x792c2d2b, 
0xf42420de, 0x83231048, 0x1a2a41f2, 0x6d2d7164, 0xf349e4c7, 0x844ed451, 
0x1d4785eb, 0x6a40b57d, 0xfaffa8ec, 0x8df8987a, 0x14f1c9c0, 0x63f6f956, 
0xfd926cf5, 0x8a955c63, 0x139c0dd9, 0x649b3d4f, 0xa42151e2, 0xd3266174, 
0x4a2f30ce, 0x3d280058, 0xa34c95fb, 0xd44ba56d, 0x4d42f4d7, 0x3a45c441, 
0xaafad9d0, 0xddfde946, 0x44f4b8fc, 0x33f3886a, 0xad971dc9, 0xda902d5f, 
0x43997ce5, 0x349e4c73, 0xb9964186, 0xce917110, 0x579820aa, 0x209f103c, 
0xbefb859f, 0xc9fcb509, 0x50f5e4b3, 0x27f2d425, 0xb74dc9b4, 0xc04af922, 
0x5943a898, 0x2e44980e, 0xb0200dad, 0xc7273d3b, 0x5e2e6c81, 0x29295c17, 
0x9f4f712a, 0xe84841bc, 0x71411006, 0x6462090, 0x9822b533, 0xef2585a5, 
0x762cd41f, 0x12be489, 0x9194f918, 0xe693c98e, 0x7f9a9834, 0x89da8a2, 
0x96f93d01, 0xe1fe0d97, 0x78f75c2d, 0xff06cbb, 0x82f8614e, 0xf5ff51d8, 
0x6cf60062, 0x1bf130f4, 0x8595a557, 0xf29295c1, 0x6b9bc47b, 0x1c9cf4ed, 
0x8c23e97c, 0xfb24d9ea, 0x622d8850, 0x152ab8c6, 0x8b4e2d65, 0xfc491df3, 
0x65404c49, 0x12477cdf, 0x3f459352, 0x4842a3c4, 0xd14bf27e, 0xa64cc2e8, 
0x3828574b, 0x4f2f67dd, 0xd6263667, 0xa12106f1, 0x319e1b60, 0x46992bf6, 
0xdf907a4c, 0xa8974ada, 0x36f3df79, 0x41f4efef, 0xd8fdbe55, 0xaffa8ec3, 
0x22f28336, 0x55f5b3a0, 0xccfce21a, 0xbbfbd28c, 0x259f472f, 0x529877b9, 
0xcb912603, 0xbc961695, 0x2c290b04, 0x5b2e3b92, 0xc2276a28, 0xb5205abe, 
0x2b44cf1d, 0x5c43ff8b, 0xc54aae31, 0xb24d9ea7, 0x42bb39a, 0x732c830c, 
0xea25d2b6, 0x9d22e220, 0x3467783, 0x74414715, 0xed4816af, 0x9a4f2639, 
0xaf03ba8, 0x7df70b3e, 0xe4fe5a84, 0x93f96a12, 0xd9dffb1, 0x7a9acf27, 
0xe3939e9d, 0x9494ae0b, 0x199ca3fe, 0x6e9b9368, 0xf792c2d2, 0x8095f244, 
0x1ef167e7, 0x69f65771, 0xf0ff06cb, 0x87f8365d, 0x17472bcc, 0x60401b5a, 
0xf9494ae0, 0x8e4e7a76, 0x102aefd5, 0x672ddf43, 0xfe248ef9, 0x8923be6f, 
0x4999d2c2, 0x3e9ee254, 0xa797b3ee, 0xd0908378, 0x4ef416db, 0x39f3264d, 
0xa0fa77f7, 0xd7fd4761, 0x47425af0, 0x30456a66, 0xa94c3bdc, 0xde4b0b4a, 
0x402f9ee9, 0x3728ae7f, 0xae21ffc5, 0xd926cf53, 0x542ec2a6, 0x2329f230, 
0xba20a38a, 0xcd27931c, 0x534306bf, 0x24443629, 0xbd4d6793, 0xca4a5705, 
0x5af54a94, 0x2df27a02, 0xb4fb2bb8, 0xc3fc1b2e, 0x5d988e8d, 0x2a9fbe1b, 
0xb396efa1, 0xc491df37, 0x72f7f20a, 0x5f0c29c, 0x9cf99326, 0xebfea3b0, 
0x759a3613, 0x29d0685, 0x9b94573f, 0xec9367a9, 0x7c2c7a38, 0xb2b4aae, 
0x92221b14, 0xe5252b82, 0x7b41be21, 0xc468eb7, 0x954fdf0d, 0xe248ef9b, 
0x6f40e26e, 0x1847d2f8, 0x814e8342, 0xf649b3d4, 0x682d2677, 0x1f2a16e1, 
0x8623475b, 0xf12477cd, 0x619b6a5c, 0x169c5aca, 0x8f950b70, 0xf8923be6, 
0x66f6ae45, 0x11f19ed3, 0x88f8cf69, 0xffffffff};
        #endregion

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
            _crc32 = (((~_crc32 >> 8) & 0x00FFFFFF) ^ (int)Table[(_crc32 ^ b) & 0x0ff]);
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

            UInt32 crc32 = (UInt32) _crc32;
            for (; length > 0; --length, ++start)
                crc32 = (((~crc32 >> 8) & 0x00FFFFFF) ^ Table[(crc32 ^ bytes[start]) & 0x0ff]);
            _crc32 = (int) crc32;
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
            UInt32 crc32 = (UInt32) _crc32;

            foreach (char ch in Check.NotNull(text))
            {
                crc32 = (((~crc32 >> 8) & 0x00FFFFFF) ^ Table[(crc32 ^ ((byte)ch >> 8)) & 0x0ff]);
                crc32 = (((~crc32 >> 8) & 0x00FFFFFF) ^ Table[(crc32 ^ ((byte)ch)) & 0x0ff]);
            }
            _crc32 = (int) crc32;
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