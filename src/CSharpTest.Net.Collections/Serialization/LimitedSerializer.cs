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

namespace CSharpTest.Net.Serialization
{
    /// <summary>
    /// Reads the same variant prefixed string and byte[] but with a hard-limit on size
    /// </summary>
    public class LimitedSerializer : ISerializer<byte[]>, ISerializer<string>
    {
        private readonly int _maxLength;

        /// <summary>
        /// Constructs a limited length-prefix data reader/writer
        /// </summary>
        public LimitedSerializer(int maxLength)
        {
            _maxLength = maxLength;
        }

        /// <summary> Reads up to 1024 length-prefixed byte array </summary>
        public static readonly ISerializer<byte[]> Bytes1024 = new LimitedSerializer(1024);
        /// <summary> Reads up to 2048 length-prefixed byte array </summary>
        public static readonly ISerializer<byte[]> Bytes2048 = new LimitedSerializer(2048);
        /// <summary> Reads up to 4092 length-prefixed byte array </summary>
        public static readonly ISerializer<byte[]> Bytes4092 = new LimitedSerializer(4092);
        /// <summary> Reads up to 8196 length-prefixed byte array </summary>
        public static readonly ISerializer<byte[]> Bytes8196 = new LimitedSerializer(8196);

        /// <summary> Reads up to 256 length-prefixed string </summary>
        public static readonly ISerializer<string> String256 = new LimitedSerializer(256);
        /// <summary> Reads up to 512 length-prefixed string </summary>
        public static readonly ISerializer<string> String512 = new LimitedSerializer(512);
        /// <summary> Reads up to 1024 length-prefixed string </summary>
        public static readonly ISerializer<string> String1024 = new LimitedSerializer(1024);

        /// <summary> This is the only class with read/write prefixed data </summary>
        internal static readonly LimitedSerializer Unlimited = new LimitedSerializer(int.MaxValue);

        #region ISerializer<string> Members

        void ISerializer<string>.WriteTo(string value, Stream stream)
        {
            if (value == null)
            {
                VariantNumberSerializer.Int32.WriteTo(int.MinValue, stream);
            }
            else
            {
                Check.Assert<InvalidDataException>(value.Length <= _maxLength);
                VariantNumberSerializer.Int32.WriteTo(value.Length, stream);
                foreach (char ch in value)
                    VariantNumberSerializer.Int32.WriteTo(ch, stream);
            }
        }

        string ISerializer<string>.ReadFrom(Stream stream)
        {
            unchecked
            {
                int sz = VariantNumberSerializer.Int32.ReadFrom(stream);
                if (sz == 0) return string.Empty;
                if (sz == int.MinValue)
                    return null;

                Check.Assert<InvalidDataException>(sz >= 0 && sz <= _maxLength);
                char[] chars = new char[sz];
                for (int i = 0; i < sz; i++)
                    chars[i] = (char)VariantNumberSerializer.Int32.ReadFrom(stream);
                return new String(chars);
            }
        }

        #endregion
        #region ISerializer<byte[]> Members

        void ISerializer<byte[]>.WriteTo(byte[] value, Stream stream)
        {
            if (value == null)
            {
                VariantNumberSerializer.Int32.WriteTo(int.MinValue, stream);
            }
            else
            {
                Check.Assert<InvalidDataException>(value.Length <= _maxLength);
                VariantNumberSerializer.Int32.WriteTo(value.Length, stream);
                foreach (byte b in value)
                    stream.WriteByte(b);
            }
        }
        byte[] ISerializer<byte[]>.ReadFrom(Stream stream)
        {
            int sz = VariantNumberSerializer.Int32.ReadFrom(stream);
            if (sz == int.MinValue)
                return null;

            Check.Assert<InvalidDataException>(sz >= 0 && sz <= _maxLength);
            byte[] bytes = new byte[sz];
            int pos = 0, len;
            while (0 != (len = stream.Read(bytes, pos, sz - pos)))
                pos += len;
            Check.Assert<InvalidDataException>(pos == sz);
            return bytes;
        }

        #endregion
    }
}
