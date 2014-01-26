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
using System.IO;

namespace CSharpTest.Net.Serialization
{
    /// <summary>
    /// Provides numeric serializers for packed int/long values.
    /// </summary>
    public class VariantNumberSerializer :
        ISerializer<int>,
        ISerializer<uint>,
        ISerializer<long>,
        ISerializer<ulong>
    {
        /// <summary> Gets a singleton of the VariantNumberSerializer </summary>
        public static readonly VariantNumberSerializer Instance = new VariantNumberSerializer();
        /// <summary> Gets a typed version of the VariantNumberSerializer </summary>
        public static readonly ISerializer<int> Int32 = Instance;
        /// <summary> Gets a typed version of the VariantNumberSerializer </summary>
        public static readonly ISerializer<uint> UInt32 = Instance;
        /// <summary> Gets a typed version of the VariantNumberSerializer </summary>
        public static readonly ISerializer<long> Int64 = Instance;
        /// <summary> Gets a typed version of the VariantNumberSerializer </summary>
        public static readonly ISerializer<ulong> UInt64 = Instance;

        #region ISerializer<int> Members

        void ISerializer<int>.WriteTo(int value, Stream stream)
        {
            ((ISerializer<uint>)this).WriteTo(unchecked((uint)value), stream);
        }

        int ISerializer<int>.ReadFrom(Stream stream)
        {
            return unchecked((int)((ISerializer<uint>)this).ReadFrom(stream));
        }

        #endregion
        #region ISerializer<uint> Members

        void ISerializer<uint>.WriteTo(uint value, Stream stream)
        {
            unchecked
            {
                while (value > 0x7F)
                {
                    stream.WriteByte((byte)(value | 0x80));
                    value >>= 7;
                }
                stream.WriteByte((byte)value);
            }
        }

        uint ISerializer<uint>.ReadFrom(Stream stream)
        {
            const uint mask = 0x7f;
            int last;
            uint value = 0;
            int shift = 0;
            do
            {
                last = stream.ReadByte();
                Check.Assert<InvalidDataException>(last != -1);

                value = (value & ~(mask << shift)) + ((uint)last << shift);
                shift += 7;
            } while ((last & 0x080) != 0);
            return value;
        }

        #endregion
        #region ISerializer<long> Members

        void ISerializer<long>.WriteTo(long value, Stream stream)
        {
            ((ISerializer<ulong>)this).WriteTo(unchecked((ulong)value), stream);
        }

        long ISerializer<long>.ReadFrom(Stream stream)
        {
            return unchecked((long)((ISerializer<ulong>)this).ReadFrom(stream));
        }

        #endregion
        #region ISerializer<ulong> Members

        /// <summary> Writes the object to the stream </summary>
        void ISerializer<ulong>.WriteTo(ulong value, Stream stream)
        {
            unchecked
            {
                while (value > 0x7F)
                {
                    stream.WriteByte((byte)(value | 0x80));
                    value >>= 7;
                }
                stream.WriteByte((byte)value);
            }
        }

        /// <summary> Reads the object from a stream </summary>
        ulong ISerializer<ulong>.ReadFrom(Stream stream)
        {
            const ulong mask = 0x7f;
            int last;
            ulong value = 0;
            int shift = 0;
            do
            {
                last = stream.ReadByte();
                Check.Assert<InvalidDataException>(last != -1);

                value = (value & ~(mask << shift)) + ((ulong)last << shift);
                shift += 7;
            } while ((last & 0x080) != 0);
            return value;
        }

        #endregion
    }
}