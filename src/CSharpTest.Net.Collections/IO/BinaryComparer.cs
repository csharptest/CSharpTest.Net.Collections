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

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Dictionary comparer for comparing arrays of bytes by value equality
    /// </summary>
    public sealed class BinaryComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
    {
        /// <summary> returns true if both arrays contain the exact same set of bytes. </summary>
        public static bool Equals(byte[] ar1, byte[] ar2)
        { return 0 == Compare(ar1, ar2); }

        /// <summary> Compares the contents of the byte arrays and returns the result. </summary>
        public static int Compare(byte[] ar1, byte[] ar2)
        {
            if (ar1 == null) return ar2 == null ? 0 : -1;
            if (ar2 == null) return 1;

            int result = 0;
            int i = 0, stop = Math.Min(ar1.Length, ar2.Length);

            for (; 0 == result && i < stop; i++)
                result = ar1[i].CompareTo(ar2[i]);

            if (result != 0)
                return result;
            if (i == ar1.Length)
                return i == ar2.Length ? 0 : -1;
            return 1;
        }

        /// <summary> Returns a hash code the instance of the object </summary>
        public static int GetHashCode(byte[] bytes)
        {
            if(bytes == null) return 0;
            return new IO.Crc32(bytes).Value;
        }

        /// <summary> Compares the contents of the byte arrays and returns the result. </summary> 
        int IComparer<byte[]>.Compare(byte[] x, byte[] y)
        {
            return BinaryComparer.Compare(x, y);
        }

        /// <summary> Returns true if the two objects are the same instance </summary>
        bool IEqualityComparer<byte[]>.Equals(byte[] x, byte[] y)
        {
            return 0 == BinaryComparer.Compare(x, y);
        }

        /// <summary> Returns a hash code the instance of the object </summary>
        int IEqualityComparer<byte[]>.GetHashCode(byte[] bytes)
        {
            return BinaryComparer.GetHashCode(bytes);
        }
    }
}
