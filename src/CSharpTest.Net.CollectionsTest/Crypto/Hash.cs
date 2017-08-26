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
using System.IO;
using System.Security.Cryptography;
using CSharpTest.Net.Bases;
using CSharpTest.Net.IO;

namespace CSharpTest.Net.Crypto
{
    /// <summary> Represents a comparable, sortable, hash code </summary>
    public sealed class Hash : Comparable<Hash>
    {
        private static readonly byte[] Empty = new byte[0];

        private readonly byte[] _hashCode;

        private Hash(byte[] hashCode)
        {
            int sz = Check.NotNull(hashCode).Length;
            Check.Assert<ArgumentOutOfRangeException>(sz == 16 || sz == 20 || sz == 32 || sz == 48 || sz == 64);
            _hashCode = hashCode;
        }

        /// <summary> Returns the length in bytes of the hash code </summary>
        public int Length => _hashCode.Length;

        /// <summary> Returns a hash of the hash code :) </summary>
        protected override int HashCode => BinaryComparer.GetHashCode(_hashCode);

        /// <summary> Computes an SHA256 hash </summary>
        public static Hash SHA256(byte[] bytes)
        {
            using (SHA256 sha256 = System.Security.Cryptography.SHA256.Create())
            {
                return new Hash(sha256.ComputeHash(bytes));
            }
        }

        /// <summary> Creates a comparable Hash object from the given hashcode bytes </summary>
        public static Hash FromBytes(byte[] bytes)
        {
            return new Hash((byte[])bytes.Clone());
        }

        /// <summary> Creates a comparable Hash object from the base-64 encoded hashcode bytes </summary>
        public static Hash FromString(string encodedBytes)
        {
            return new Hash(Convert.FromBase64String(encodedBytes));
        }

        /// <summary>
        ///     Creates the hash algorithm associated with this length of hash
        /// </summary>
        public IncrementalHash CreateAlgorithm()
        {
            return IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        }

        /// <summary>
        ///     If the hash provided is the same size as this hash both hash codes are feed back into
        ///     the hash algorithm associated with this length of hash to produce the result value.
        ///     If the hash provided is a different length, it is first hashed with this algorithm
        ///     before the two values are combined.
        /// </summary>
        public Hash Combine(Hash other)
        {
            if (Length != other.Length)
                return Combine(other._hashCode);

            //We have two hash values of equal lenth, we can now easily combine them.
            IncrementalHash alg = CreateAlgorithm();
            alg.AppendData(_hashCode, 0, _hashCode.Length);
            alg.AppendData(other._hashCode, 0, _hashCode.Length);
            return FromBytes(alg.GetHashAndReset());
        }

        /// <summary>
        ///     Combines the bytes provided by first computing a like sized hash of those bytes and
        ///     then combining the two equal hash values with the same hash algorithm.
        /// </summary>
        public Hash Combine(byte[] bytes)
        {
            return Combine(SHA256(Check.NotNull(bytes)));
        }

        /// <summary> Returns a copy of the hash code bytes </summary>
        public byte[] ToArray()
        {
            return (byte[])_hashCode.Clone();
        }

        /// <summary> Returns the hash code as a base-64 encoded string </summary>
        public override string ToString()
        {
            return Convert.ToBase64String(_hashCode);
        }

        /// <summary> Compares the hash codes and returns the result </summary>
        public override int CompareTo(Hash other)
        {
            return other == null ? 1 : BinaryComparer.Compare(_hashCode, other._hashCode);
        }
    }
}