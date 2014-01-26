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
using System.Text;
using CSharpTest.Net.Bases;
using CSharpTest.Net.Collections;
using System.Security.Cryptography;
using System.IO;
using CSharpTest.Net.IO;

namespace CSharpTest.Net.Crypto
{
    /// <summary> Represents a comparable, sortable, hash code </summary>
    public sealed class Hash : Comparable<Hash>
    {
        private static readonly byte[] Empty = new byte[0];

        private static Hash Create<T>(byte[] data)
            where T : HashAlgorithm, new()
        {
            using(T algo = new T())
                return new Hash(algo.ComputeHash(data == null ? Empty : data));
        }

        private static Hash Create<T>(Stream data)
            where T : HashAlgorithm, new()
        {
            using (data)
            using (T algo = new T())
                return new Hash(algo.ComputeHash(data == null ? Stream.Null : data));
        }

        /// <summary> Computes an MD5 hash </summary>
        public static Hash MD5(byte[] bytes) { return Create<MD5CryptoServiceProvider>(bytes); }
        /// <summary> Computes an MD5 hash </summary>
        public static Hash MD5(Stream bytes) { return Create<MD5CryptoServiceProvider>(bytes); }

        /// <summary> Computes an SHA1 hash </summary>
        public static Hash SHA1(byte[] bytes) { return Create<SHA1Managed>(bytes); }
        /// <summary> Computes an SHA1 hash </summary>
        public static Hash SHA1(Stream bytes) { return Create<SHA1Managed>(bytes); }

        /// <summary> Computes an SHA256 hash </summary>
        public static Hash SHA256(byte[] bytes) { return Create<SHA256Managed>(bytes); }
        /// <summary> Computes an SHA256 hash </summary>
        public static Hash SHA256(Stream bytes) { return Create<SHA256Managed>(bytes); }

        /// <summary> Computes an SHA384 hash </summary>
        public static Hash SHA384(byte[] bytes) { return Create<SHA384Managed>(bytes); }
        /// <summary> Computes an SHA384 hash </summary>
        public static Hash SHA384(Stream bytes) { return Create<SHA384Managed>(bytes); }

        /// <summary> Computes an SHA512 hash </summary>
        public static Hash SHA512(byte[] bytes) { return Create<SHA512Managed>(bytes); }
        /// <summary> Computes an SHA512 hash </summary>
        public static Hash SHA512(Stream bytes) { return Create<SHA512Managed>(bytes); }

        /// <summary> Creates a comparable Hash object from the given hashcode bytes </summary>
        public static Hash FromBytes(byte[] bytes) { return new Hash((byte[])bytes.Clone()); }
        /// <summary> Creates a comparable Hash object from the base-64 encoded hashcode bytes </summary>
        public static Hash FromString(string encodedBytes) { return new Hash(Convert.FromBase64String(encodedBytes)); }

        private readonly byte[] _hashCode;
        private Hash(byte[] hashCode)
        {
            int sz = Check.NotNull(hashCode).Length;
            Check.Assert<ArgumentOutOfRangeException>(sz == 16 || sz == 20 || sz == 32 || sz == 48 || sz == 64);
            _hashCode = hashCode;
        }

        /// <summary>
        /// Creates the hash algorithm associated with this length of hash
        /// </summary>
        public HashAlgorithm CreateAlgorithm()
        {
            return Check.IsAssignable<HashAlgorithm>(CryptoConfig.CreateFromName(AlgorithmName));
        }

        /// <summary>
        /// If the hash provided is the same size as this hash both hash codes are feed back into
        /// the hash algorithm associated with this length of hash to produce the result value.
        /// If the hash provided is a different length, it is first hashed with this algorithm
        /// before the two values are combined.
        /// </summary>
        public Hash Combine(Hash other)
        {
            if (Length != other.Length)
                return Combine(other._hashCode);

            //We have two hash values of equal lenth, we can now easily combine them.
            HashAlgorithm alg = CreateAlgorithm();
            alg.TransformBlock(_hashCode, 0, _hashCode.Length, _hashCode, 0);
            alg.TransformFinalBlock(other._hashCode, 0, _hashCode.Length);
            return FromBytes(alg.Hash);
        }

        /// <summary>
        /// Combines the bytes provided by first computing a like sized hash of those bytes and
        /// then combining the two equal hash values with the same hash algorithm.
        /// </summary>
        public Hash Combine(byte[] bytes)
        {
            return Combine(FromBytes(CreateAlgorithm().ComputeHash(Check.NotNull(bytes))));
        }

        /// <summary> Returns the OID of the hash algorithm </summary>
        public string AlgorithmOID
        { get { return CryptoConfig.MapNameToOID(AlgorithmName); } }

        /// <summary> Returns the name of the hash algorithm </summary>
        public string AlgorithmName
        {
            get
            {
                switch (_hashCode.Length)
                {
                    case 16: return ("MD5");
                    case 20: return ("SHA1");
                    case 32: return ("SHA256");
                    case 48: return ("SHA384");
                    case 64: return ("SHA512");
                    default: throw new ArgumentOutOfRangeException();
                }
            }
        }

        /// <summary> Returns the length in bytes of the hash code </summary>
        public int Length { get { return _hashCode.Length; } }

        /// <summary> Returns a copy of the hash code bytes </summary>
        public byte[] ToArray() { return (byte[])_hashCode.Clone(); }

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

        /// <summary> Returns a hash of the hash code :) </summary>
        protected override int HashCode
        {
            get { return BinaryComparer.GetHashCode(_hashCode); }
        }
    }
}
