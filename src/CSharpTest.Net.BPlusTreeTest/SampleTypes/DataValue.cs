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
using CSharpTest.Net.Crypto;

namespace CSharpTest.Net.BPlusTree.Test.SampleTypes
{
    public class DataValue
    {
        private Hash _hash;
        private byte[] _bytes;
        public DataValue(KeyInfo key, byte[] data)
        {
            Key = key;
            Bytes = data;
        }

        public readonly KeyInfo Key;

        public Hash Hash { get { return _hash; } }
        public byte[] Bytes
        {
            get { return (byte[])_bytes.Clone(); }
            set
            {
                _bytes = (byte[])value.Clone();
                _hash = Hash.SHA256(_bytes);
            }
        }
    }
}