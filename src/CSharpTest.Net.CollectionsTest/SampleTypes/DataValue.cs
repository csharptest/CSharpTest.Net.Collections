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

using CSharpTest.Net.Collections.Test.Crypto;

namespace CSharpTest.Net.Collections.Test.SampleTypes
{
    public class DataValue
    {
        public readonly KeyInfo Key;
        private byte[] _bytes;

        public DataValue(KeyInfo key, byte[] data)
        {
            Key = key;
            Bytes = data;
        }

        public Hash Hash { get; private set; }

        public byte[] Bytes
        {
            get => (byte[]) _bytes.Clone();
            set
            {
                _bytes = (byte[]) value.Clone();
                Hash = Hash.SHA256(_bytes);
            }
        }
    }
}