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
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test.SampleTypes
{
    class DataValueSerializer : ISerializer<DataValue>
    {
        readonly KeyInfoSerializer _keySerializer = new KeyInfoSerializer();

        public DataValue ReadFrom(System.IO.Stream stream)
        {
            DataValue value = new DataValue(
                _keySerializer.ReadFrom(stream),
                PrimitiveSerializer.Bytes.ReadFrom(stream)
                );

            Hash cmpHash = Hash.FromString(PrimitiveSerializer.String.ReadFrom(stream));
            Assert.AreEqual(value.Hash, cmpHash);
            return value;
        }

        public void WriteTo(DataValue value, System.IO.Stream stream)
        {
            _keySerializer.WriteTo(value.Key, stream);
            PrimitiveSerializer.Bytes.WriteTo(value.Bytes, stream);
            PrimitiveSerializer.String.WriteTo(value.Hash.ToString(), stream);
        }
    }
}