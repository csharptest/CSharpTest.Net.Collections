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
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.BPlusTree.Test.SampleTypes
{
    class KeyInfoSerializer : ISerializer<KeyInfo>
    {
        public KeyInfo ReadFrom(System.IO.Stream stream)
        {
            return new KeyInfo(
                PrimitiveSerializer.Guid.ReadFrom(stream),
                VariantNumberSerializer.Int32.ReadFrom(stream)
                );
        }
        public void WriteTo(KeyInfo value, System.IO.Stream stream)
        {
            PrimitiveSerializer.Guid.WriteTo(value.UID, stream);
            VariantNumberSerializer.Int32.WriteTo(value.Version, stream);
        }
    }
}