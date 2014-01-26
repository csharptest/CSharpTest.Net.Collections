#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Collections.Generic;

namespace CSharpTest.Net.Serialization
{
    /// <summary>
    /// Implements ISerializer of KeyValuePair&lt;TKey, TValue&gt;
    /// </summary>
    public sealed class KeyValueSerializer<TKey, TValue> : ISerializer<KeyValuePair<TKey, TValue>>
    {
        private readonly ISerializer<TKey> _keySerializer;
        private readonly ISerializer<TValue> _valueSerializer;

        /// <summary>
        /// Provide the key/value serializers to use.
        /// </summary>
        public KeyValueSerializer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
        }

        /// <summary> Writes the object to the stream </summary>
        public void WriteTo(KeyValuePair<TKey, TValue> value, System.IO.Stream stream)
        {
            _keySerializer.WriteTo(value.Key, stream);
            _valueSerializer.WriteTo(value.Value, stream);
        }

        /// <summary> Reads the object from a stream </summary>
        public KeyValuePair<TKey, TValue> ReadFrom(System.IO.Stream stream)
        {
            return new KeyValuePair<TKey, TValue>(
                _keySerializer.ReadFrom(stream),
                _valueSerializer.ReadFrom(stream)
            );
        }
    }
}
