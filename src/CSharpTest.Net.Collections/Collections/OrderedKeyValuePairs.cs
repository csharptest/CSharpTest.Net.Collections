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
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Speicalizes the OrderedEnumeration of T to use key/value pairs with a key comparer.
    /// </summary>
    public class OrderedKeyValuePairs<TKey, TValue> : OrderedEnumeration<KeyValuePair<TKey, TValue>>
    {
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IEnumerable<KeyValuePair<TKey, TValue>> unordered)
            : base(KeyValueComparer<TKey, TValue>.Default, unordered)
        {
        }
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> unordered)
            : base(new KeyValueComparer<TKey, TValue>(comparer), unordered)
        {
        }
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> unordered,
                                    ISerializer<KeyValuePair<TKey, TValue>> serializer)
            : base(new KeyValueComparer<TKey, TValue>(comparer), unordered, serializer)
        {
        }
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> unordered,
                                    ISerializer<KeyValuePair<TKey, TValue>> serializer, int memoryLimit)
            : base(new KeyValueComparer<TKey, TValue>(comparer), unordered, serializer, memoryLimit)
        {
        }
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> unordered,
                                    ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            : base(
                new KeyValueComparer<TKey, TValue>(comparer), unordered,
                new KeyValueSerializer<TKey, TValue>(keySerializer, valueSerializer))
        {
        }
        /// <summary> Constructs an ordered set of KeyValuePair structs </summary>
        public OrderedKeyValuePairs(IComparer<TKey> comparer, IEnumerable<KeyValuePair<TKey, TValue>> unordered,
                                    ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer,
                                    int memoryLimit)
            : base(
                new KeyValueComparer<TKey, TValue>(comparer), unordered,
                new KeyValueSerializer<TKey, TValue>(keySerializer, valueSerializer), memoryLimit)
        {
        }
        /// <summary>
        /// Merges n-number of ordered enumerations based on the comparer provided.
        /// </summary>
        public static IEnumerable<KeyValuePair<TKey, TValue>> Merge(IComparer<TKey> comparer, params IEnumerable<KeyValuePair<TKey, TValue>>[] enums)
        {
            return Merge(new KeyValueComparer<TKey, TValue>(comparer), enums);
        }
        /// <summary>
        /// Merges n-number of ordered enumerations based on the comparer provided.
        /// </summary>
        public static IEnumerable<KeyValuePair<TKey, TValue>> Merge(IComparer<TKey> comparer, DuplicateHandling duplicateHandling, params IEnumerable<KeyValuePair<TKey, TValue>>[] enums)
        {
            KeyValueComparer<TKey, TValue> kvcompare = new KeyValueComparer<TKey, TValue>(comparer);
            return WithDuplicateHandling(Merge(kvcompare, enums), kvcompare, duplicateHandling);
        }
    }
}
