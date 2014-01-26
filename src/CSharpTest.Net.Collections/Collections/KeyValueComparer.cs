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

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Represents a key-value comparison
    /// </summary>
    public class KeyValueComparer<TKey, TValue> : IComparer<KeyValuePair<TKey, TValue>>
    {
        private readonly IComparer<TKey> _keyComparer;

        private static KeyValueComparer<TKey, TValue> _defaultInstance;
        /// <summary>
        /// Represents a key-value comparison using the default comparer for type TKey
        /// </summary>
        public static KeyValueComparer<TKey, TValue> Default 
        {
            get
            {
                return _defaultInstance ??
                       (_defaultInstance = new KeyValueComparer<TKey, TValue>(Comparer<TKey>.Default));
            }
        }

        /// <summary>
        /// Returns the comparer being used by this instance
        /// </summary>
        public IComparer<TKey> Comparer { get { return _keyComparer; } }

            /// <summary>
        /// Creates a key-value comparison using the default comparer for type TKey
        /// </summary>
        public KeyValueComparer() : this(Comparer<TKey>.Default) { }

        /// <summary>
        /// Creates a key-value comparison with the specified comparer
        /// </summary>
        public KeyValueComparer(IComparer<TKey> keyComparer)
        {
            _keyComparer = Check.NotNull(keyComparer);
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        public int Compare(KeyValuePair<TKey,TValue> x, KeyValuePair<TKey,TValue> y)
        {
            return _keyComparer.Compare(x.Key, y.Key);
        }
    }
}