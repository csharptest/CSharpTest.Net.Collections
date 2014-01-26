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
using System;
using System.Collections.Generic;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Represents a thread-safe generic collection of key/value pairs.
    /// </summary>
    public class SynchronizedDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDisposable,
                                                        IDictionaryEx<TKey, TValue>, IConcurrentDictionary<TKey, TValue>
    {
        private IDictionary<TKey, TValue> _store;
        private readonly ILockStrategy _lock;

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs using exclusive locking.
        /// </summary>
        public SynchronizedDictionary()
            : this(new Dictionary<TKey, TValue>(), new ExclusiveLocking())
        {
        }

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs using exclusive locking.
        /// </summary>
        public SynchronizedDictionary(IEqualityComparer<TKey> comparer)
            : this(new Dictionary<TKey, TValue>(comparer), new ExclusiveLocking())
        {
        }

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs using the lock provided.
        /// </summary>
        public SynchronizedDictionary(IEqualityComparer<TKey> comparer, ILockStrategy locking)
            : this(new Dictionary<TKey, TValue>(comparer), locking)
        {
        }

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs using the lock provided.
        /// </summary>
        public SynchronizedDictionary(ILockStrategy locking)
            : this(new Dictionary<TKey, TValue>(), locking)
        {
        }

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs using the default locking
        /// type for exclusive access, akin to placing lock(this) around each call.  If you want to
        /// allow reader/writer locking provide one of those lock types from the Synchronization
        /// namespace.
        /// </summary>
        public SynchronizedDictionary(IDictionary<TKey, TValue> storage)
            : this(storage, new ExclusiveLocking())
        {
        }

        /// <summary>
        /// Constructs a thread-safe generic collection of key/value pairs.
        /// </summary>
        public SynchronizedDictionary(IDictionary<TKey, TValue> storage, ILockStrategy locking)
        {
            _store = Check.NotNull(storage);
            _lock = Check.NotNull(locking);
        }

        /// <summary>
        /// Defines a method to release allocated resources.
        /// </summary>
        public void Dispose()
        {
            _lock.Dispose();

            if (_store is IDisposable)
                ((IDisposable) _store).Dispose();

            _store = null;
        }

        ///<summary> Exposes the interal lock so that you can syncronize several calls </summary>
        public ILockStrategy Lock
        {
            get { return _lock; }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        /// </summary>
        public bool IsReadOnly
        {
            get { return _store.IsReadOnly; }
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        public int Count
        {
            get
            {
                using (_lock.Read())
                    return _store.Count;
            }
        }

        /// <summary>
        /// Locks the collection and replaces the underlying storage dictionary.
        /// </summary>
        public IDictionary<TKey, TValue> ReplaceStorage(IDictionary<TKey, TValue> newStorage)
        {
            using (_lock.Write())
            {
                IDictionary<TKey, TValue> storage = _store;
                _store = Check.NotNull(newStorage);
                return storage;
            }
        }

        /// <summary>
        /// Gets or sets the element with the specified key.
        /// </summary>
        public TValue this[TKey key]
        {
            get
            {
                using (_lock.Read())
                    return _store[key];
            }
            set
            {
                using (_lock.Write())
                    _store[key] = value;
            }
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        public void Add(TKey key, TValue value)
        {
            using (_lock.Write())
                _store.Add(key, value);
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            using (_lock.Write())
                _store.Add(item);
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        public bool Remove(TKey key)
        {
            using (_lock.Write())
                return _store.Remove(key);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            using (_lock.Write())
                return _store.Remove(item);
        }

        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        public void Clear()
        {
            using (_lock.Write())
                _store.Clear();
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            using (_lock.Read())
                return _store.Contains(item);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.IDictionary`2"/> contains an element with the specified key.
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            using (_lock.Read())
                return _store.ContainsKey(key);
        }

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        public bool TryGetValue(TKey key, out TValue value)
        {
            using (_lock.Read())
                return _store.TryGetValue(key, out value);
        }

        /// <summary>
        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the keys of the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get
            {
                using (_lock.Read())
                    return new List<TKey>(_store.Keys);
            }
        }

        /// <summary>
        /// Gets an <see cref="T:System.Collections.Generic.ICollection`1"/> containing the values in the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        public ICollection<TValue> Values
        {
            get
            {
                using (_lock.Read())
                    return new List<TValue>(_store.Values);
            }
        }

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            using (_lock.Read())
                _store.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            using (_lock.Read())
            {
                foreach (KeyValuePair<TKey, TValue> kv in _store)
                    yield return kv;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #region IConcurrentDictionary<TKey,TValue> Members

        /// <summary>
        /// Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value to be added, if the key does not already exist.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public TValue GetOrAdd(TKey key, TValue value)
        {
            using (_lock.Write())
            {
                TValue found;
                if (_store.TryGetValue(key, out found))
                    return found;
                _store.Add(key, value);
                return value;
            }
        }

        /// <summary>
        /// Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="fnCreate">Constructs a new value for the key.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public TValue GetOrAdd(TKey key, Converter<TKey, TValue> fnCreate)
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value))
                    return value;
                _store.Add(key, value = fnCreate(key));
                return value;
            }
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <param name="key">The object to use as the key of the element to add.</param>
        /// <param name="value">The object to use as the value of the element to add.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public bool TryAdd(TKey key, TValue value)
        {
            using (_lock.Write())
            {
                if (_store.ContainsKey(key))
                    return false;
                _store.Add(key, value);
                return true;
            }
        }

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>
        /// by calling the provided factory method to construct the value if the key is not already present in the collection.
        /// </summary>
        public bool TryAdd(TKey key, Converter<TKey, TValue> fnCreate)
        {
            using (_lock.Write())
            {
                if (_store.ContainsKey(key))
                    return false;
                TValue value = fnCreate(key);
                _store.Add(key, value);
                return true;
            }
        }

        /// <summary>
        /// Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist, 
        /// or updates a key/value pair if the key already exists.
        /// </summary>
        public TValue AddOrUpdate(TKey key, TValue addValue, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value))
                {
                    value = fnUpdate(key, value);
                    _store[key] = value;
                    return value;
                }
                _store.Add(key, addValue);
                return addValue;
            }
        }

        /// <summary>
        /// Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist, 
        /// or updates a key/value pair if the key already exists.
        /// </summary>
        public TValue AddOrUpdate(TKey key, Converter<TKey, TValue> fnCreate, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value))
                {
                    value = fnUpdate(key, value);
                    _store[key] = value;
                    return value;
                }
                value = fnCreate(key);
                _store.Add(key, value);
                return value;
            }
        }

        /// <summary>
        /// Add, update, or fetche a key/value pair from the dictionary via an implementation of the
        /// <see cref="T:CSharpTest.Net.Collections.ICreateOrUpdateValue`2"/> interface.
        /// </summary>
        public bool AddOrUpdate<T>(TKey key, ref T createOrUpdateValue) where T : ICreateOrUpdateValue<TKey, TValue>
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value))
                {
                    if(createOrUpdateValue.UpdateValue(key, ref value))
                    {
                        _store[key] = value;
                        return true;
                    }
                    return false;
                }
                if (createOrUpdateValue.CreateValue(key, out value))
                {
                    _store.Add(key, value);
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public bool TryUpdate(TKey key, TValue value)
        {
            using (_lock.Write())
            {
                if (_store.ContainsKey(key))
                {
                    _store[key] = value;
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <param name="comparisonValue">The value that is compared to the value of the element with key.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public bool TryUpdate(TKey key, TValue value, TValue comparisonValue)
        {
            using (_lock.Write())
            {
                TValue test;
                if (_store.TryGetValue(key, out test) && EqualityComparer<TValue>.Default.Equals(comparisonValue, test))
                {
                    _store[key] = value;
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Modify the value associated with the result of the provided update method
        /// as an atomic operation, Allows for reading/writing a single record within
        /// the tree lock.  Be cautious about the behavior and performance of the code 
        /// provided as it can cause a dead-lock to occur.  If the method returns an
        /// instance who .Equals the original, no update is applied.
        /// </summary>
        public bool TryUpdate(TKey key, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value))
                {
                    _store[key] = fnUpdate(key, value);
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </summary>
        /// <returns>
        /// true if the element is successfully removed; otherwise, false.  This method also returns false if <paramref name="key"/> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2"/>.
        /// </returns>
        /// <param name="key">The key of the element to remove.</param>
        /// <param name="value">The value that was removed.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        public bool TryRemove(TKey key, out TValue value)
        {
            using (_lock.Write())
            {
                if (_store.TryGetValue(key, out value))
                {
                    _store.Remove(key);
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>
        /// if the fnCondition predicate is null or returns true.
        /// </summary>
        public bool TryRemove(TKey key, KeyValuePredicate<TKey, TValue> fnCondition)
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value) && fnCondition(key, value))
                {
                    _store.Remove(key);
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Conditionally removes a key/value pair from the dictionary via an implementation of the
        /// <see cref="T:CSharpTest.Net.Collections.IRemoveValue`2"/> interface.
        /// </summary>
        public bool TryRemove<T>(TKey key, ref T removeValue) where T : IRemoveValue<TKey, TValue>
        {
            using (_lock.Write())
            {
                TValue value;
                if (_store.TryGetValue(key, out value) && removeValue.RemoveValue(key, value))
                {
                    _store.Remove(key);
                    return true;
                }
                return false;
            }
        }

        #endregion
    }
}