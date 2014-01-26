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
using System;

namespace CSharpTest.Net.Collections
{
    /// <summary> Provides a delegate that performs an atomic update of a key/value pair </summary>
    public delegate TValue KeyValueUpdate<TKey, TValue>(TKey key, TValue original);

    /// <summary> Provides a delegate that performs a test on key/value pair </summary>
    public delegate bool KeyValuePredicate<TKey, TValue>(TKey key, TValue original);

    /// <summary>
    /// An interface to provide conditional or custom creation logic to a concurrent dictionary.
    /// </summary>
    public interface ICreateValue<TKey, TValue>
    {
        /// <summary>
        /// Called when the key was not found within the dictionary to produce a new value that can be added.
        /// Return true to continue with the insertion, or false to prevent the key/value from being inserted.
        /// </summary>
        bool CreateValue(TKey key, out TValue value);
    }
    /// <summary>
    /// An interface to provide conditional or custom update logic to a concurrent dictionary.
    /// </summary>
    public interface IUpdateValue<TKey, TValue>
    {
        /// <summary>
        /// Called when the key was found within the dictionary to produce a modified value to update the item
        /// to. Return true to continue with the update, or false to prevent the key/value from being updated.
        /// </summary>
        bool UpdateValue(TKey key, ref TValue value);
    }
    /// <summary>
    /// An interface to provide conditional or custom creation or update logic to a concurrent dictionary.
    /// </summary>
    /// <remarks>
    /// Generally implemented as a struct and passed by ref to save stack space and to retrieve the values
    /// that where inserted or updated.
    /// </remarks>
    public interface ICreateOrUpdateValue<TKey, TValue> : ICreateValue<TKey, TValue>, IUpdateValue<TKey, TValue>
    {
    }

    /// <summary>
    /// An interface to provide conditional removal of an item from a concurrent dictionary.
    /// </summary>
    /// <remarks>
    /// Generally implemented as a struct and passed by ref to save stack space and to retrieve the values
    /// that where inserted or updated.
    /// </remarks>
    public interface IRemoveValue<TKey, TValue>
    {
        /// <summary>
        /// Called when the dictionary is about to remove the key/value pair provided, return true to allow
        /// it's removal, or false to prevent it from being removed.
        /// </summary>
        bool RemoveValue(TKey key, TValue value);
    }

    /// <summary>
    /// Extends the IDictionaryEx interface to encompass concurrent/atomic operations
    /// </summary>
    public interface IConcurrentDictionary<TKey, TValue> : IDictionaryEx<TKey, TValue>
    {
        /// <summary>
        /// Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="fnCreate">Constructs a new value for the key.</param>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IDictionary`2"/> is read-only.</exception>
        TValue GetOrAdd(TKey key, Converter<TKey, TValue> fnCreate);

        /// <summary>
        /// Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist, 
        /// or updates a key/value pair if the key already exists.
        /// </summary>
        TValue AddOrUpdate(TKey key, TValue addValue, KeyValueUpdate<TKey, TValue> fnUpdate);

        /// <summary>
        /// Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2"/> if the key does not already exist, 
        /// or updates a key/value pair if the key already exists.
        /// </summary>
        /// <remarks>
        /// Adds or modifies an element with the provided key and value.  If the key does not exist in the collection,
        /// the factory method fnCreate will be called to produce the new value, if the key exists, the converter method
        /// fnUpdate will be called to create an updated value.
        /// </remarks>
        TValue AddOrUpdate(TKey key, Converter<TKey, TValue> fnCreate, KeyValueUpdate<TKey, TValue> fnUpdate);

        /// <summary>
        /// Add, update, or fetche a key/value pair from the dictionary via an implementation of the
        /// <see cref="T:CSharpTest.Net.Collections.ICreateOrUpdateValue`2"/> interface.
        /// </summary>
        bool AddOrUpdate<T>(TKey key, ref T createOrUpdateValue) where T : ICreateOrUpdateValue<TKey, TValue>;

        /// <summary>
        /// Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2"/>
        /// by calling the provided factory method to construct the value if the key is not already present in the collection.
        /// </summary>
        bool TryAdd(TKey key, Converter<TKey, TValue> fnCreate);

        /// <summary>
        /// Modify the value associated with the result of the provided update method
        /// as an atomic operation, Allows for reading/writing a single record within
        /// the tree lock.  Be cautious about the behavior and performance of the code 
        /// provided as it can cause a dead-lock to occur.  If the method returns an
        /// instance who .Equals the original, no update is applied.
        /// </summary>
        bool TryUpdate(TKey key, KeyValueUpdate<TKey, TValue> fnUpdate);

        /// <summary>
        /// Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2"/>
        /// if the fnCondition predicate is null or returns true.
        /// </summary>
        bool TryRemove(TKey key, KeyValuePredicate<TKey, TValue> fnCondition);

        /// <summary>
        /// Conditionally removes a key/value pair from the dictionary via an implementation of the
        /// <see cref="T:CSharpTest.Net.Collections.IRemoveValue`2"/> interface.
        /// </summary>
        bool TryRemove<T>(TKey key, ref T removeValue) where T : IRemoveValue<TKey, TValue>;
    }
}