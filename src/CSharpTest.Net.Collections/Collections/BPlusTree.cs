﻿#region Copyright 2011-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using CSharpTest.Net.Collections.Exceptions;
using CSharpTest.Net.Interfaces;


namespace CSharpTest.Net.Collections
{
    /// <summary>
    ///     Implements an IDictionary interface for a simple file-based database
    /// </summary>
    public partial class BPlusTree<TKey, TValue> : ITransactable, IConcurrentDictionary<TKey, TValue>
    {
        private readonly IComparer<Element> _itemComparer;
        private readonly IComparer<TKey> _keyComparer;
        private readonly BPlusTreeOptions<TKey, TValue> _options;
        private readonly NodeCacheBase _storage;
        private int _count;

        private bool _disposed;
        private bool _hasCount;

        /// <summary>
        ///     Constructs an in-memory BPlusTree
        /// </summary>
        public BPlusTree() : this(Comparer<TKey>.Default)
        {
        }

        /// <summary>
        ///     Constructs an in-memory BPlusTree
        /// </summary>
        public BPlusTree(IComparer<TKey> comparer)
            : this((BPlusTreeOptions<TKey, TValue>)
                new Options(InvalidSerializer<TKey>.Instance,
                    InvalidSerializer<TValue>.Instance, comparer))
        {
        }

        /// <summary>
        ///     Constructs a BPlusTree using a Version 2 file format
        /// </summary>
        public BPlusTree(OptionsV2 optionsV2)
            : this((BPlusTreeOptions<TKey, TValue>)optionsV2)
        {
        }

        /// <summary>
        ///     Constructs a BPlusTree using a Version 1 file format
        /// </summary>
        public BPlusTree(Options optionsV1)
            : this((BPlusTreeOptions<TKey, TValue>)optionsV1)
        {
        }

        /// <summary>
        ///     Constructs a BPlusTree
        /// </summary>
        public BPlusTree(BPlusTreeOptions<TKey, TValue> ioptions)
        {
            bool fileExists =
                ioptions.StorageType == StorageType.Disk &&
                ioptions.CreateFile != CreatePolicy.Always &&
                File.Exists(ioptions.FileName) &&
                new FileInfo(ioptions.FileName).Length > 0;

            _options = ioptions.Clone();
            _keyComparer = _options.KeyComparer;
            _itemComparer = new ElementComparer(_keyComparer);

            switch (_options.CachePolicy)
            {
                case CachePolicy.All:
                    _storage = new NodeCacheFull(_options);
                    break;
                case CachePolicy.Recent:
                    _storage = new NodeCacheNormal(_options);
                    break;
                case CachePolicy.None:
                    _storage = new NodeCacheNone(_options);
                    break;
                default: throw new InvalidConfigurationValueException("CachePolicy");
            }

            try
            {
                _storage.Load();
            }
            catch
            {
                _storage.Dispose();
                throw;
            }

            if (_options.LogFile != null && !_options.ReadOnly)
                if (_options.ExistingLogAction == ExistingLogAction.Truncate ||
                    _options.ExistingLogAction == ExistingLogAction.Default && !fileExists)
                {
                    _options.LogFile.TruncateLog();
                }
                else if (_options.LogFile.Size > 0 && (
                             _options.ExistingLogAction == ExistingLogAction.Replay ||
                             _options.ExistingLogAction == ExistingLogAction.ReplayAndCommit ||
                             _options.ExistingLogAction == ExistingLogAction.Default && fileExists
                         ))
                {
                    bool commit = _options.ExistingLogAction == ExistingLogAction.ReplayAndCommit ||
                                 _options.ExistingLogAction == ExistingLogAction.Default && fileExists;

                    bool merge = false;
                    if (_options.StorageType == StorageType.Disk)
                        merge = new FileInfo(_options.FileName).Length < _options.LogFile.Size;

                    if (merge) // log data is larger than we are...
                    {
                        BulkInsertOptions opts = new BulkInsertOptions();
                        opts.CommitOnCompletion = commit;
                        opts.DuplicateHandling = DuplicateHandling.LastValueWins;
                        opts.InputIsSorted = true;
                        opts.ReplaceContents = true;
                        BulkInsert(
                            _options.LogFile.MergeLog(_options.KeyComparer,
                                File.Exists(ioptions.FileName)
                                    ? EnumerateFile(ioptions)
                                    : new KeyValuePair<TKey, TValue>[0]),
                            opts
                        );
                    }
                    else
                    {
                        _options.LogFile.ReplayLog(this);
                        if (commit) //Now commit the recovered changes
                            Commit();
                    }
                }

            INodeStoreWithCount nodeStoreWithCount = _storage.Storage as INodeStoreWithCount;
            if (nodeStoreWithCount != null)
            {
                _count = nodeStoreWithCount.Count;
                _hasCount = _count >= 0;
            }
        }

        /// <summary>
        ///     Modify the value associated with the result of the provided update method
        ///     as an atomic operation, Allows for reading/writing a single record within
        ///     the tree lock.  Be cautious about the behavior and performance of the code
        ///     provided as it can cause a dead-lock to occur.  If the method returns an
        ///     instance who .Equals the original, no update is applied.
        /// </summary>
        public bool TryUpdate(TKey key, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            UpdateInfo ui = new UpdateInfo(fnUpdate);
            bool result;
            using (RootLock root = LockRoot(LockType.Update))
            {
                result = Update(root.Pin, key, ref ui);
            }
            DebugComplete("Updated({0}) = {1}", key, result);
            return ui.Updated;
        }

        /// <summary>
        ///     Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2" /> if the key does not already
        ///     exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="fnCreate">Constructs a new value for the key.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> fnCreate)
        {
            InsertionInfo ii = new InsertionInfo(fnCreate, IgnoreUpdate);
            AddEntry(key, ref ii);
            return ii.Value;
        }

        /// <summary>
        ///     Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2" />
        ///     by calling the provided factory method to construct the value if the key is not already present in the collection.
        /// </summary>
        public bool TryAdd(TKey key, Func<TKey, TValue> fnCreate)
        {
            InsertionInfo ii = new InsertionInfo(fnCreate, IgnoreUpdate);
            return InsertResult.Inserted == AddEntry(key, ref ii);
        }

        /// <summary>
        ///     Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2" /> if the key does not already
        ///     exist,
        ///     or updates a key/value pair if the key already exists.
        /// </summary>
        public TValue AddOrUpdate(TKey key, TValue addValue, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            InsertionInfo ii = new InsertionInfo(addValue, fnUpdate);
            AddEntry(key, ref ii);
            return ii.Value;
        }

        /// <summary>
        ///     Adds a key/value pair to the <see cref="T:System.Collections.Generic.IDictionary`2" /> if the key does not already
        ///     exist,
        ///     or updates a key/value pair if the key already exists.
        /// </summary>
        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> fnCreate, KeyValueUpdate<TKey, TValue> fnUpdate)
        {
            InsertionInfo ii = new InsertionInfo(fnCreate, fnUpdate);
            AddEntry(key, ref ii);
            return ii.Value;
        }

        /// <summary>
        ///     Add, update, or fetche a key/value pair from the dictionary via an implementation of the
        ///     <see cref="T:CSharpTest.Net.Collections.ICreateOrUpdateValue`2" /> interface.
        /// </summary>
        public bool AddOrUpdate<T>(TKey key, ref T createOrUpdateValue) where T : ICreateOrUpdateValue<TKey, TValue>
        {
            InsertResult result = AddEntry(key, ref createOrUpdateValue);
            return result == InsertResult.Inserted || result == InsertResult.Updated;
        }

        /// <summary>
        ///     Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2" />
        ///     if the fnCondition predicate is null or returns true.
        /// </summary>
        public bool TryRemove(TKey key, KeyValuePredicate<TKey, TValue> fnCondition)
        {
            RemoveIfPredicate ri = new RemoveIfPredicate(fnCondition);
            return RemoveEntry(key, ref ri) == RemoveResult.Removed;
        }

        /// <summary>
        ///     Conditionally removes a key/value pair from the dictionary via an implementation of the
        ///     <see cref="T:CSharpTest.Net.Collections.IRemoveValue`2" /> interface.
        /// </summary>
        public bool TryRemove<T>(TKey key, ref T removeValue) where T : IRemoveValue<TKey, TValue>
        {
            return RemoveEntry(key, ref removeValue) == RemoveResult.Removed;
        }

        /// <summary> See comments on EnableCount() for usage of this property </summary>
        public int Count => _hasCount ? _count : int.MinValue;

        /// <summary>
        ///     Gets or sets the element with the specified key.
        /// </summary>
        public TValue this[TKey key]
        {
            get
            {
                TValue result;
                if (!TryGetValue(key, out result))
                    throw new IndexOutOfRangeException();
                return result;
            }
            set
            {
                InsertValue ii = new InsertValue(value, true);
                AddEntry(key, ref ii);
            }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            return ContainsKey(item.Key);
        }

        /// <summary>
        ///     Determines whether the <see cref="T:System.Collections.Generic.IDictionary`2" /> contains an element with the
        ///     specified key.
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            return TryGetValue(key, out _);
        }

        /// <summary>
        ///     Gets the value associated with the specified key.
        /// </summary>
        public bool TryGetValue(TKey key, out TValue value)
        {
            bool result;
            value = default(TValue);
            using (RootLock root = LockRoot(LockType.Read))
            {
                result = Search(root.Pin, key, ref value);
            }
            DebugComplete("Found({0}) = {1}", key, result);
            return result;
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            InsertValue ii = new InsertValue(item.Value, false);
            AddEntry(item.Key, ref ii);
        }

        /// <summary>
        ///     Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public void Add(TKey key, TValue value)
        {
            InsertValue ii = new InsertValue(value, false);
            AddEntry(key, ref ii);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            RemoveIfValue ri = new RemoveIfValue(item.Key, item.Value);
            return RemoveEntry(item.Key, ref ri) == RemoveResult.Removed;
        }

        /// <summary>
        ///     Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public bool Remove(TKey key)
        {
            RemoveAlways ri = new RemoveAlways();
            return RemoveEntry(key, ref ri) == RemoveResult.Removed;
        }

        /// <summary>
        ///     Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new Enumerator(this);
        }

        [Obsolete]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        ///     Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public bool TryUpdate(TKey key, TValue value)
        {
            UpdateInfo ui = new UpdateInfo(value);
            bool result;
            using (RootLock root = LockRoot(LockType.Update))
            {
                result = Update(root.Pin, key, ref ui);
            }
            DebugComplete("Updated({0}) = {1}", key, result);
            return ui.Updated;
        }

        /// <summary>
        ///     Updates an element with the provided key to the value if it exists.
        /// </summary>
        /// <returns>Returns true if the key provided was found and updated to the value.</returns>
        /// <param name="key">The object to use as the key of the element to update.</param>
        /// <param name="value">The new value for the key if found.</param>
        /// <param name="comparisonValue">The value that is compared to the value of the element with key.</param>
        public bool TryUpdate(TKey key, TValue value, TValue comparisonValue)
        {
            UpdateIfValue ui = new UpdateIfValue(value, comparisonValue);
            bool result;
            using (RootLock root = LockRoot(LockType.Update))
            {
                result = Update(root.Pin, key, ref ui);
            }
            DebugComplete("Updated({0}) = {1}", key, result);
            return ui.Updated;
        }

        /// <summary>
        ///     Adds a key/value pair to the  <see cref="T:System.Collections.Generic.IDictionary`2" /> if the key does not already
        ///     exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">The value to be added, if the key does not already exist.</param>
        /// <exception cref="T:System.NotSupportedException">
        ///     The <see cref="T:System.Collections.Generic.IDictionary`2" /> is
        ///     read-only.
        /// </exception>
        public TValue GetOrAdd(TKey key, TValue value)
        {
            FetchValue ii = new FetchValue(value);
            AddEntry(key, ref ii);
            return ii.Value;
        }

        /// <summary>
        ///     Adds an element with the provided key and value to the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public bool TryAdd(TKey key, TValue value)
        {
            FetchValue ii = new FetchValue(value);
            return InsertResult.Inserted == AddEntry(key, ref ii);
        }

        /// <summary>
        ///     Removes the element with the specified key from the <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        /// <returns>
        ///     true if the element is successfully removed; otherwise, false.  This method also returns false if
        ///     <paramref name="key" /> was not found in the original <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </returns>
        /// <param name="key">The key of the element to remove.</param>
        /// <param name="value">The value that was removed.</param>
        public bool TryRemove(TKey key, out TValue value)
        {
            RemoveAlways ri = new RemoveAlways();
            if (RemoveEntry(key, ref ri) == RemoveResult.Removed && ri.TryGetValue(out value))
                return true;
            value = default(TValue);
            return false;
        }

        /// <summary>
        ///     Closes the storage and clears memory used by the instance
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            try
            {
                try
                {
                    if (!IsReadOnly)
                    {
                        CommitChanges();
                    }
                    _storage.Dispose();
                }
                finally
                {
                    if (_options.LogFile != null)
                        _options.LogFile.Dispose();
                }
            }
            finally
            {
                _disposed = true;
            }
        }

        /// <summary>
        ///     When using TransactionLog, this method commits the changes in the current
        ///     instance to the output file and truncates the log.  For all other cases the method is a
        ///     no-op and no exception is raised.  This method is NOT thread safe UNLESS the CallLevelLock
        ///     property has been set to valid reader/writer lock.  If you need to call this method while
        ///     writers are currently accessing the tree, make sure the CallLevelLock options is specified.
        /// </summary>
        public void Commit()
        {
            CommitChanges();
        }

        /// <summary>
        ///     With version 2 storage this will revert the contents of tree to it's initial state when
        ///     the file was first opened, or to the state captured with the last call to commit.  Any
        ///     transaction log data will be truncated.
        /// </summary>
        /// <exception cref="InvalidOperationException">Raised when called for a BPlusTree that is not using v2 files</exception>
        public void Rollback()
        {
            NotDisposed();

            if (_storage.Storage is ITransactable)
                ((ITransactable)_storage.Storage).Rollback();
            else
                throw new InvalidOperationException();

            if (_options.LogFile != null)
                _options.LogFile.TruncateLog();

            _storage.ResetCache();

            //Finally we may need to rebuild count
            if (_hasCount)
            {
                _hasCount = false;
                EnableCount();
            }
        }

        private void NotDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(GetType().FullName);
        }

        private void CommitChanges()
        {
            NotDisposed();
            if (_storage.Storage is INodeStoreWithCount)
                ((INodeStoreWithCount)_storage.Storage).Count = _hasCount ? _count : -1;
            if (_storage.Storage is ITransactable)
                ((ITransactable)_storage.Storage).Commit();
            if (_options.LogFile != null)
                _options.LogFile.TruncateLog();
        }

        private void OnChanged()
        {
            if (_options.TransactionLogLimit > 0 && _options.LogFile != null)
                if (_options.LogFile.Size > _options.TransactionLogLimit)
                {
                    if (_options.LogFile.Size > _options.TransactionLogLimit)
                        CommitChanges();
                }
        }

        /// <summary>
        ///     Due to the cost of upkeep, this must be enable each time the object is created via a call to
        ///     EnableCount() which itself must be done before any writer threads are active for it to be
        ///     accurate.  This requires that the entire tree be loaded (sequentially) in order to build
        ///     the initial working count.  Once completed, members like Add() and Remove() will keep the
        ///     initial count accurate.
        /// </summary>
        public void EnableCount()
        {
            if (_hasCount)
                return;
            _count = 0;
            using (RootLock root = LockRoot(LockType.Read))
            {
                _count = CountValues(root.Pin);
            }
            _hasCount = true;
        }

        /// <summary>
        ///     Safely removes all items from the in-memory cache.
        /// </summary>
        public void UnloadCache()
        {
            NotDisposed();
            _storage.ResetCache();
        }

        private RootLock LockRoot(LockType ltype)
        {
            return new RootLock(this, ltype);
        }

        /// <summary>
        ///     Presorts the provided enumeration in batches and then performs an optimized insert on the resulting set(s).
        /// </summary>
        /// <param name="unorderedItems">The items to insert</param>
        public void AddRange(IEnumerable<KeyValuePair<TKey, TValue>> unorderedItems)
        {
            AddRange(unorderedItems, false);
        }

        /// <summary>
        ///     Presorts the provided enumeration in batches and then performs an optimized insert on the resulting set(s).
        /// </summary>
        /// <param name="unorderedItems">The items to insert</param>
        /// <param name="allowUpdates">True to overwrite any existing records</param>
        /// <returns>The total number of records inserted or updated</returns>
        public int AddRange(IEnumerable<KeyValuePair<TKey, TValue>> unorderedItems, bool allowUpdates)
        {
            int total = 0;
            int count = 0;

            KeyValuePair<TKey, TValue>[] items = new KeyValuePair<TKey, TValue>[2048];
            KeyValueComparer<TKey, TValue> kvcomparer = new KeyValueComparer<TKey, TValue>(_keyComparer);

            foreach (KeyValuePair<TKey, TValue> item in unorderedItems)
            {
                if (count == 0x010000)
                {
                    MergeSort.Sort(items, kvcomparer);
                    total += AddRangeSorted(items, allowUpdates);
                    Array.Clear(items, 0, items.Length);
                    count = 0;
                }

                if (count == items.Length)
                    Array.Resize(ref items, items.Length * 2);
                items[count++] = item;
            }

            if (count > 0)
            {
                if (count != items.Length)
                    Array.Resize(ref items, count);

                MergeSort.Sort(items, kvcomparer);
                total += AddRangeSorted(items, allowUpdates);
            }
            return total;
        }

        /// <summary>
        ///     Optimized insert of presorted key/value pairs.
        ///     If the input is not presorted, please use AddRange() instead.
        /// </summary>
        /// <param name="items">The ordered list of items to insert</param>
        public int AddRangeSorted(IEnumerable<KeyValuePair<TKey, TValue>> items)
        {
            return AddRangeSorted(items, false);
        }

        /// <summary>
        ///     Optimized insert of presorted key/value pairs.
        ///     If the input is not presorted, please use AddRange() instead.
        /// </summary>
        /// <param name="items">The ordered list of items to insert</param>
        /// <param name="allowUpdates">True to overwrite any existing records</param>
        /// <returns>The total number of records inserted or updated</returns>
        public int AddRangeSorted(IEnumerable<KeyValuePair<TKey, TValue>> items, bool allowUpdates)
        {
            int result = 0;
            using (AddRangeInfo bulk = new AddRangeInfo(allowUpdates, items))
            {
                while (!bulk.IsComplete)
                {
                    KeyRange range = new KeyRange(_keyComparer);
                    using (RootLock root = LockRoot(LockType.Insert))
                    {
                        result += AddRange(root.Pin, ref range, bulk, null, int.MinValue);
                    }
                }
            }
            DebugComplete("AddRange({0} records)", result);
            return result;
        }

        private InsertResult AddEntry<T>(TKey key, ref T info) where T : ICreateOrUpdateValue<TKey, TValue>
        {
            InsertResult result;
            using (RootLock root = LockRoot(LockType.Insert))
            {
                result = Insert(root.Pin, key, ref info, null, int.MinValue);
            }
            if (result == InsertResult.Inserted && _hasCount)
                Interlocked.Increment(ref _count);
            DebugComplete("Added({0}) = {1}", key, result);
            return result;
        }

        private RemoveResult RemoveEntry<T>(TKey key, ref T removeValue) where T : IRemoveValue<TKey, TValue>
        {
            RemoveResult result;
            using (RootLock root = LockRoot(LockType.Delete))
            {
                result = Delete(root.Pin, key, ref removeValue, null, int.MinValue);
            }
            if (result == RemoveResult.Removed && _hasCount)
                Interlocked.Decrement(ref _count);
            DebugComplete("Removed({0}) = {1}", key, result);
            return result;
        }

        /// <summary>
        ///     Returns the first key and it's associated value.
        /// </summary>
        public KeyValuePair<TKey, TValue> First()
        {
            if (!TryGetFirst(out KeyValuePair<TKey, TValue> result))
                throw new InvalidOperationException();
            return result;
        }

        /// <summary>
        ///     Returns the first key and it's associated value.
        /// </summary>
        public bool TryGetFirst(out KeyValuePair<TKey, TValue> item)
        {
            using (RootLock root = LockRoot(LockType.Read))
            {
                return TryGetEdge(root.Pin, true, out item);
            }
        }

        /// <summary>
        ///     Returns the last key and it's associated value.
        /// </summary>
        public KeyValuePair<TKey, TValue> Last()
        {
            if (!TryGetLast(out KeyValuePair<TKey, TValue> result))
                throw new InvalidOperationException();
            return result;
        }

        /// <summary>
        ///     Returns the last key and it's associated value.
        /// </summary>
        public bool TryGetLast(out KeyValuePair<TKey, TValue> item)
        {
            using (RootLock root = LockRoot(LockType.Read))
            {
                return TryGetEdge(root.Pin, false, out item);
            }
        }

        /// <summary>
        ///     Inclusivly enumerates from start key to the end of the collection
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> EnumerateFrom(TKey start)
        {
            return new Enumerator(this, start);
        }

        /// <summary>
        ///     Inclusivly enumerates from start key to stop key
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> EnumerateRange(TKey start, TKey end)
        {
            return new Enumerator(this, start, x => _keyComparer.Compare(x.Key, end) <= 0);
        }

        private struct RootLock : IDisposable
        {
            private readonly BPlusTree<TKey, TValue> _tree;
            private readonly LockType _type;
            private NodeVersion _version;
            public readonly NodePin Pin;

            public RootLock(BPlusTree<TKey, TValue> tree, LockType type)
            {
                tree.NotDisposed();
                _tree = tree;
                _type = type;
                _version = type == LockType.Read ? tree._storage.CurrentVersion : null;
                Pin = _tree._storage.LockRoot(type);
            }

            void IDisposable.Dispose()
            {
                Pin.Dispose();

                _tree._storage.ReturnVersion(ref _version);

                if (_type != LockType.Read)
                    _tree.OnChanged();
            }
        }

        #region class KeyCollection

        private class KeyCollection : ICollection<TKey>
        {
            private readonly IDictionary<TKey, TValue> _owner;

            public KeyCollection(IDictionary<TKey, TValue> owner)
            {
                _owner = owner;
            }

            #region ICollection<TKey> Members

            public int Count => _owner.Count;
            public bool IsReadOnly => true;

            public bool Contains(TKey item)
            {
                return _owner.ContainsKey(item);
            }

            public void CopyTo(TKey[] array, int arrayIndex)
            {
                foreach (TKey key in this)
                    array[arrayIndex++] = key;
            }

            public IEnumerator<TKey> GetEnumerator()
            {
                return new KeyEnumerator(_owner.GetEnumerator());
            }

            [Obsolete]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class KeyEnumerator : IEnumerator<TKey>
            {
                private readonly IEnumerator<KeyValuePair<TKey, TValue>> _e;

                public KeyEnumerator(IEnumerator<KeyValuePair<TKey, TValue>> e)
                {
                    _e = e;
                }

                public TKey Current => _e.Current.Key;

                [Obsolete]
                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _e.Dispose();
                }

                public bool MoveNext()
                {
                    return _e.MoveNext();
                }

                public void Reset()
                {
                    _e.Reset();
                }
            }

            [Obsolete]
            void ICollection<TKey>.Add(TKey item)
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            void ICollection<TKey>.Clear()
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            bool ICollection<TKey>.Remove(TKey item)
            {
                throw new NotSupportedException();
            }

            #endregion
        }

        #endregion

        #region class ValueCollection

        private class ValueCollection : ICollection<TValue>
        {
            private readonly IDictionary<TKey, TValue> _owner;

            public ValueCollection(IDictionary<TKey, TValue> owner)
            {
                _owner = owner;
            }

            #region ICollection<TKey> Members

            public int Count => _owner.Count;
            public bool IsReadOnly => true;

            public bool Contains(TValue item)
            {
                IEqualityComparer<TValue> c = EqualityComparer<TValue>.Default;
                foreach (TValue value in this)
                    if (c.Equals(item, value))
                        return true;
                return false;
            }

            public void CopyTo(TValue[] array, int arrayIndex)
            {
                foreach (TValue value in this)
                    array[arrayIndex++] = value;
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                return new ValueEnumerator(_owner.GetEnumerator());
            }

            [Obsolete]
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class ValueEnumerator : IEnumerator<TValue>
            {
                private readonly IEnumerator<KeyValuePair<TKey, TValue>> _e;

                public ValueEnumerator(IEnumerator<KeyValuePair<TKey, TValue>> e)
                {
                    _e = e;
                }

                public TValue Current => _e.Current.Value;

                [Obsolete]
                object IEnumerator.Current => Current;

                public void Dispose()
                {
                    _e.Dispose();
                }

                public bool MoveNext()
                {
                    return _e.MoveNext();
                }

                public void Reset()
                {
                    _e.Reset();
                }
            }

            [Obsolete]
            void ICollection<TValue>.Add(TValue item)
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            void ICollection<TValue>.Clear()
            {
                throw new NotSupportedException();
            }

            [Obsolete]
            bool ICollection<TValue>.Remove(TValue item)
            {
                throw new NotSupportedException();
            }

            #endregion
        }

        #endregion

        #region ICollection Members

        private KeyCollection _keysCollection;
        private ValueCollection _valuesCollection;

        /// <summary>
        ///     Gets an <see cref="T:System.Collections.Generic.ICollection`1" /> containing the keys of the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public ICollection<TKey> Keys =>
            _keysCollection != null ? _keysCollection : (_keysCollection = new KeyCollection(this));

        /// <summary>
        ///     Gets an <see cref="T:System.Collections.Generic.ICollection`1" /> containing the values in the
        ///     <see cref="T:System.Collections.Generic.IDictionary`2" />.
        /// </summary>
        public ICollection<TValue> Values => _valuesCollection != null
            ? _valuesCollection
            : (_valuesCollection = new ValueCollection(this));


        /// <summary>
        ///     Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1" /> to an
        ///     <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
        /// </summary>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            foreach (KeyValuePair<TKey, TValue> kv in this)
                array[arrayIndex++] = kv;
        }

        /// <summary>
        ///     Removes all items from the collection and permanently destroys all storage.
        /// </summary>
        public void Clear()
        {
            NotDisposed();
            _storage.DeleteAll();
            _count = 0;
            //Since transaction logs do not deal with Clear(), we need to commit our current state
            CommitChanges();
            DebugComplete("Clear()");
        }

        /// <summary>
        ///     Returns true if the file was opened in ReadOnly mode.
        /// </summary>
        public bool IsReadOnly => _options.ReadOnly;

        #endregion
    }
}