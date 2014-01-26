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
using System.IO;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using CSharpTest.Net.Utils;

namespace CSharpTest.Net.Collections
{
    /// <summary> Defines the storage type to use </summary>
    public enum StorageType
    {
        /// <summary> Uses in-memory storage </summary>
        Memory,
        /// <summary> Uses a file to store data, (Set by setting the FileName property) </summary>
        Disk,
        /// <summary> Uses a custom data store, (Set by setting the StorageSystem property) </summary>
        Custom
    }

    /// <summary> Determines if the file specified should be created </summary>
    public enum CreatePolicy
    {
        /// <summary> Does not create a new file </summary>
        Never,
        /// <summary> Creates a new file even if one already exists </summary>
        Always,
        /// <summary> Creates a new file only if it does not exist </summary>
        IfNeeded
    }

    /// <summary> Determines the type of node caching used in the tree </summary>
    public enum CachePolicy
    {
        /// <summary> Does not cache, allways loads from storage. </summary>
        None,
        /// <summary> Keeps every loaded object in memory. </summary>
        All,
        /// <summary> Keeps a history of objects in memory (see CacheKeepAliveXXX properties) </summary>
        Recent,
    }

    /// <summary>
    /// Defines the action to perform when opening a BPlusTree with an existing log file.
    /// </summary>
    public enum ExistingLogAction
    {
        /// <summary> 
        /// Infers the default wether or not the data file was created.  For newly created data
        /// files (CreatePolicy = Always, or IfNeeded and the file is missing) the default will
        /// be Truncate.  When existing data files are opened the default will ReplayAndCommit.
        /// </summary>
        Default,
        /// <summary> Ignore the existing entries in the log </summary>
        Ignore,
        /// <summary> Replay the log entries uncommitted </summary>
        Replay,
        /// <summary> Replay the log entries and commit the changes to the store </summary>
        ReplayAndCommit,
        /// <summary> Ignore the existing entries and truncate the log </summary>
        Truncate,
    }

    /// <summary>
    /// Defines the levels of durability the store will try to achieve.  'Uncommitted changes' in the descriptions below
    /// refers to all changes made to the tree since the last call to CommitChanges() on the BPlusTree class.
    /// </summary>
    public enum StoragePerformance
    {
        /// <summary> (100k rps) Uncommitted changes will be lost, a crash durring commit may corrupt state. </summary>
        /// <remarks> 
        /// No changes are committed until a call to Commit is made, durring the commit a partial write may corrupt the store.
        /// </remarks>
        Fastest = 1,
        /// <summary> (30k rps) Uses a system-cached transaction log to recover uncommitted changes after a process crash. </summary>
        /// <remarks> Will not corrupt state; however, in a power outage or system failure it may loose some comitted records. </remarks>
        LogFileInCache = 2,
        /// <summary> (8k rps) Every write will commit changes to the storage file immediately into system cache </summary>
        /// <remarks> May corrupt state and/or loose data in the event of a power outage </remarks>
        CommitToCache = 3,
        /// <summary> (2k rps) Uses a cache-writethrough transaction log to recover uncommitted changes after a power outage or system crash. </summary>
        /// <remarks> Complies with ACID durability requirements, can be expensive to recover from the log. </remarks>
        LogFileNoCache = 4,
        /// <summary> (1k rps) Every write will commit changes to the storage file immediately bypassing system cache (Slowest/Safest) </summary>
        /// <remarks> Complies with ACID durability requirements </remarks>
        CommitToDisk = 5,

        /// <summary> Defaults to using a transaction log in system cache for best performance/durability. </summary>
        Default = LogFileInCache,
    }

    /// <summary> Determines the binary file format and backwards compatibility </summary>
    public enum FileVersion
    {
        /// <summary> Version 1 compatable </summary>
        Version1 = 1,
        /// <summary> Version 2 compatable </summary>
        Version2 = 2,
    }

    /// <summary>
    /// Defines the options nessessary to construct a BPlusTree implementation
    /// </summary>
    public abstract class BPlusTreeOptions<TKey, TValue> : ICloneable
    {
        private readonly ISerializer<TKey> _keySerializer;
        private readonly ISerializer<TValue> _valueSerializer;

        private IComparer<TKey> _keyComparer;
        private StorageType _storageType = StorageType.Memory;
        private CreatePolicy _createFile = CreatePolicy.Never;
        private bool _readOnly;

        private ILockFactory _lockingFactory = new LockFactory<WriterOnlyLocking>();
        private ILockStrategy _callLevelLock = IgnoreLocking.Instance;
        private int _lockTimeout = 120000;

        private int _minimumChildNodes = 12;
        private int _maximumChildNodes = 32; // (assumes a key size of apx 100 bytes: (FileBlockSize - StoreageOverhead) / (AvgKeyBytes + ChildLinkSize)
        private int _fillChildNodes = 22;
        private int _minimumValueNodes = 3;
        private int _maximumValueNodes = 8; // (assumes a value size of apx 500 bytes: (FileBlockSize - StoreageOverhead) / (AvgValueBytes + AvgKeyBytes)
        private int _fillValueNodes = 4;

        private int _fileBlockSize = 4096;
        private ExistingLogAction _existingLogAction = ExistingLogAction.Default;

        private string _fileName;
        private INodeStorage _storageSystem;

        private CachePolicy _cachePolicy = CachePolicy.Recent;
        private int _keepAliveMinHistory = 10;
        private int _keepAliveMaxHistory = 100;
        private int _keepAliveTimeout = 60000;

        /// <summary>
        /// Constructs the options configuration to initialize a BPlusTree instance
        /// </summary>
        protected BPlusTreeOptions(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, IComparer<TKey> comparer)
        {
            _keySerializer = Check.NotNull(keySerializer);
            _valueSerializer = Check.NotNull(valueSerializer);
            KeyComparer = comparer;
            TransactionLogLimit = -1;
        }

        /// <summary> Accesses the key serializer given to the constructor </summary>
        public ISerializer<TKey> KeySerializer { get { return _keySerializer; } }

        /// <summary> Accesses the key serializer given to the constructor </summary>
        public ISerializer<TValue> ValueSerializer { get { return _valueSerializer; } }

        /// <summary> Defines a custom IComparer&lt;T> to be used for comparing keys </summary>
        public IComparer<TKey> KeyComparer
        {
            get { return _keyComparer ?? Comparer<TKey>.Default; }
            set
            {
                try { Check.NotNull(value); }
                catch (Exception e)
                { throw new InvalidConfigurationValueException("KeyComparer", "You must speicify a valid IComparer<TKey>.", e); }
                _keyComparer = value;
            }
        }
        /// <summary>
        /// Returns the version this option set is compatable with.
        /// </summary>
        public abstract FileVersion FileVersion { get; }

        /// <summary>
        /// Calculates default node-threasholds based upon the average number of bytes in key and value
        /// </summary>
        public void CalcBTreeOrder(int avgKeySizeBytes, int avgValueSizeBytes)
        {
            CalculateOrder(avgKeySizeBytes, avgValueSizeBytes);
        }

        /// <summary>
        /// Calculates default node-threasholds based upon the average number of bytes in key and value
        /// </summary>
        protected abstract void CalculateOrder(int avgKeySizeBytes, int avgValueSizeBytes);
        /// <summary> 
        /// Can be used to explicitly specify the storage type, or by simply providing a file name this
        /// will be done for you.  If no file name was specified the default is to use a memory store.
        /// </summary>
        public StorageType StorageType
        {
            get { return _storageType; }
            set
            {
                InvalidConfigurationValueException.Assert(Enum.IsDefined(typeof(StorageType), value), "StorageType", "The value is not defined.");
                InvalidConfigurationValueException.Assert(value != StorageType.Custom || _storageSystem != null, "StorageType", "Please provide the StorageSystem to be used.");
                InvalidConfigurationValueException.Assert(value != StorageType.Disk || _fileName != null, "StorageType", "Please provide the FileName to be used.");
                _storageType = value;
            }
        }
        /// <summary>
        /// Sets the BTree into a read-only mode (only supported when opening an existing file)
        /// </summary>
        public bool ReadOnly
        {
            get { return _readOnly; }
            set
            {
                if (value)
                {
                    InvalidConfigurationValueException.Assert(CreateFile == CreatePolicy.Never, "ReadOnly", "ReadOnly can only be used when CreateFile is Never");
                    InvalidConfigurationValueException.Assert(StorageType == StorageType.Disk, "ReadOnly", "ReadOnly can only be used with the file storage");
                    InvalidConfigurationValueException.Assert(File.Exists(FileName), "ReadOnly", "ReadOnly can only be used with an existing file");
                }
                _readOnly = value;
            }
        }
        /// <summary>
        /// Sets the custom implementation of the storage back-end to use for the BTree
        /// </summary>
        public INodeStorage StorageSystem
        {
            get { return _storageType == StorageType.Custom ? _storageSystem : null; }
            set { _storageSystem = Check.NotNull(value); _storageType = StorageType.Custom; }
        }
        /// <summary>
        /// Gets or sets the FileName that should be used to store the BTree
        /// </summary>
        public string FileName
        {
            get { return _fileName; }
            set
            {
                Check.NotNull(value);
                try { GC.KeepAlive(Path.GetFullPath(value)); }
                catch (Exception e)
                { throw new InvalidConfigurationValueException("FileName", e.Message, e); }
                _fileName = value;
                _storageType = StorageType.Disk;
            }
        }
        /// <summary>
        /// Gets or sets the file-create policy used when backing with a file storage
        /// </summary>
        public CreatePolicy CreateFile
        {
            get { return _createFile; }
            set
            {
                InvalidConfigurationValueException.Assert(Enum.IsDefined(typeof(CreatePolicy), value), "CreateFile", "The value is not defined.");
                _createFile = value;
            }
        }
        /// <summary>
        /// Gets or sets the number of bytes per file-block used in the file storage
        /// </summary>
        public int FileBlockSize
        {
            get { return _fileBlockSize; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 512 && value <= 0x10000, "FileBlockSize", "The valid range is from 512 bytes to 64 kilobytes in powers of 2.");
                _fileBlockSize = value;
            }
        }
        /// <summary>
        /// Gets or sets the number of milliseconds to wait before failing a lock request, the default
        /// of two minutes should be more than adequate.
        /// </summary>
        public int LockTimeout
        {
            get { return _lockTimeout; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= -1 && value <= int.MaxValue, "LockTimeout", "The valid range is from -1 to MaxValue.");
                _lockTimeout = value;
            }
        }
        /// <summary>
        /// Gets or sets the locking factory to use for accessing shared data. The default is WriterOnlyLocking() 
        /// which does not perform read locks, rather it will rely on the cache of the btree and may preform dirty
        /// reads.  You can use any implementation of ILockFactory; however, the SimpleReadWriteLocking seems to 
        /// perform the most efficiently for both reader/writer locks.  Additionally wrapping that instance in a
        /// ReserveredWriterLocking() instance will allow reads to continue up until a writer begins the commit
        /// process.  If you are only accessing the BTree instance from a single thread this can be set to 
        /// IgnoreLocking. Be careful of using ReaderWriterLocking as the write-intesive nature of the BTree will 
        /// suffer extreme performance penalties with this lock.
        /// </summary>
        public ILockFactory LockingFactory
        {
            get { return _lockingFactory; }
            set { _lockingFactory = Check.NotNull(value); }
        }
        /// <summary>
        /// Defines a reader/writer lock that used to control exclusive tree access when needed.  The public
        /// methods for EnableCount(), Clear(), and UnloadCache() each acquire an exclusive (write) lock while
        /// all other public methods acquire a shared (read) lock.  By default this lock is non-operational
        /// (an instance of IgnoreLocking) so if you need the above methods to work while multiple threads are
        /// accessing the tree, or if you exclusive access to the tree, specify a lock instance.  Since this
        /// lock is primarily a read-heavy lock consider using the ReaderWriterLocking or SimpleReadWriteLocking.
        /// </summary>
        public virtual ILockStrategy CallLevelLock
        {
            get { return _callLevelLock; }
            set { _callLevelLock = Check.NotNull(value); }
        }
        /// <summary>
        /// A quick means of setting all the min/max values for the node counts using this value as a basis
        /// for the Maximum fields and one-quarter of this value for Minimum fields provided the result is in
        /// range.
        /// </summary>
        public int BTreeOrder
        {
            set
            {
                InvalidConfigurationValueException.Assert(value >= 4 && value <= 256, "BTreeOrder", "The valid range is from 4 to 256.");
                MaximumChildNodes = MaximumValueNodes = value;
                MinimumChildNodes = MinimumValueNodes = Math.Max(2, value >> 2);
            }
        }
        /// <summary>
        /// The smallest number of child nodes that should be linked to before refactoring the tree to remove
        /// this node.  In a 'normal' and/or purest B+Tree this is always half of max; however for performance
        /// reasons this B+Tree allow any value equal to or less than half of max but at least 2.
        /// </summary>
        /// <value>A number in the range of 2 to 128 that is at most half of MaximumChildNodes.</value>
        public int MinimumChildNodes
        {
            get { return _minimumChildNodes; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 2 && value <= (MaximumChildNodes / 2), "MinimumChildNodes", "The valid range is from 2 to (MaximumChildNodes / 2).");
                _minimumChildNodes = value;
                _fillChildNodes = ((_maximumChildNodes - _minimumChildNodes) >> 1) + _minimumChildNodes;
            }
        }
        /// <summary>
        /// The largest number of child nodes that should be linked to before refactoring the tree to split
        /// this node into two.  This property has a side-effect on MinimumChildNodes to ensure that it continues
        /// to be at most half of MaximumChildNodes.
        /// </summary>
        /// <value>A number in the range of 4 to 256.</value>
        public int MaximumChildNodes
        {
            get { return _maximumChildNodes; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 4 && value <= 256, "MaximumChildNodes", "The valid range is from 4 to 256.");
                _maximumChildNodes = value;
                _minimumChildNodes = Math.Min(value, _maximumChildNodes / 2);
                _fillChildNodes = ((_maximumChildNodes - _minimumChildNodes) >> 1) + _minimumChildNodes;
            }
        }
        /// <summary>
        /// The smallest number of values that should be contained in this node before refactoring the tree to remove
        /// this node.  In a 'normal' and/or purest B+Tree this is always half of max; however for performance
        /// reasons this B+Tree allow any value equal to or less than half of max but at least 2.
        /// </summary>
        /// <value>A number in the range of 2 to 128 that is at most half of MaximumValueNodes.</value>
        public int MinimumValueNodes
        {
            get { return _minimumValueNodes; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 2 && value <= (MaximumValueNodes / 2), "MinimumValueNodes", "The valid range is from 2 to (MaximumValueNodes / 2).");
                _minimumValueNodes = value;
                _fillValueNodes = ((_maximumValueNodes - _minimumValueNodes) >> 1) + _minimumValueNodes;
            }
        }
        /// <summary>
        /// The largest number of values that should be contained in this node before refactoring the tree to split
        /// this node into two.  This property has a side-effect on MinimumValueNodes to ensure that it continues
        /// to be at most half of MaximumValueNodes.
        /// </summary>
        /// <value>A number in the range of 4 to 256.</value>
        public int MaximumValueNodes
        {
            get { return _maximumValueNodes; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 4 && value <= 256, "MaximumValueNodes", "The valid range is from 4 to 256.");
                _maximumValueNodes = value;
                _minimumValueNodes = Math.Min(value, _maximumValueNodes / 2);
                _fillValueNodes = ((_maximumValueNodes - _minimumValueNodes) >> 1) + _minimumValueNodes;
            }
        }
        /// <summary>
        /// Determines how long loaded nodes stay in memory, Full keeps all loaded nodes alive and is the
        /// most efficient, The default Recent keeps recently visited nodes alive based on the CacheKeepAlive
        /// properties, and None does not cache the nodes at all but does maintain a cache of locks for 
        /// each node visited.
        /// </summary>
        public CachePolicy CachePolicy
        {
            get { return _cachePolicy; }
            set
            {
                InvalidConfigurationValueException.Assert(Enum.IsDefined(typeof(CachePolicy), value), "CachePolicy", "The value is not defined.");
                _cachePolicy = value;
            }
        }

        /// <summary>
        /// CacheKeepAliveFactory provides a delegate to inject an implementation of the IObjectKeepAlive
        /// interface while then igoring all the other CacheKeepAliveXXX properties.
        /// </summary>
        public FactoryMethod<IObjectKeepAlive> CacheKeepAliveFactory
        {
            get { return _cacheKeepAliveFactory; }
            set { _cacheKeepAliveFactory = value; }
        }

        /// <summary> 
        /// Determins minimum number of recently visited nodes to keep alive in memory.  This number defines
        /// the history size, not the number of distinct nodes.  This number will always be kept reguardless
        /// of the timeout.  Specify a value of 0 to allow the timeout to empty the cache.
        /// </summary>
        public int CacheKeepAliveMinimumHistory
        {
            get { return _keepAliveMinHistory; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 0 && value <= int.MaxValue, "CacheKeepAliveMinimumHistory", "The valid range is from 0 to MaxValue.");
                _keepAliveMinHistory = value;
                _keepAliveMaxHistory = Math.Max(_keepAliveMaxHistory, value);
            }
        }
        /// <summary> 
        /// Determins maximum number of recently visited nodes to keep alive in memory.  This number defines
        /// the history size, not the number of distinct nodes.  The ceiling is always respected reguardless
        /// of the timeout.  Specify a value of 0 to disable history keep alive.
        /// </summary>
        public int CacheKeepAliveMaximumHistory
        {
            get { return _keepAliveMaxHistory; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 0 && value <= int.MaxValue, "CacheKeepAliveMaximumHistory", "The valid range is from 0 to MaxValue.");
                _keepAliveMaxHistory = value;
                _keepAliveMinHistory = Math.Min(_keepAliveMinHistory, value);
            }
        }
        /// <summary>
        /// If the cache contains more that CacheKeepAliveMinimumHistory items, this timeout will start to
        /// remove those items until the cache history is reduced to CacheKeepAliveMinimumHistory.  It is 
        /// important to know that the BPlusTree itself contains no theads and this timeout will not be 
        /// respected if cache is not in use.
        /// </summary>
        public int CacheKeepAliveTimeout
        {
            get { return _keepAliveTimeout; }
            set
            {
                InvalidConfigurationValueException.Assert(value >= 0 && value <= int.MaxValue, "CacheKeepAliveTimeout", "The valid range is from 0 to MaxValue.");
                _keepAliveTimeout = value;
            }
        }

        /// <summary>
        /// Creates a shallow clone of the configuration options.
        /// </summary>
        public BPlusTreeOptions<TKey, TValue> Clone() { return (BPlusTreeOptions<TKey, TValue>)MemberwiseClone(); }
        object ICloneable.Clone() { return MemberwiseClone(); }

        #region INTERNAL USE ONLY
        /// <summary> Enables or disables the caching and reordering of node write operations </summary>
        protected void SetStorageCache(bool cached) { UseStorageCache = cached; }
        /// <summary> Sets the transaction log to use </summary>
        protected void SetLogFile(ITransactionLog<TKey, TValue> logFile) { LogFile = logFile; }

        internal bool UseStorageCache;
        internal ITransactionLog<TKey, TValue> LogFile;
        private FactoryMethod<IObjectKeepAlive> _cacheKeepAliveFactory;

        internal ExistingLogAction ExistingLogAction
        {
            get { return _existingLogAction; }
            set { _existingLogAction = value; }
        }

        internal long TransactionLogLimit { get; set; }

        /// <summary> Used to create the correct storage type </summary>
        internal abstract INodeStorage CreateStorage();
        /// <summary> The desired fill-size of node that contain children </summary>
        internal int FillChildNodes { get { return _fillChildNodes; } }
        /// <summary> The desired fill-size of node that contain values </summary>
        internal int FillValueNodes { get { return _fillValueNodes; } }
        /// <summary>
        /// Creates the keep-alive object reference tracking implementation
        /// </summary>
        internal IObjectKeepAlive CreateCacheKeepAlive()
        {
            if (_cacheKeepAliveFactory != null)
                return _cacheKeepAliveFactory();

            return new ObjectKeepAlive(
                CacheKeepAliveMinimumHistory, 
                CacheKeepAliveMaximumHistory,
                TimeSpan.FromMilliseconds(CacheKeepAliveTimeout));
        }
        #endregion
    }
}
