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
using CSharpTest.Net.IO;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Storage;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        #region VERSION 2 - Options

        /// <summary>
        /// Defines the options nessessary to construct a BPlusTree implementation
        /// </summary>
        public sealed class OptionsV2 : BPlusTreeOptions<TKey, TValue>
        {
            private string _logFileName;
            private StoragePerformance _protection = StoragePerformance.Default;
            private ILockStrategy _callLevelLock = null;

            /// <summary>
            /// Constructs the options configuration to initialize a BPlusTree instance using the default Comparer for TKey
            /// </summary>
            public OptionsV2(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
                : this(keySerializer, valueSerializer, Comparer<TKey>.Default)
            { }

            /// <summary>
            /// Constructs the options configuration to initialize a BPlusTree instance
            /// </summary>
            public OptionsV2(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, IComparer<TKey> comparer)
                : base(keySerializer, valueSerializer, comparer)
            {
                CacheKeepAliveMinimumHistory = 128;
                CacheKeepAliveMaximumHistory = 1024;
            }

            /// <summary>
            /// Creates a shallow clone of the configuration options.
            /// </summary>
            public new OptionsV2 Clone() { return (OptionsV2)MemberwiseClone(); }

            /// <summary>
            /// Defines a reader/writer lock that used to control exclusive tree access when needed.  
            /// Version2 files using trasacation logs will use this to gain exclusive access to the tree
            /// during calls to Commit, Rollback, etc.  The default is to use a SimpleReadWriteLocking
            /// class.  If you are accessing the tree from a single thread, consider using the IgnoreLocking
            /// class for better performance.
            /// </summary>
            public override ILockStrategy CallLevelLock
            {
                get
                {
                    if (_callLevelLock == null)
                        _callLevelLock = new ReaderWriterLocking();
                    return _callLevelLock;
                }
                set { _callLevelLock = value; }
            }

            /// <summary>
            /// Returns the version this option set is compatible with.
            /// </summary>
            public override FileVersion FileVersion
            {
                get { return FileVersion.Version2; }
            }
            /// <summary>
            /// Returns the DurabilityProtection of the underlying storage to create.
            /// </summary>
            public StoragePerformance StoragePerformance
            {
                get { return _protection; }
                set { _protection = value; }
            }
            /// <summary>
            /// Gets or sets a filename to write binary log files to.
            /// </summary>
            /// <remarks> 
            /// This is just a short-hand way of setting the TransactionLog instance.  For better performance
            /// at the risk of loosing a record or two, consider creating the TransactionLog instance with the
            /// FileOptions set to (FileOptions.WriteThrough | FileOptions.Asynchronous).
            /// </remarks>
            public string TransactionLogFileName
            {
                get { return _logFileName; }
                set { _logFileName = value; }
            }

            /// <summary>
            /// Gets or sets an implementation of ITransactionLog&lt;TKey, TValue> used to log writes to this
            /// tree for recovery and/or monitoring.
            /// </summary>
            public ITransactionLog<TKey, TValue> TransactionLog
            {
                get
                {
                    if (LogFile == null && _logFileName != null)
                        SetLogFile(new TransactionLog<TKey, TValue>(
                            new TransactionLogOptions<TKey, TValue>(_logFileName, KeySerializer, ValueSerializer)));
                    return LogFile;
                }
                set { LogFile = value; }
            }

            /// <summary>
            /// Defines the action to perform when opening a BPlusTree with an existing log file.
            /// </summary>
            public new ExistingLogAction ExistingLogAction 
            { 
                get { return base.ExistingLogAction; } 
                set { base.ExistingLogAction = value; } 
            }

            /// <summary>
            /// Defines the number of bytes in the transaction log file before the BPlusTree will auto-commit
            /// and truncate the log.  Values equal to or less than zero will not auto-commit (default).
            /// </summary>
            public new long TransactionLogLimit
            { 
                get { return base.TransactionLogLimit; } 
                set { base.TransactionLogLimit = value; } 
            }


            /// <summary>
            /// Calculates default node-thresholds based upon the average number of bytes in key and value
            /// </summary>
            public new OptionsV2 CalcBTreeOrder(int avgKeySizeBytes, int avgValueSizeBytes)
            {
                CalculateOrder(avgKeySizeBytes, avgValueSizeBytes);
                return this;
            }

            /// <summary>
            /// Calculates default node-thresholds based upon the average number of bytes in key and value
            /// </summary>
            protected override void CalculateOrder(int avgKeySizeBytes, int avgValueSizeBytes)
            {
                const int childLinkSize = 8;

                avgKeySizeBytes = Math.Max(0, Math.Min(ushort.MaxValue, avgKeySizeBytes));
                avgValueSizeBytes = Math.Max(0, Math.Min(ushort.MaxValue, avgValueSizeBytes));

                int maxChildNodes = Math.Min(256, Math.Max(4, FileBlockSize / (avgKeySizeBytes + childLinkSize)));
                int maxValueNodes = Math.Min(256, Math.Max(4, FileBlockSize / Math.Max(1, (avgValueSizeBytes + avgKeySizeBytes))));
                MaximumChildNodes = maxChildNodes;
                MinimumChildNodes = Math.Max(2, maxChildNodes / 3);
                MaximumValueNodes = maxValueNodes;
                MinimumValueNodes = Math.Max(2, maxValueNodes / 3);
            }
            
            /// <summary> Used to create the correct storage type </summary>
            internal override INodeStorage CreateStorage()
            {
                if (StorageType == StorageType.Custom) return Check.NotNull(StorageSystem);
                if (StorageType == StorageType.Memory) return new BTreeMemoryStore();

                InvalidConfigurationValueException.Assert(StorageType == StorageType.Disk, "StorageType", "Unknown value defined.");
                bool exists = File.Exists(FileName);
                if (exists && new FileInfo(FileName).Length == 0)
                {
                    exists = false;
                    File.Delete(FileName);
                }
                bool createNew = CreateFile == CreatePolicy.Always ||
                                 (exists == false && CreateFile == CreatePolicy.IfNeeded);

                if (!exists && !createNew)
                    throw new InvalidConfigurationValueException("CreateFile", "The file does not exist and CreateFile is Never");

                TransactedCompoundFile.Options foptions =
                    new TransactedCompoundFile.Options(FileName)
                    {
                        BlockSize = FileBlockSize,
                        FileOptions = FileOptions.None,
                        ReadOnly = ReadOnly,
                        CreateNew = createNew
                    };

                switch (StoragePerformance)
                {
                    case StoragePerformance.Fastest:
                        {
                            SetStorageCache(true);
                            break;
                        }
                    case StoragePerformance.CommitToCache:
                        {
                            foptions.FileOptions = FileOptions.None;
                            foptions.CommitOnWrite = true;
                            break;
                        }
                    case StoragePerformance.CommitToDisk:
                        {
                            foptions.FileOptions = FileOptions.WriteThrough;
                            foptions.CommitOnWrite = true;
                            break;
                        }
                    case StoragePerformance.LogFileInCache:
                    case StoragePerformance.LogFileNoCache:
                        {
                            SetStorageCache(true);
                            if (LogFile == null)
                            {
                                _logFileName = _logFileName ?? Path.ChangeExtension(FileName, ".tlog");
                                SetLogFile(new TransactionLog<TKey, TValue>(
                                    new TransactionLogOptions<TKey, TValue>(_logFileName, KeySerializer, ValueSerializer)
                                    {
                                        FileOptions = StoragePerformance == StoragePerformance.LogFileNoCache
                                            ? FileOptions.WriteThrough : FileOptions.None,
                                    }
                                ));
                            }
                            break;
                        }
                    default:
                        throw new InvalidConfigurationValueException("DurabilityProtection", "The configuration option is not valid.");
                }

                return new BTreeFileStoreV2(foptions);
            }
        }

        #endregion
        #region VERSION 1 - Options

        /// <summary>
        /// Defines the options nessessary to construct a BPlusTree implementation
        /// </summary>
        [System.ComponentModel.Browsable(false)]
        public sealed partial class Options : BPlusTreeOptions<TKey, TValue>
        {
            private int _fileGrowthRate = 100;
            private int _concurrentWriters = 8;
            private FileOptions _fileOptions = FragmentedFile.OptionsDefault;
        
            /// <summary>
            /// Constructs the options configuration to initialize a BPlusTree instance using the default Comparer for TKey
            /// </summary>
            public Options(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
                : this(keySerializer, valueSerializer, Comparer<TKey>.Default)
            { }

            /// <summary>
            /// Constructs the options configuration to initialize a BPlusTree instance
            /// </summary>
            public Options(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, IComparer<TKey> comparer)
                : base(keySerializer, valueSerializer, comparer)
            {
                try { _concurrentWriters = Math.Max(4, Math.Min(16, Environment.ProcessorCount)); }
                catch { _concurrentWriters = 4; }
                //defaults have been increased in v2
                CacheKeepAliveMinimumHistory = 10;
                CacheKeepAliveMaximumHistory = 100;
            }

            /// <summary>
            /// Creates a shallow clone of the configuration options.
            /// </summary>
            public new Options Clone() { return (Options)MemberwiseClone(); }
            /// <summary>
            /// Gets or sets the number of bytes per file-block used in the file storage
            /// </summary>
            public FileOptions FileOpenOptions
            {
                get { return _fileOptions; }
                set { _fileOptions = value; }
            }
            /// <summary>
            /// Gets or sets the number of blocks that a file will grow by when all blocks are used, use zero for incremental growth
            /// </summary>
            public int FileGrowthRate
            {
                get { return _fileGrowthRate; }
                set
                {
                    InvalidConfigurationValueException.Assert(value >= 0 && value <= ushort.MaxValue, "FileGrowthRate", "The valid range is from 0 bytes to 65,535.");
                    _fileGrowthRate = value;
                }
            }
            /// <summary>
            /// Gets or sets the number of streams that will be created for threads to write in the file store
            /// </summary>
            public int ConcurrentWriters
            {
                get { return _concurrentWriters; }
                set
                {
                    InvalidConfigurationValueException.Assert(value >= 1 && value < 64, "ConcurrentWriters", "The valid range is from 1 to 64.");
                    _concurrentWriters = value;
                }
            }

            /// <summary>
            /// Returns the version this option set is compatable with.
            /// </summary>
            public override FileVersion FileVersion { get { return FileVersion.Version1; } }


            /// <summary>
            /// Calculates default node-threasholds based upon the average number of bytes in key and value
            /// </summary>
            public new Options CalcBTreeOrder(int avgKeySizeBytes, int avgValueSizeBytes)
            {
                CalculateOrder(avgKeySizeBytes, avgValueSizeBytes);
                return this;
            }

            /// <summary>
            /// Calculates default node-threasholds based upon the average number of bytes in key and value
            /// </summary>
            protected override void CalculateOrder(int avgKeySizeBytes, int avgValueSizeBytes)
            {
                const int storeageOverhead = 64;
                const int childLinkSize = 32;

                avgKeySizeBytes = Math.Max(0, Math.Min(ushort.MaxValue, avgKeySizeBytes));
                avgValueSizeBytes = Math.Max(0, Math.Min(ushort.MaxValue, avgValueSizeBytes));

                int maxChildNodes = Math.Min(256, Math.Max(4, (FileBlockSize - storeageOverhead) / (avgKeySizeBytes + childLinkSize)));
                int maxValueNodes = Math.Min(256, Math.Max(4, (FileBlockSize - storeageOverhead) / Math.Max(1, (avgValueSizeBytes + avgKeySizeBytes))));
                MaximumChildNodes = maxChildNodes;
                MinimumChildNodes = Math.Max(2, maxChildNodes / 3);
                MaximumValueNodes = maxValueNodes;
                MinimumValueNodes = Math.Max(2, maxValueNodes / 3);
            }
            /// <summary> Used to create the correct storage type </summary>
            internal override INodeStorage CreateStorage()
            {
                if (StorageType == StorageType.Custom) return Check.NotNull(StorageSystem);
                if (StorageType == StorageType.Memory) return new BTreeMemoryStore();

                bool exists = File.Exists(FileName);
                if (CreateFile == CreatePolicy.Always || (!exists && CreateFile == CreatePolicy.IfNeeded))
                    return BTreeFileStore.CreateNew(FileName, FileBlockSize, FileGrowthRate, ConcurrentWriters, FileOpenOptions);

                InvalidConfigurationValueException.Assert(exists, "CreateFile", "The file does not exist and CreateFile is Never");
                return new BTreeFileStore(FileName, FileBlockSize, FileGrowthRate, ConcurrentWriters, FileOpenOptions, ReadOnly);
            }
        }
        #endregion
    }
}
