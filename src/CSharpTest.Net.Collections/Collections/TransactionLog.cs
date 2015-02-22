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
using System.IO;
using System.Threading;
using System.Collections.Generic;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.IO;

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// A value representing the state/identifer/object of a single transaction.  The field's
    /// meaning is defined by the ITrasactionLog implementation and is otherwise treated as an
    /// opaque token identifier of the transaction.
    /// </summary>
    public struct TransactionToken
    {
        /// <summary> Undefined </summary>
        public int State;
        /// <summary> Undefined </summary>
        public long Handle;
        /// <summary> Undefined </summary>
        public object Object;
    }
    /// <summary>
    /// Options used to initialize a TransactionLog
    /// </summary>
    public class TransactionLogOptions<TKey, TValue>
    {
        private readonly string _fileName;
        private readonly ISerializer<TKey> _keySerializer;
        private readonly ISerializer<TValue> _valueSerializer;
        private FileOptions _foptions;
        private int _fbuffer;
        private bool _readOnly;

        /// <summary>
        /// Options used to initialize a TransactionLog
        /// </summary>
        public TransactionLogOptions(string fileName, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
        {
            _fileName = Check.NotEmpty(fileName);
            _keySerializer = Check.NotNull(keySerializer);
            _valueSerializer = Check.NotNull(valueSerializer);
            _foptions = FileOptions.WriteThrough;
            _fbuffer = 8;
        }

        /// <summary> The serializer for the TKey type </summary>
        public ISerializer<TKey> KeySerializer { get { return _keySerializer; } }
        /// <summary> The serializer for the TValue type </summary>
        public ISerializer<TValue> ValueSerializer { get { return _valueSerializer; } }

        /// <summary> The file name to read/write the log </summary>
        public string FileName { get { return _fileName; } }
        /// <summary> The file open options for appending to a log, default = WriteThrough </summary>
        public FileOptions FileOptions { get { return _foptions; } set { _foptions = value; } }
        /// <summary> The file buffer size, CAUTION: values above 16 bytes may leave data in memory </summary>
        public int FileBuffer { get { return _fbuffer; } set { _fbuffer = value; } }
        /// <summary> Gets or sets if the transaction log is treated as read-only </summary>
        public bool ReadOnly { get { return _readOnly; } set { _readOnly = value; } }

        /// <summary> Creates a shallow clone of the instance </summary>
        public TransactionLogOptions<TKey, TValue> Clone()
        {
            return (TransactionLogOptions<TKey, TValue>)MemberwiseClone();
        }
    }

    /// <summary>
    /// Represents a transaction log of writes to a dictionary.
    /// </summary>
    public interface ITransactionLog<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Replay the entire log file to the provided dictionary interface
        /// </summary>
        void ReplayLog(IDictionary<TKey, TValue> target);
        /// <summary>
        /// Replay the log file from the position provided and output the new log position
        /// </summary>
        void ReplayLog(IDictionary<TKey, TValue> target, ref long position);
        /// <summary>
        /// Merges the contents of the log with an existing ordered key/value pair collection.
        /// </summary>
        IEnumerable<KeyValuePair<TKey, TValue>> MergeLog(
            IComparer<TKey> keyComparer, IEnumerable<KeyValuePair<TKey, TValue>> existing);
        /// <summary>
        /// Truncate the log and remove all existing entries
        /// </summary>
        void TruncateLog();

        /// <summary>
        /// Notifies the log that a transaction is begining and create a token for this
        /// transaction scope.
        /// </summary>
        TransactionToken BeginTransaction();

        /// <summary> The provided key/value pair was added in the provided transaction </summary>
        void AddValue(ref TransactionToken token, TKey key, TValue value);
        /// <summary> The provided key/value pair was updated in the provided transaction </summary>
        void UpdateValue(ref TransactionToken token, TKey key, TValue value);
        /// <summary> The provided key/value pair was removed in the provided transaction </summary>
        void RemoveValue(ref TransactionToken token, TKey key);

        /// <summary>
        /// Commits the provided transaction
        /// </summary>
        void CommitTransaction(ref TransactionToken token);
        /// <summary>
        /// Abandons the provided transaction
        /// </summary>
        void RollbackTransaction(ref TransactionToken token);
        /// <summary>
        /// Returns the filename being currently used for transaction logging
        /// </summary>
        string FileName { get; }
        /// <summary>
        /// Returns the current size of the log file in bytes
        /// </summary>
        long Size { get; }
    }

    /// <summary>
    /// The default transaction log for a BPlusTree instance to provide backup+log recovery
    /// </summary>
    public class TransactionLog<TKey, TValue> : ITransactionLog<TKey, TValue>
    {
        private const int StateOpen = 1, StateCommitted = 2, StateRolledback = 3;
        #region Private Types
        enum OperationCode { Add = 1, Update = 2, Remove = 3 }

        private delegate void WriteBytesDelegate(byte[] buffer, int offset, int length);

        struct LogEntry
        {
            public int TransactionId;
            public OperationCode OpCode;
            public TKey Key;
            public TValue Value;

            public static IEnumerable<LogEntry> FromKeyValuePairs(IEnumerable<KeyValuePair<TKey, TValue>> e)
            {
                foreach (KeyValuePair<TKey, TValue> kv in e)
                    yield return new LogEntry
                    {
                        TransactionId = 0,
                        OpCode = OperationCode.Add,
                        Key = kv.Key,
                        Value = kv.Value,
                    };
            }
        }

        private class LogEntryComparer : IComparer<LogEntry>
        {
            private IComparer<TKey> _keyComparer;
            public LogEntryComparer(IComparer<TKey> keyComparer)
            {
                _keyComparer = keyComparer;
            }
            public int Compare(LogEntry x, LogEntry y)
            {
                return _keyComparer.Compare(x.Key, y.Key);
            }
        }

        private class LogEntrySerializer : ISerializer<LogEntry>
        {
            private readonly ISerializer<TKey> _keySerializer;
            private readonly ISerializer<TValue> _valueSerializer;

            public LogEntrySerializer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer)
            {
                _keySerializer = keySerializer;
                _valueSerializer = valueSerializer;
            }
            public void WriteTo(LogEntry value, Stream stream)
            {
                PrimitiveSerializer.Int32.WriteTo(value.TransactionId, stream);
                PrimitiveSerializer.Int16.WriteTo((short)value.OpCode, stream);
                _keySerializer.WriteTo(value.Key, stream);
                if (value.OpCode != OperationCode.Remove)
                    _valueSerializer.WriteTo(value.Value, stream);
            }
            public LogEntry ReadFrom(Stream stream)
            {
                LogEntry entry = new LogEntry();
                entry.TransactionId = PrimitiveSerializer.Int32.ReadFrom(stream);
                entry.OpCode = (OperationCode)PrimitiveSerializer.Int16.ReadFrom(stream);
                entry.Key = _keySerializer.ReadFrom(stream);
                if (entry.OpCode != OperationCode.Remove)
                    entry.Value = _valueSerializer.ReadFrom(stream);
                return entry;
            }
        }
        #endregion

        private readonly object _logSync;
        private readonly TransactionLogOptions<TKey, TValue> _options;

        private long _transactionId;
        private long _fLength;
        private Stream _logfile;

        /// <summary>
        /// Creates an instance of a transaction log
        /// </summary>
        public TransactionLog(TransactionLogOptions<TKey, TValue> options)
        {
            _options = options.Clone();
            _logSync = new object();
            _transactionId = 1;
            _logfile = null;
            try
            {
                _fLength = File.Exists(_options.FileName) ? new FileInfo(_options.FileName).Length : 0;
            }
            catch (FileNotFoundException)
            {
                _fLength = 0;
            }
        }

        /// <summary>
        /// Returns the file name of the current transaction log file
        /// </summary>
        public string FileName { get { return _options.FileName; } }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        void IDisposable.Dispose()
        {
            Close();
        }
        /// <summary>
        /// Flushes any pending writes and closes the writer.
        /// </summary>
        public void Close()
        {
            lock(_logSync)
            {
                if (_logfile != null)
                {
                    _logfile.Flush();
                    _logfile.Dispose();
                    _logfile = null;
                }

                if (Size == 0)
                    File.Delete(_options.FileName);
            }
        }

        /// <summary>
        /// Returns the current size of the log file in bytes
        /// </summary>
        public long Size
        { 
            get
            {
                return _logfile != null ? _fLength
                    : (File.Exists(_options.FileName) ? new FileInfo(_options.FileName).Length : 0); 
            }
        }
        
        /// <summary>
        /// Replay the entire log file to the provided dictionary interface
        /// </summary>
        public void ReplayLog(IDictionary<TKey, TValue> target)
        {
            long position = 0L;
            ReplayLog(target, ref position);
        }
        /// <summary>
        /// Replay the log file from the position provided and output the new log position
        /// </summary>
        public void ReplayLog(IDictionary<TKey, TValue> target, ref long position)
        {
            long[] refposition = new long[] { position };
            try
            {
                foreach (LogEntry entry in EnumerateLog(refposition))
                {
                    if (entry.OpCode == OperationCode.Remove)
                        target.Remove(entry.Key);
                    else
                        target[entry.Key] = entry.Value;
                }
            }
            finally
            {
                position = refposition[0];
            }
        }

        /// <summary>
        /// Merges the contents of the log with an existing ordered key/value pair collection.
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> MergeLog(IComparer<TKey> keyComparer, IEnumerable<KeyValuePair<TKey, TValue>> existing)
        {
            LogEntryComparer comparer = new LogEntryComparer(keyComparer);
            // Order the log entries by key
            OrderedEnumeration<LogEntry> orderedLog = new OrderedEnumeration<LogEntry>(
                comparer,
                EnumerateLog(new long[1]),
                new LogEntrySerializer(_options.KeySerializer, _options.ValueSerializer)
                );

            // Merge the existing data with the ordered log, using last value
            IEnumerable<LogEntry> all = OrderedEnumeration<LogEntry>.Merge(
                comparer, DuplicateHandling.LastValueWins, LogEntry.FromKeyValuePairs(existing), orderedLog);

            // Returns all key/value pairs that are not a remove operation
            foreach (LogEntry le in all)
            {
                if (le.OpCode != OperationCode.Remove)
                    yield return new KeyValuePair<TKey, TValue>(le.Key, le.Value);
            }
        }

        /// <summary>
        /// Replay the log file from the position provided and output the new log position
        /// </summary>
        IEnumerable<LogEntry> EnumerateLog(long[] position)
        {
            lock (_logSync)
            {
                long pos = 0;
                long length;

                if (!File.Exists(_options.FileName))
                {
                    position[0] = 0;
                    yield break;
                }

                using (MemoryStream buffer = new MemoryStream(8192))
                using (Stream io = new FileStream(_options.FileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 0x10000, FileOptions.SequentialScan))
                {
                    bool valid = true;
                    const int minSize = 16;
                    byte[] bytes = buffer.GetBuffer();
                    int size, temp, nbytes, szcontent;
                    short opCount;
                    LogEntry entry = new LogEntry();

                    length = io.Length;
                    if (position[0] < 0 || position[0] > length)
                    {
                        position[0] = length;
                        yield break;
                    }

                    bool fixedOffset = position[0] > 0;
                    io.Position = position[0];

                    while (valid && (pos = position[0] = io.Position) + minSize < length)
                    {
                        try
                        {
                            size = PrimitiveSerializer.Int32.ReadFrom(io);
                            size = ((byte)(size >> 24) == 0xbb) ? size & 0x00FFFFFF : -1;
                            if (size < minSize || pos + size + 4 > length)
                            {
                                if (fixedOffset)
                                    yield break;
                                break;
                            }
                            fixedOffset = false;

                            if (size > buffer.Capacity)
                            {
                                buffer.Capacity = (size + 8192);
                                bytes = buffer.GetBuffer();
                            }

                            szcontent = size - 8;

                            buffer.Position = 0;
                            buffer.SetLength(szcontent);
                            nbytes = 0;
                            while (nbytes < szcontent && (temp = io.Read(bytes, nbytes, szcontent - nbytes)) != 0)
                                nbytes += temp;

                            if (nbytes != szcontent)
                                break;
                            Crc32 crc = new Crc32();
                            crc.Add(bytes, 0, nbytes);
                            temp = PrimitiveSerializer.Int32.ReadFrom(io);
                            if (crc.Value != temp)
                                break;

                            temp = PrimitiveSerializer.Int32.ReadFrom(io);
                            if ((byte)(temp >> 24) != 0xee || (temp & 0x00FFFFFF) != size)
                                break;

                            entry.TransactionId = PrimitiveSerializer.Int32.ReadFrom(buffer);
                            _transactionId = Math.Max(_transactionId, entry.TransactionId + 1);

                            opCount = PrimitiveSerializer.Int16.ReadFrom(buffer);
                            if (opCount <= 0 || opCount >= short.MaxValue)
                                break;
                        }
                        catch(InvalidDataException)
                        {
                            break;
                        }
                        while (opCount-- > 0)
                        {
                            entry.OpCode = (OperationCode)PrimitiveSerializer.Int16.ReadFrom(buffer);

                            if (entry.OpCode != OperationCode.Add && entry.OpCode != OperationCode.Update && entry.OpCode != OperationCode.Remove)
                            {
                                valid = false;
                                break;
                            }

                            try
                            {
                                entry.Key = _options.KeySerializer.ReadFrom(buffer);
                                entry.Value = (entry.OpCode == OperationCode.Remove)
                                    ? default(TValue)
                                    : _options.ValueSerializer.ReadFrom(buffer);
                            }
                            catch
                            {
                                valid = false;
                                break;
                            }
                            if ((buffer.Position == buffer.Length) != (opCount == 0))
                            {
                                valid = false;
                                break;
                            }

                            yield return entry;
                        }
                    }
                }

                if (!_options.ReadOnly && pos < length)
                    TruncateLog(pos);
            }
        }

        /// <summary>
        /// Truncate the log and remove all existing entries
        /// </summary>
        public void TruncateLog()
        {
            TruncateLog(0);
        }

        void TruncateLog(long position)
        {
            lock (_logSync)
            {
                Close();
                using (Stream io = new FileStream(_options.FileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read))
                {
                    io.SetLength(position);
                    _fLength = position;
                }
            }
        }

        /// <summary>
        /// Notifies the log that a transaction is begining and create a token for this
        /// transaction scope.
        /// </summary>
        public TransactionToken BeginTransaction()
        {
            return new TransactionToken
                       {
                           State = StateOpen,
                           Handle = Interlocked.Increment(ref _transactionId),
                       };
        }

        /// <summary> The provided key/value pair was added in the provided transaction </summary>
        public void AddValue(ref TransactionToken token, TKey key, TValue value)
        {
            Write(ref token, OperationCode.Add, key, value);
        }

        /// <summary> The provided key/value pair was updated in the provided transaction </summary>
        public void UpdateValue(ref TransactionToken token, TKey key, TValue value)
        {
            Write(ref token, OperationCode.Update, key, value);
        }

        /// <summary> The provided key/value pair was removed in the provided transaction </summary>
        public void RemoveValue(ref TransactionToken token, TKey key)
        {
            Write(ref token, OperationCode.Remove, key, default(TValue));
        }

        private void Write(ref TransactionToken token, OperationCode operation, TKey key, TValue value)
        {
            AssertionFailedException.Assert(token.State == StateOpen);
            MemoryStream buffer = token.Object as MemoryStream;
            if (buffer == null)
            {
                token.Object = buffer = new MemoryStream();
                PrimitiveSerializer.Int32.WriteTo(0, buffer);
                PrimitiveSerializer.Int32.WriteTo(unchecked((int)token.Handle), buffer);
                PrimitiveSerializer.Int16.WriteTo(0, buffer);
            }

            PrimitiveSerializer.Int16.WriteTo((short)operation, buffer);
            _options.KeySerializer.WriteTo(key, buffer);
            if (operation != OperationCode.Remove)
                _options.ValueSerializer.WriteTo(value, buffer);

            //Increment the operation counter at offset 8
            long pos = buffer.Position;

            buffer.Position = 8;
            short count = PrimitiveSerializer.Int16.ReadFrom(buffer);
            
            buffer.Position = 8;
            PrimitiveSerializer.Int16.WriteTo(++count, buffer);
            
            buffer.Position = pos;
        }

        /// <summary>
        /// Commits the provided transaction
        /// </summary>
        public void CommitTransaction(ref TransactionToken token)
        {
            AssertionFailedException.Assert(token.State == StateOpen);
            token.State = StateCommitted;

            MemoryStream buffer = token.Object as MemoryStream;
            if (buffer == null)
                return; // nothing to commit

            byte[] bytes = buffer.GetBuffer();
            Crc32 crc = new Crc32();
            crc.Add(bytes, 4, (int)buffer.Position - 4);
            PrimitiveSerializer.Int32.WriteTo(crc.Value, buffer);

            int len = (int)buffer.Position;
            PrimitiveSerializer.Int32.WriteTo((0xee << 24) + len, buffer);
            buffer.Position = 0;
            PrimitiveSerializer.Int32.WriteTo((0xbb << 24) + len, buffer);
            bytes = buffer.GetBuffer();

            WriteBytes(bytes, 0, len + 4);
        }

        private void WriteBytes(byte[] bytes, int offset, int length)
        {
            if (_options.ReadOnly) return;
            lock (_logSync)
            {
                if (_logfile == null)
                {
                    _logfile = new FileStream(_options.FileName, FileMode.Append, FileAccess.Write, FileShare.Read,
                                              _options.FileBuffer, _options.FileOptions);
                }
                _logfile.Write(bytes, offset, length);
                _fLength = _logfile.Position;
            }
        }

        /// <summary>
        /// Abandons the provided transaction
        /// </summary>
        public void RollbackTransaction(ref TransactionToken token)
        {
            if (token.State == StateRolledback)
                return;

            AssertionFailedException.Assert(token.State == StateOpen);
            token.State = StateRolledback;
            MemoryStream buffer = token.Object as MemoryStream;
            if (buffer != null)
                buffer.Dispose();
            token.Object = null;
            token.Handle = 0;
        }
    }
}
