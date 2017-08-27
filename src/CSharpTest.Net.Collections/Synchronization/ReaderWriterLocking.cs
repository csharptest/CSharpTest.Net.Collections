﻿#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Diagnostics;
using System.Threading;

namespace CSharpTest.Net.Synchronization
{
    /// <summary>
    ///     wraps the System.Threading.ReaderWriterLock lock, does not support read->write upgrades
    /// </summary>
    public class ReaderWriterLocking : ILockStrategy
    {
        private readonly ReaderWriterLockSlim _lock;
        private int _writeVersion;

        /// <summary>
        ///     wraps the reader/writer lock
        /// </summary>
        public ReaderWriterLocking() : this(new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion))
        {
        }

        /// <summary>
        ///     wraps the reader/writer lock
        /// </summary>
        public ReaderWriterLocking(ReaderWriterLockSlim lck)
        {
            _lock = lck;
        }

        void IDisposable.Dispose()
        {
        }

        /// <summary> Changes every time a write lock is aquired.  If WriteVersion == 0, no write locks have been issued. </summary>
        public int WriteVersion => _writeVersion;

        /// <summary>
        ///     Returns true if the lock was successfully obtained within the timeout specified
        /// </summary>
        [DebuggerNonUserCode]
        public bool TryRead(int timeout)
        {
            try
            {
                return _lock.TryEnterReadLock(timeout);
            }
            catch
            {
            }

            return false;
        }

        /// <summary>
        ///     Releases a read lock
        /// </summary>
        public void ReleaseRead()
        {
            _lock.ExitReadLock();
        }

        /// <summary>
        ///     Returns true if the lock was successfully obtained within the timeout specified
        /// </summary>
        [DebuggerNonUserCode]
        public bool TryWrite(int timeout)
        {
            try
            {
                if (_lock.TryEnterWriteLock(timeout))
                {
                    Interlocked.Increment(ref _writeVersion);
                    return true;
                }
            }
            catch
            {
            }

            return false;
        }

        /// <summary>
        ///     Releases a writer lock
        /// </summary>
        public void ReleaseWrite()
        {
            _lock.ExitWriteLock();
        }

        /// <summary>
        ///     Returns a reader lock that can be elevated to a write lock
        /// </summary>
        public ReadLock Read()
        {
            return ReadLock.Acquire(this, -1);
        }

        /// <summary>
        ///     Returns a reader lock that can be elevated to a write lock
        /// </summary>
        /// <exception cref="System.TimeoutException" />
        public ReadLock Read(int timeout)
        {
            return ReadLock.Acquire(this, timeout);
        }

        /// <summary>
        ///     Returns a read and write lock
        /// </summary>
        public WriteLock Write()
        {
            return WriteLock.Acquire(this, -1);
        }

        /// <summary>
        ///     Returns a read and write lock
        /// </summary>
        /// <exception cref="System.TimeoutException" />
        public WriteLock Write(int timeout)
        {
            return WriteLock.Acquire(this, timeout);
        }
    }
}