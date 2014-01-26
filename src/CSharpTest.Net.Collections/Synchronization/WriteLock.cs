#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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

namespace CSharpTest.Net.Synchronization
{
    /// <summary>
    /// Allows a write lock to be disposed
    /// </summary>
    public struct WriteLock : IDisposable
    { 
        /// <summary> Acquires the lock within the timeout or throws TimeoutException </summary>
        /// <exception cref="System.TimeoutException"/>
        public static WriteLock Acquire(ILockStrategy lck, int timeout)
        {
            if(!lck.TryWrite(timeout)) throw new TimeoutException();
            return new WriteLock(lck, true);
        }

        bool _hasLock;
        readonly ILockStrategy _lock;

        /// <summary> Tracks an existing read lock on a resource </summary>
        public WriteLock(ILockStrategy lck, bool locked)
        {
            _lock = lck;
            _hasLock = locked;
        }

        /// <summary> Acquires a read lock on the resource </summary>
        public WriteLock(ILockStrategy lck, int timeout)
        {
            _lock = lck;
            _hasLock = lck.TryWrite(timeout);
        }

        /// <summary>
        /// Returns true if write access is locked
        /// </summary>
        public bool HasWriteLock { get { return _hasLock; } }

        /// <summary> Unlocks the resource </summary>
        [Obsolete("Do not call directly, use a using(...) statement only.", true)]
        public void Dispose()
        {
            if (_hasLock)
                _lock.ReleaseWrite();
            _hasLock = false;
        }
    }
}