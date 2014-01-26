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
    /// <summary> Singleton instance of ignore locking </summary>
    public class IgnoreLockFactory : ILockFactory
    {
        /// <summary> Returns the IgnoreLocking.Instance singleton </summary>
        public ILockStrategy Create() { return IgnoreLocking.Instance; }
    }

    /// <summary>
    /// wraps the reader/writer lock around Monitor
    /// </summary>
    public class IgnoreLocking : ILockStrategy
    {
        /// <summary> Singleton instance of ignore locking </summary>
        public static IgnoreLocking Instance = new IgnoreLocking();

        void IDisposable.Dispose() { }

        /// <summary> Returns Zero. </summary>
        public int WriteVersion { get { return 0; } }

        /// <summary>
        /// Returns true if the lock was successfully obtained within the timeout specified
        /// </summary>
        public bool TryRead(int timeout)
        {
            return true;
        }

        /// <summary>
        /// Releases a read lock
        /// </summary>
        public void ReleaseRead()
        { }

        /// <summary>
        /// Returns true if the lock was successfully obtained within the timeout specified
        /// </summary>
        public bool TryWrite(int timeout)
        {
            return true;
        }

        /// <summary>
        /// Releases a writer lock
        /// </summary>
        public void ReleaseWrite()
        { }

        /// <summary>
        /// Returns a reader lock that can be elevated to a write lock
        /// </summary>
        public ReadLock Read() { return new ReadLock(this, true); }

        /// <summary>
        /// Returns a reader lock that can be elevated to a write lock
        /// </summary>
        /// <exception cref="System.TimeoutException"/>
        public ReadLock Read(int timeout) { return new ReadLock(this, true); }

        /// <summary>
        /// Returns a read and write lock
        /// </summary>
        public WriteLock Write() { return new WriteLock(this, true); }

        /// <summary>
        /// Returns a read and write lock
        /// </summary>
        /// <exception cref="System.TimeoutException"/>
        public WriteLock Write(int timeout) { return new WriteLock(this, true); }
    }
}
