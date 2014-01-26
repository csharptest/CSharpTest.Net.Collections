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
	/// An interface that allows reader/writer locking with the using() statement
	/// </summary>
	public interface ILockStrategy : IDisposable
	{
		/// <summary>
		/// Returns a reader lock that can be elevated to a write lock
		/// </summary>
        ReadLock Read();
        /// <summary>
        /// Returns a reader lock that can be elevated to a write lock
        /// </summary>
        /// <exception cref="System.TimeoutException"/>
        ReadLock Read(int timeout);
		/// <summary>
		/// Returns true if the lock was successfully obtained within the timeout specified
		/// </summary>
		bool TryRead(int timeout);
        /// <summary>
        /// Releases a read lock
        /// </summary>
        void ReleaseRead();
        /// <summary>
        /// The the current writer sequence number
        /// </summary>
        int WriteVersion { get; }
		/// <summary>
		/// Returns a read and write lock
		/// </summary>
        WriteLock Write();
        /// <summary>
        /// Returns a read and write lock
        /// </summary>
        /// <exception cref="System.TimeoutException"/>
        WriteLock Write(int timeout);
		/// <summary>
		/// Returns true if the lock was successfully obtained within the timeout specified
		/// </summary>
		bool TryWrite(int timeout);
        /// <summary>
        /// Releases a writer lock
        /// </summary>
        void ReleaseWrite();
	}
}
