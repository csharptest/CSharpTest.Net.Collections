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
using System.Threading;

namespace CSharpTest.Net.Synchronization
{
    /// <summary>
    /// Used to acquire a lock(object) with a timeout, either specified or the default of 2 minutes.
    /// </summary>
    public struct SafeLock : IDisposable
    {
        /// <summary>
        /// The default timeout value used when one is not provided to the constructor
        /// </summary>
        public const int DefaultTimeout = 120000;

        object _sync;
        
        /// <summary>
        /// Acquires the monitor lock on the object within 2 minutes, or throws TimeoutException
        /// </summary>
        public SafeLock(object monitor)
            : this(monitor, DefaultTimeout)
        { }

        /// <summary>
        /// Acquires the monitor lock on the object within timeoutMilliseconds, or throws TimeoutException
        /// </summary>
        public SafeLock(object monitor, int timeoutMilliseconds)
        {
            if (!Monitor.TryEnter(monitor, timeoutMilliseconds))
                throw new TimeoutException();
            _sync = monitor;
        }

        /// <summary> Releases the lock acquired by the constructor </summary>
        [Obsolete("Do not call directly, use a using(...) statement only.", true)]
        public void Dispose()
        {
            if (_sync != null)
            {
                object monitor = _sync;
                _sync = null;
                Monitor.Exit(monitor);
            }
        }
    }

    /// <summary>
    /// Exactly as SafeLock except that &lt;T> specifies the exception type to throw.
    /// Used to acquire a lock(object) with a timeout, either specified or the default of 2 minutes.
    /// </summary>
    public struct SafeLock<TException> : IDisposable
        where TException : Exception, new()
    {
        object _sync;

        /// <summary>
        /// Acquires the monitor lock on the object within 2 minutes, or throws TimeoutException
        /// </summary>
        public SafeLock(object monitor)
            : this(monitor, SafeLock.DefaultTimeout)
        { }

        /// <summary>
        /// Acquires the monitor lock on the object within timeoutMilliseconds, or throws TimeoutException
        /// </summary>
        public SafeLock(object monitor, int timeoutMilliseconds)
        {
            if (!Monitor.TryEnter(monitor, timeoutMilliseconds))
                throw new TException();
            _sync = monitor;
        }

        /// <summary> Releases the lock acquired by the constructor </summary>
        [Obsolete("Do not call directly, use a using(...) statement only.", true)]
        public void Dispose()
        {
            if (_sync != null)
            {
                object monitor = _sync;
                _sync = null;
                Monitor.Exit(monitor);
            }
        }
    }
}
