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
using System.IO;

namespace CSharpTest.Net.Threading
{
    /// <summary> Provides an interface for a simple WorkQueue </summary>
    public interface IWorkQueue<T> : IDisposable
    {
        /// <summary> Raised when a task fails to handle an error </summary>
        event ErrorEventHandler OnError;
        /// <summary>Waits for all queued tasks to complete and then exists all threads</summary>
        /// <returns>Returns true if all pending tasks were completed before the timeout expired.</returns>
        bool Complete(bool completePending, int timeout);
        /// <summary> Enqueues a task </summary>
        void Enqueue(T instance);
    }
}