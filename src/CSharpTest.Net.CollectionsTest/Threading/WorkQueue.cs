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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Action = System.Threading.ThreadStart;

namespace CSharpTest.Net.Threading
{
    /// <summary>
    ///     An extremely basic WorkQueue using a fixed number of threads to execute Action() or Action&lt;T> delegates
    /// </summary>
    [DebuggerNonUserCode]
    public class WorkQueue : WorkQueue<Action>
    {
        /// <summary>
        ///     Constructs the Work Queue with the specified number of threads.
        /// </summary>
        public WorkQueue(int nThreads) : base(DoAction, nThreads)
        {
        }

        private static void DoAction(Action process)
        {
            process();
        }

        /// <summary> Enqueues a task with a parameter of type T </summary>
        public void Enqueue<T>(Action<T> process, T instance)
        {
            Enqueue(new WorkItem<T>(process, instance).Exec);
        }

        [DebuggerNonUserCode]
        private class WorkItem<T>
        {
            private readonly T _instance;
            private readonly Action<T> _process;

            public WorkItem(Action<T> process, T instance)
            {
                _process = process;
                _instance = instance;
            }

            public void Exec()
            {
                _process(_instance);
            }
        }
    }

    public class ErrorEventArgs : EventArgs
    {
        private Exception e;

        public ErrorEventArgs(Exception e)
        {
            this.e = e;
        }

        public Exception GetException()
        {
            return e;
        }
    }

    public delegate void ErrorEventHandler(
        object sender,
        ErrorEventArgs e
    );

    /// <summary>
    ///     An extremely basic WorkQueue using a fixed number of threads to execute Action&lt;T>
    ///     over the enqueued instances of type T, aggregates an instance of WorkQueue()
    /// </summary>
    public class WorkQueue<T> : IWorkQueue<T>
    {
        private readonly Action<T> _process;
        private readonly Queue<T> _queue;
        private readonly ManualResetEvent _quit;
        private readonly ManualResetEvent _ready;
        private readonly Thread[] _workers;
        private bool _disposed, _completePending;

        /// <summary>
        ///     Constructs the Work Queue with the specified number of threads.
        /// </summary>
        public WorkQueue(Action<T> process, int nThreads)
        {
            _process = Check.NotNull(process);
            Check.InRange(nThreads, 1, 10000);
            _queue = new Queue<T>(nThreads * 2);
            _quit = new ManualResetEvent(false);
            _ready = new ManualResetEvent(false);

            _workers = new Thread[nThreads];
            for (int i = 0; i < nThreads; i++)
            {
                _workers[i] = new Thread(Run);
                _workers[i].IsBackground = true;
                _workers[i].Name = string.Format("WorkQueue[{0}]", i);
                _workers[i].Start();
            }
            _completePending = false;
        }

        /// <summary> Raised when a task fails to handle an error </summary>
        public event ErrorEventHandler OnError;

        /// <summary>Immediatly stops processing tasks and exits all worker threads</summary>
        void IDisposable.Dispose()
        {
            Complete(false, 100);
        }

        /// <summary>
        ///     Waits for all executing tasks to complete and then exists all threads, If completePending
        ///     is false no more tasks will begin, if true threads will continue to pick up tasks and
        ///     run until the queue is empty.  The timeout period is used to join each thread in turn,
        ///     if the timeout expires that thread will be aborted.
        /// </summary>
        /// <param name="completePending">True to complete enqueued activities</param>
        /// <param name="timeout">The timeout to wait for a thread before Abort() is called</param>
        public bool Complete(bool completePending, int timeout)
        {
            bool shutdownFailed = false;
            bool completed = completePending;
            if (_disposed) return completed;
            try
            {
                _completePending = completePending;
                _quit.Set();
                foreach (Thread t in _workers)
                    if (!t.Join(timeout))
                    {
                        completed = false;
                       
                        if (!t.Join(10000))
                            shutdownFailed = true;
                    }

                if (shutdownFailed)
                    throw new Exception("WorkQueue shutdown failed, unable to join worker threads.");
            }
            finally
            {
                lock (_queue)
                {
                    _disposed = true;
                    if (!completePending)
                    {
                        _queue.Clear();
                    }
                    else
                    {
                        // usually a sign that other threads are still enqueuing messages
                        if (_queue.Count > 0)
                            Run();
                        Check.IsEqual(0, _queue.Count);
                    }
                }
            }
            return completed;
        }

        /// <summary> Enqueues a task </summary>
        public void Enqueue(T instance)
        {
            lock (_queue)
            {
                if (_disposed)
                    throw new ObjectDisposedException(GetType().FullName);

                _queue.Enqueue(instance);
                _ready.Set();
            }
        }

        private void Run()
        {
            WaitHandle[] wait = {_quit, _ready};

            while (WaitHandle.WaitAny(wait) == 1 || _completePending)
            {
                T item;
                lock (_queue)
                {
                    if (_queue.Count > 0)
                    {
                        item = _queue.Dequeue();
                    }
                    else
                    {
                        if (_quit.WaitOne(0))
                            break;

                        _ready.Reset();
                        continue;
                    }
                }

                try
                {
                    _process(item);
                }
                catch (Exception e)
                {
                    ErrorEventHandler h = OnError;
                    if (h != null)
                        h(item, new ErrorEventArgs(e));
                }
            }
        }
    }
}