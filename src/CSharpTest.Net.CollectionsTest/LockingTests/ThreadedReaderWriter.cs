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
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test.LockingTests
{
    public class ThreadedReader : ThreadedWriter
    {
        public ThreadedReader(ILockStrategy lck)
            : base(lck)
        {
        }

        protected override IDisposable Lock()
        {
            return _lck.Read(100);
        }
    }

    public class ThreadedWriter : IDisposable
    {
        private readonly IAsyncResult _async;
        private readonly ManualResetEvent _complete;
        private readonly ThreadStart _delegate;
        protected readonly ILockStrategy _lck;
        private readonly ManualResetEvent _started;
        private bool _locked;

        public ThreadedWriter(ILockStrategy lck)
        {
            _started = new ManualResetEvent(false);
            _complete = new ManualResetEvent(false);

            _lck = lck;
            _delegate = HoldLock;
            _async = _delegate.BeginInvoke(null, null);
            if (!_started.WaitOne(1000, false))
            {
                _delegate.EndInvoke(_async);
                Assert.Fail("Unable to acquire lock");
            }
            Assert.IsTrue(_locked);
        }

        public void Dispose()
        {
            if (_locked)
            {
                _locked = false;
                _complete.Set();
                _delegate.EndInvoke(_async);
            }
        }

        private void HoldLock()
        {
            using (Lock())
            {
                _locked = true;
                _started.Set();
                _complete.WaitOne();
            }
        }

        protected virtual IDisposable Lock()
        {
            return _lck.Write(100);
        }
    }
}