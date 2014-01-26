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
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
	[TestFixture]
	public partial class TestDisposable
	{
		#region TestFixture SetUp/TearDown
		[TestFixtureSetUp]
		public virtual void Setup()
		{
		}

		[TestFixtureTearDown]
		public virtual void Teardown()
		{
		}
		#endregion

		class MyDisposable : Bases.Disposable
		{
			public int _disposedCount = 0;
			protected override void Dispose(bool disposing)
			{
				_disposedCount++;
			}
			public void TestAssert() { Assert(); }
		}

		[Test]
		public void TestDisposedOnce()
		{
			MyDisposable o = new MyDisposable();
			using (o)
			{
				Assert.AreEqual(0, o._disposedCount);
				o.Dispose();
				Assert.AreEqual(1, o._disposedCount);
				o.Dispose();
				Assert.AreEqual(1, o._disposedCount);
			}
			Assert.AreEqual(1, o._disposedCount);
		}

		[Test]
		public void TestDisposedEvent()
		{
			MyDisposable o = new MyDisposable();
			bool disposed = false;
			o.Disposed += delegate { disposed = true; };
			o.Dispose();
			Assert.IsTrue(disposed, "Disposed event failed.");
		}

		[Test]
		public void TestRemoveDisposedEvent()
		{
			MyDisposable o = new MyDisposable();
			bool disposed = false;
			EventHandler handler = delegate { disposed = true; };
			o.Disposed += handler;
			o.Disposed -= handler;
			o.Dispose();
			Assert.IsFalse(disposed, "Disposed fired?");
		}

		[Test]
		public void TestDisposeOnFinalize()
		{
			MyDisposable o = new MyDisposable();
			bool disposed = false;
			o.Disposed += delegate { disposed = true; };

			o = null;
			GC.Collect(0, GCCollectionMode.Forced);
			GC.WaitForPendingFinalizers();

			Assert.IsTrue(disposed, "Disposed event failed.");
		}

		[Test]
		public void TestAssertBeforeDispose()
		{
			MyDisposable o = new MyDisposable();
			o.TestAssert();
		}

		[Test, ExpectedException(typeof(ObjectDisposedException))]
		public void TestAssertWhenDisposed()
		{
			MyDisposable o = new MyDisposable();
			o.Dispose();
			o.TestAssert();
		}
	}
}
