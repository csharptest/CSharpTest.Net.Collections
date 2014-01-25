#region Copyright 2010-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.ComponentModel;

namespace CSharpTest.Net.Bases
{
	/// <summary>
	/// Wraps the IDisposable object interface for classes that desire to be sure of being called 
	/// a single time for the dispose.
	/// </summary>
	public abstract class Disposable : IDisposable
	{
		private bool _isDisposed;
		private event EventHandler DisposedEvent;

		/// <summary> </summary>
		protected Disposable()
		{
			_isDisposed = false;
		}

		/// <summary> last-chance dispose </summary>
		~Disposable() 
		{ try { OnDispose(false); } catch { } }

		/// <summary> disposes of the object if it has not already been disposed </summary>
		public void Dispose()
		{
			try { OnDispose(true); }
			finally { GC.SuppressFinalize(this); }
		}

		private void OnDispose(bool disposing)
		{
			try
			{
				if (!_isDisposed)
				{
					Dispose(disposing);
					if (DisposedEvent != null)
						DisposedEvent(this, EventArgs.Empty);
				}
			}
			finally
			{
				_isDisposed = true;
				DisposedEvent = null;
			}
		}

		/// <summary> Raised when the object is disposed </summary>
		public event EventHandler Disposed
		{
			add { Assert(); DisposedEvent += value; }
			remove { DisposedEvent -= value; }
		}

		/// <summary> Raises the ObjectDisposedException if this object has already been disposed </summary>
		protected virtual void Assert()
		{
			if (_isDisposed) throw new ObjectDisposedException(GetType().FullName);
		}

		/// <summary> Your implementation of the dispose method </summary>
		protected abstract void Dispose(bool disposing);
	}
}
