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

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Disposes of each element in the collection when the collection is disposed.
    /// </summary>
    public class DisposingList : DisposingList<IDisposable>
    {
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that is empty and has the default initial capacity.
        /// </summary>
        public DisposingList() : base() { }
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that contains elements copied from the specified collection and has sufficient
        ///     capacity to accommodate the number of elements copied.
        ///</summary>
        public DisposingList(IEnumerable<IDisposable> collection) : base(collection) { }
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that is empty and has the specified initial capacity.
        ///</summary>
        public DisposingList(int capacity) : base(capacity) { }
    }
    
    /// <summary>
    /// Disposes of each element in the collection when the collection is disposed.
    /// </summary>
    public class DisposingList<T> : List<T>, IDisposable
        where T : IDisposable
    {
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that is empty and has the default initial capacity.
        /// </summary>
        public DisposingList() : base() { }
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that contains elements copied from the specified collection and has sufficient
        ///     capacity to accommodate the number of elements copied.
        ///</summary>
        public DisposingList(IEnumerable<T> collection) : base(collection) { }
        ///<summary>
        ///     Initializes a new instance of the System.Collections.Generic.List&gt;T> class
        ///     that is empty and has the specified initial capacity.
        ///</summary>
        public DisposingList(int capacity) : base(capacity) { }

        /// <summary>
        /// Disposes of each element in the collection when the collection is disposed.
        /// </summary>
        public void Dispose()
        {
            for (int i = base.Count - 1; i >= 0; i--)
            {
                T item = base[i];
                base.RemoveAt(i);

                if (item != null)
                    item.Dispose();
            }
        }
    }
}
