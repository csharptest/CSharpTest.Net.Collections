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
using CSharpTest.Net.Bases;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.Synchronization
{
    /// <summary> A factory that produces instances of ILockStrategy to aquire/release read/write locks </summary>
    public interface ILockFactory : IFactory<ILockStrategy>
    { }

    /// <summary> A generic implementation that constructs a lock by type </summary>
    public class LockFactory<T> : ILockFactory
        where T : ILockStrategy, new()
    {
        /// <summary> Returns a new lock of type T </summary>
        public ILockStrategy Create()
        { return new T(); }
    }
}
