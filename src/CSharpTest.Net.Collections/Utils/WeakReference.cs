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

namespace CSharpTest.Net.Utils
{
    /// <summary>
    /// A strong-typed derivation of the WeakReference class
    /// </summary>
    [Serializable]
    public class WeakReference<T> : WeakReference
        where T : class
    {
        /// <summary> Creates a new WeakReference that keeps track of target. </summary>
        public WeakReference(T instance)
            : base(instance)
        { }

        /// <summary />
        protected WeakReference(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        { }

        /// <summary>
        /// Gets an indication whether the object referenced by the current object has been garbage collected.
        /// </summary>
        public override bool IsAlive { get { return base.IsAlive && base.Target is T; } }

        /// <summary> Gets or sets the Object stored in the handle if it's accessible. </summary>
        public new T Target
        {
            get { return base.Target as T; }
            set { base.Target = value; }
        }

        /// <summary> Returns true if the Object was retrieved. </summary>
        public bool TryGetTarget(out T value)
        {
            value = base.Target as T;
            return value != null;
        }
    }
}
