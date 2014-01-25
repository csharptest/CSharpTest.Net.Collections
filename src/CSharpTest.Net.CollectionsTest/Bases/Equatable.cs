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

namespace CSharpTest.Net.Bases
{
    /// <summary> Provides a base-class for non-reference equality objects </summary>
    [System.Diagnostics.DebuggerNonUserCode]
    public abstract class Equatable<T> : IEquatable<T>
        where T : Equatable<T>
    {
        /// <summary> return a non-reference equality comparer for this class </summary>
        public static readonly EqualityComparer Comparer = new EqualityComparer();

        /// <summary> Extracts the correct hash code </summary>
        protected abstract int HashCode { get; }

        /// <summary> Returns true if the other object is equal to this one </summary>
        public abstract bool Equals(T other);

        /// <summary> Returns true if the other object is equal to this one </summary>
        public sealed override bool Equals(object obj)
        {
            return Comparer.Equals(this as T, obj as T);
        }

        /// <summary> Extracts the correct hash code </summary>
        public sealed override int GetHashCode()
        {
            return Comparer.GetHashCode(this as T);
        }

        /// <summary> Compares the two objects for non-reference equality </summary>
        public static bool Equals(T x, T y)
        {
            return Comparer.Equals(x, y);
        }
        /// <summary> Compares the two objects for non-reference equality </summary>
        public static int GetHashCode(T obj)
        {
            return Comparer.GetHashCode(obj);
        }

        /// <summary> Compares the two objects for non-reference equality </summary>
        public static bool operator ==(Equatable<T> x, Equatable<T> y)
        {
            return Comparer.Equals(x as T, y as T);
        }
        /// <summary> Compares the two objects for non-reference equality </summary>
        public static bool operator !=(Equatable<T> x, Equatable<T> y)
        {
            return !Comparer.Equals(x as T, y as T);
        }

        /// <summary> Implements the equality comparer </summary>
        [System.Diagnostics.DebuggerNonUserCode]
        public sealed class EqualityComparer : EqualityComparer<T>, IEqualityComparer<T>
        {
            /// <summary> Compares the two objects for non-reference equality </summary>
            public override bool Equals(T x, T y) 
            {
                if (((object)x) == null) return ((object)y) == null;
                if (((object)y) == null) return false;
                return x.Equals(y);
            }
            /// <summary> Extracts the correct hash code </summary>
            public override int GetHashCode(T obj) 
            { 
                return ((object)obj) == null ? 0 : obj.HashCode;
            }
        }
    }
}
