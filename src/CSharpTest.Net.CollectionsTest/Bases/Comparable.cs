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
using System.ComponentModel;

namespace CSharpTest.Net.Bases
{
    /// <summary> Provides a base-class for non-reference comparison of objects </summary>
    [System.Diagnostics.DebuggerNonUserCode]
    public abstract class Comparable<T> : Equatable<T>, IComparable, IComparable<T>
        where T : Comparable<T>
    {
        /// <summary> returns a non-reference comparer for this class </summary>
        public static new readonly EqualityComparer Comparer = new EqualityComparer();

        /// <summary> Returns true if the object is equal </summary>
        public override bool Equals(T other)
        { return ((object)other) == null ? false : (0 == this.CompareTo(other)); }

        /// <summary> Compares with another object of T </summary>
        public abstract int CompareTo(T other);

        int IComparable.CompareTo(object obj)
        { return this.CompareTo(obj as T); }

        /// <summary> Compares two objects </summary>
        public static bool operator <(Comparable<T> a, Comparable<T> b)
        {
            return Comparer.Compare(a as T, b as T) < 0;
        }
        /// <summary> Compares two objects </summary>
        public static bool operator <=(Comparable<T> a, Comparable<T> b)
        {
            return Comparer.Compare(a as T, b as T) <= 0;
        }
        /// <summary> Compares two objects </summary>
        public static bool operator >(Comparable<T> a, Comparable<T> b)
        {
            return Comparer.Compare(a as T, b as T) > 0;
        }
        /// <summary> Compares two objects </summary>
        public static bool operator >=(Comparable<T> a, Comparable<T> b)
        {
            return Comparer.Compare(a as T, b as T) >= 0;
        }

        /// <summary> Implements the equality comparer </summary>
        [System.Diagnostics.DebuggerNonUserCode]
        public sealed new class EqualityComparer : Comparer<T>, IEqualityComparer<T>, IComparer<T>
        {
            /// <summary> Compares the two objects for non-reference equality </summary>
            public bool Equals(T x, T y)
            {
                if (((object)x) == null) return ((object)y) == null;
                if (((object)y) == null) return false;
                return 0 == x.CompareTo(y);
            }
            /// <summary> Extracts the correct hash code </summary>
            public int GetHashCode(T obj)
            {
                return ((object)obj) == null ? 0 : obj.HashCode;
            }
            /// <summary> Returns the comparison between the two objects </summary>
            public override int Compare(T x, T y)
            {
                if (((object)x) == null) return ((object)y) == null ? 0 : -1;
                if (((object)y) == null) return 1;
                return x.CompareTo(y);
            }

            #region Hide base-members
#if false
            private const string ERROR = "This method should not be called.";

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public static new bool ReferenceEquals(object x, object y) { return Object.ReferenceEquals(x, y); }

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public static new bool Equals(object x, object y) { return Object.Equals(x, y); }

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public new bool Equals(object obj) { return base.Equals(obj); }

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public new int GetHashCode() { return base.GetHashCode(); }

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public new string ToString() { return base.ToString(); }

            [Obsolete(ERROR, true), DebuggerHidden, EditorBrowsable(EditorBrowsableState.Never)]
            public new Type GetType() { return base.GetType(); }
#endif
            #endregion
        }
    }
}
