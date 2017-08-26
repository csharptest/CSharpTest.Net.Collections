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
using CSharpTest.Net.Collections.Test.Bases;
using Xunit;

#pragma warning disable 1591

namespace CSharpTest.Net.Collections.Test
{
    
    public class TestEquatable
    {
        public sealed class Null<T> : Comparable<Null<T>>
            where T : struct, IComparable
        {
            public readonly T Value;

            public Null(T value)
            {
                Value = value;
            }

            protected override int HashCode => Value.GetHashCode();

            public override string ToString()
            {
                return (object) Value == null ? string.Empty : Value.ToString();
            }

            public override int CompareTo(Null<T> other)
            {
                if ((object) other == null) return 1;
                return Value.CompareTo(other.Value);
            }

            public static implicit operator T(Null<T> value)
            {
                return value.Value; /*throws null reference*/
            }

            public static implicit operator T?(Null<T> value)
            {
                return value == null ? new T?() : value.Value;
            }

            public static implicit operator Null<T>(T value)
            {
                return new Null<T>(value);
            }

            public static implicit operator Null<T>(T? value)
            {
                return !value.HasValue ? null : new Null<T>(value.Value);
            }
        }

        [Fact]
        public void TestEqualityOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.True(null == nil);
            Assert.True(nil == null);
            Assert.True(b1 == b2);
            Assert.False(a == b1);
            Assert.False(b1 == a);
            Assert.False(nil == a);
            Assert.False(a == nil);
            Assert.False(null == a);
            Assert.False(a == null);

            Assert.False(null != nil);
            Assert.False(nil != null);
            Assert.False(b1 != b2);
            Assert.True(a != b1);
            Assert.True(b1 != a);
            Assert.True(nil != a);
            Assert.True(a != nil);
            Assert.True(null != a);
            Assert.True(a != null);
        }

        [Fact]
        public void TestEquals()
        {
            object nil = null;
            object a = new Null<int>(5);
            object b1 = 6;

            Assert.True(a.Equals((Null<int>) 5));
            Assert.False(a.Equals(nil));
            Assert.False(a.Equals(b1));
        }

        [Fact]
        public void TestGreaterThanOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.True(null >= nil);
            Assert.True(nil >= null);
            Assert.True(b1 >= b2);
            Assert.False(a >= b1);
            Assert.True(b1 >= a);
            Assert.False(nil >= a);
            Assert.True(a >= nil);
            Assert.False(null >= a);
            Assert.True(a >= null);

            Assert.False(null > nil);
            Assert.False(nil > null);
            Assert.False(b1 > b2);
            Assert.False(a > b1);
            Assert.True(b1 > a);
            Assert.False(nil > a);
            Assert.True(a > nil);
            Assert.False(null > a);
            Assert.True(a > null);
        }

        [Fact]
        public void TestIComparable()
        {
            Null<int> nil = null;
            IComparable<Null<int>> a = new Null<int>(5);
            IComparable a1 = new Null<int>(5);
            Null<int> b1 = 6;

            Assert.True(a.CompareTo(5) == 0);
            Assert.True(a.CompareTo(nil) > 0);
            Assert.True(a.CompareTo(b1) < 0);

            Assert.True(a1.CompareTo((Null<int>) 5) == 0);
            Assert.True(a1.CompareTo(nil) > 0);
            Assert.True(a1.CompareTo(b1) < 0);
        }

        [Fact]
        public void TestIComparer()
        {
            IComparer<Null<int>> cmp = Null<int>.Comparer;
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.True(cmp.Compare(null, nil) == 0);
            Assert.True(cmp.Compare(nil, null) == 0);
            Assert.True(cmp.Compare(b1, b2) == 0);
            Assert.True(cmp.Compare(a, b1) < 0);
            Assert.True(cmp.Compare(b1, a) > 0);
            Assert.True(cmp.Compare(nil, a) < 0);
            Assert.True(cmp.Compare(a, nil) > 0);
            Assert.True(cmp.Compare(null, a) < 0);
            Assert.True(cmp.Compare(a, null) > 0);
        }

        [Fact]
        public void TestIEqualityComparer()
        {
            IEqualityComparer<Null<int>> cmp = Null<int>.Comparer;
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.True(cmp.Equals(null, nil));
            Assert.True(cmp.Equals(nil, null));
            Assert.True(cmp.Equals(b1, b2));
            Assert.False(cmp.Equals(a, b1));
            Assert.False(cmp.Equals(b1, a));
            Assert.False(cmp.Equals(nil, a));
            Assert.False(cmp.Equals(a, nil));
            Assert.False(cmp.Equals(null, a));
            Assert.False(cmp.Equals(a, null));

            Assert.True(Null<int>.Equals(null, nil));
            Assert.True(Null<int>.Equals(nil, null));
            Assert.True(Null<int>.Equals(b1, b2));
            Assert.False(Null<int>.Equals(a, b1));
            Assert.False(Null<int>.Equals(b1, a));
            Assert.False(Null<int>.Equals(nil, a));
            Assert.False(Null<int>.Equals(a, nil));
            Assert.False(Null<int>.Equals(null, a));
            Assert.False(Null<int>.Equals(a, null));

            Assert.NotEqual(a.GetHashCode(), b2.GetHashCode());
            Assert.Equal(b1.GetHashCode(), b2.GetHashCode());

            Assert.Equal(cmp.GetHashCode(nil), 0);
            Assert.Equal(cmp.GetHashCode(a), a.GetHashCode());
            Assert.Equal(cmp.GetHashCode(b1), b2.GetHashCode());

            Assert.Equal(Null<int>.GetHashCode(nil), 0);
            Assert.Equal(Null<int>.GetHashCode(a), a.GetHashCode());
            Assert.Equal(Null<int>.GetHashCode(b1), b2.GetHashCode());
        }

        [Fact]
        public void TestIEquatable()
        {
            Null<int> nil = null;
            IEquatable<Null<int>> a = new Null<int>(5);
            Null<int> b1 = 6;

            Assert.True(a.Equals(5));
            Assert.False(a.Equals(nil));
            Assert.False(a.Equals(b1));
        }

        [Fact]
        public void TestLessThanOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.True(null <= nil);
            Assert.True(nil <= null);
            Assert.True(b1 <= b2);
            Assert.True(a <= b1);
            Assert.False(b1 <= a);
            Assert.True(nil <= a);
            Assert.False(a <= nil);
            Assert.True(null <= a);
            Assert.False(a <= null);

            Assert.False(null < nil);
            Assert.False(nil < null);
            Assert.False(b1 < b2);
            Assert.True(a < b1);
            Assert.False(b1 < a);
            Assert.True(nil < a);
            Assert.False(a < nil);
            Assert.True(null < a);
            Assert.False(a < null);
        }
    }
}