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
using CSharpTest.Net.Bases;
using System.ComponentModel;

#pragma warning disable 1591

namespace CSharpTest.Net.Library.Test
{
	[TestFixture]
	public partial class TestEquatable
	{
        public sealed class Null<T> : Comparable<Null<T>>
            where T : struct, IComparable
        {
            public readonly T Value;
            public Null(T value)
            { Value = value; }

            protected override int HashCode { get { return Value.GetHashCode(); } }
            public override string ToString()
            {
                return ((object)Value) == null ? String.Empty : Value.ToString();
            }

            public override int CompareTo(Null<T> other)
            {
                if (((object)other) == null) return 1;
                return Value.CompareTo(other.Value);
            }

            public static implicit operator T(Null<T> value) { return value.Value; /*throws null reference*/ }
            public static implicit operator T?(Null<T> value) { return value == null ? new T?() : value.Value; }
            public static implicit operator Null<T>(T value) { return new Null<T>(value); }
            public static implicit operator Null<T>(T? value) { return !value.HasValue ? null : new Null<T>(value.Value); }
        }

        [Test]
        public void TestEqualityOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.IsTrue(null == nil);
            Assert.IsTrue(nil == null);
            Assert.IsTrue(b1 == b2);
            Assert.IsFalse(a == b1);
            Assert.IsFalse(b1 == a);
            Assert.IsFalse(nil == a);
            Assert.IsFalse(a == nil);
            Assert.IsFalse(null == a);
            Assert.IsFalse(a == null);

            Assert.IsFalse(null != nil);
            Assert.IsFalse(nil != null);
            Assert.IsFalse(b1 != b2);
            Assert.IsTrue(a != b1);
            Assert.IsTrue(b1 != a);
            Assert.IsTrue(nil != a);
            Assert.IsTrue(a != nil);
            Assert.IsTrue(null != a);
            Assert.IsTrue(a != null);
        }

        [Test]
        public void TestLessThanOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.IsTrue(null <= nil);
            Assert.IsTrue(nil <= null);
            Assert.IsTrue(b1 <= b2);
            Assert.IsTrue(a <= b1);
            Assert.IsFalse(b1 <= a);
            Assert.IsTrue(nil <= a);
            Assert.IsFalse(a <= nil);
            Assert.IsTrue(null <= a);
            Assert.IsFalse(a <= null);

            Assert.IsFalse(null < nil);
            Assert.IsFalse(nil < null);
            Assert.IsFalse(b1 < b2);
            Assert.IsTrue(a < b1);
            Assert.IsFalse(b1 < a);
            Assert.IsTrue(nil < a);
            Assert.IsFalse(a < nil);
            Assert.IsTrue(null < a);
            Assert.IsFalse(a < null);
        }

        [Test]
        public void TestGreaterThanOperators()
        {
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.IsTrue(null >= nil);
            Assert.IsTrue(nil >= null);
            Assert.IsTrue(b1 >= b2);
            Assert.IsFalse(a >= b1);
            Assert.IsTrue(b1 >= a);
            Assert.IsFalse(nil >= a);
            Assert.IsTrue(a >= nil);
            Assert.IsFalse(null >= a);
            Assert.IsTrue(a >= null);

            Assert.IsFalse(null > nil);
            Assert.IsFalse(nil > null);
            Assert.IsFalse(b1 > b2);
            Assert.IsFalse(a > b1);
            Assert.IsTrue(b1 > a);
            Assert.IsFalse(nil > a);
            Assert.IsTrue(a > nil);
            Assert.IsFalse(null > a);
            Assert.IsTrue(a > null);
        }

        [Test]
        public void TestIEquatable()
        {
            Null<int> nil = null;
            IEquatable<Null<int>> a = new Null<int>(5);
            Null<int> b1 = 6;

            Assert.IsTrue(a.Equals(5));
            Assert.IsFalse(a.Equals(nil));
            Assert.IsFalse(a.Equals(b1));
        }

        [Test]
        public void TestIEqualityComparer()
        {
            IEqualityComparer<Null<int>> cmp = Null<int>.Comparer;
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.IsTrue(cmp.Equals(null, nil));
            Assert.IsTrue(cmp.Equals(nil, null));
            Assert.IsTrue(cmp.Equals(b1, b2));
            Assert.IsFalse(cmp.Equals(a, b1));
            Assert.IsFalse(cmp.Equals(b1, a));
            Assert.IsFalse(cmp.Equals(nil, a));
            Assert.IsFalse(cmp.Equals(a, nil));
            Assert.IsFalse(cmp.Equals(null, a));
            Assert.IsFalse(cmp.Equals(a, null));

            Assert.IsTrue(Null<int>.Equals(null, nil));
            Assert.IsTrue(Null<int>.Equals(nil, null));
            Assert.IsTrue(Null<int>.Equals(b1, b2));
            Assert.IsFalse(Null<int>.Equals(a, b1));
            Assert.IsFalse(Null<int>.Equals(b1, a));
            Assert.IsFalse(Null<int>.Equals(nil, a));
            Assert.IsFalse(Null<int>.Equals(a, nil));
            Assert.IsFalse(Null<int>.Equals(null, a));
            Assert.IsFalse(Null<int>.Equals(a, null));

            Assert.AreNotEqual(a.GetHashCode(), b2.GetHashCode());
            Assert.AreEqual(b1.GetHashCode(), b2.GetHashCode());

            Assert.AreEqual(cmp.GetHashCode(nil), 0);
            Assert.AreEqual(cmp.GetHashCode(a), a.GetHashCode());
            Assert.AreEqual(cmp.GetHashCode(b1), b2.GetHashCode());

            Assert.AreEqual(Null<int>.GetHashCode(nil), 0);
            Assert.AreEqual(Null<int>.GetHashCode(a), a.GetHashCode());
            Assert.AreEqual(Null<int>.GetHashCode(b1), b2.GetHashCode());
        }

        [Test]
        public void TestIComparable()
        {
            Null<int> nil = null;
            IComparable<Null<int>> a = new Null<int>(5);
            IComparable a1 = new Null<int>(5);
            Null<int> b1 = 6;

            Assert.IsTrue(a.CompareTo(5) == 0);
            Assert.IsTrue(a.CompareTo(nil) > 0);
            Assert.IsTrue(a.CompareTo(b1) < 0);

            Assert.IsTrue(a1.CompareTo((Null<int>)5) == 0);
            Assert.IsTrue(a1.CompareTo(nil) > 0);
            Assert.IsTrue(a1.CompareTo(b1) < 0);
        }

        [Test]
        public void TestIComparer()
        {
            IComparer<Null<int>> cmp = Null<int>.Comparer;
            Null<int> nil = null;
            Null<int> a = 5;
            Null<int> b1 = 6;
            Null<int> b2 = 6;

            Assert.IsTrue(cmp.Compare(null, nil) == 0);
            Assert.IsTrue(cmp.Compare(nil, null) == 0);
            Assert.IsTrue(cmp.Compare(b1, b2) == 0);
            Assert.IsTrue(cmp.Compare(a, b1) < 0);
            Assert.IsTrue(cmp.Compare(b1, a) > 0);
            Assert.IsTrue(cmp.Compare(nil, a) < 0);
            Assert.IsTrue(cmp.Compare(a, nil) > 0);
            Assert.IsTrue(cmp.Compare(null, a) < 0);
            Assert.IsTrue(cmp.Compare(a, null) > 0);
        }

        [Test]
        public void TestEquals()
        {
            object nil = null;
            object a = new Null<int>(5);
            object b1 = 6;

            Assert.IsTrue(a.Equals((Null<int>)5));
            Assert.IsFalse(a.Equals(nil));
            Assert.IsFalse(a.Equals(b1));
        }
    }
}
