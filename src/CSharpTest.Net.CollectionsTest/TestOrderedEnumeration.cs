#region Copyright 2012-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.IO;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestOrderedEnumeration
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

        [Test]
        public void TestKeyValueComparer()
        {
            KeyValueComparer<int, int> cmp = new KeyValueComparer<int, int>();
            Assert.IsTrue(ReferenceEquals(Comparer<int>.Default, cmp.Comparer));
            Assert.IsTrue(ReferenceEquals(Comparer<int>.Default, KeyValueComparer<int, int>.Default.Comparer));

            Assert.AreEqual(-1, cmp.Compare(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(2, 1)));
            Assert.AreEqual(0, cmp.Compare(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(1, 2)));
            Assert.AreEqual(1, cmp.Compare(new KeyValuePair<int, int>(2, 1), new KeyValuePair<int, int>(1, 1)));
        }

        [Test]
        public void TestMergeSortBasicOverloads()
        {
            Guid[] test, arrTest = new Guid[255];
            for (int i = 1; i < arrTest.Length; i++) 
                arrTest[i] = Guid.NewGuid();
            Guid[] expect = (Guid[])arrTest.Clone();
            Array.Sort(expect);

            test = (Guid[])arrTest.Clone();
            MergeSort.Sort(test);
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);

            test = (Guid[])arrTest.Clone();
            MergeSort.Sort(test, delegate(Guid x, Guid y) { return x.CompareTo(y); });
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);

            test = (Guid[])arrTest.Clone();
            MergeSort.Sort(test, Comparer<Guid>.Default);
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);
        }

        static void AssertArrayEquals<T>(IComparer<T> cmp, T[] x, T[] y)
        {
            Assert.AreEqual(x.Length, y.Length);
            for (int i = 0; i < x.Length; i++)
                Assert.AreEqual(0, cmp.Compare(x[i], y[i]));
        }

        [Test]
        public void TestMergeSortRangeOverloads()
        {
            char[] input = "zrogera".ToCharArray();
            MergeSort.Sort(input, 1, 5, Comparer<Char>.Default);
            Assert.AreEqual("zegorra", new string(input));

            input = "zrogera".ToCharArray();
            MergeSort.Sort(input, 1, 5, delegate(char x, char y) { return y.CompareTo(x); } );
            Assert.AreEqual("zrrogea", new string(input));
        }

        class AllEqual : IComparable<AllEqual>
        {
            public int CompareTo(AllEqual other)
            { return 0; }
        }

        [Test]
        public void TestMergeSortStable()
        {
            AllEqual[] set = new[] { new AllEqual(), new AllEqual(), new AllEqual() };
            AllEqual[] copy = (AllEqual[])set.Clone();
            MergeSort.Sort(copy);
            Assert.IsTrue(ReferenceEquals(set[0], copy[0]));
            Assert.IsTrue(ReferenceEquals(set[1], copy[1]));
            Assert.IsTrue(ReferenceEquals(set[2], copy[2]));
        }

        [Test]
        public void TestOrderedEnum()
        {
            byte[] input = new byte[256];
            new Random().NextBytes(input);

            byte last = 0;
            foreach (byte b in new OrderedEnumeration<byte>(input))
            {
                Assert.IsTrue(last <= b);
                last = b;
            }
        }

        class ReverseOrder<T> : IComparer<T>
        {
            readonly IComparer<T> _compare;
            public ReverseOrder(IComparer<T> compare)
            { _compare = compare; }
            public int Compare(T x, T y)
            {
                return -_compare.Compare(x, y);
            }
        }

        [Test]
        public void TestDedupFirst()
        {
            AllEqual[] set = new[] { new AllEqual(), new AllEqual(), new AllEqual() };
            List<AllEqual> arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                set, Comparer<AllEqual>.Default, DuplicateHandling.FirstValueWins));
            Assert.AreEqual(1, arr.Count);
            Assert.IsTrue(ReferenceEquals(set[0], arr[0]));

            arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                set, Comparer<AllEqual>.Default, DuplicateHandling.LastValueWins));
            Assert.AreEqual(1, arr.Count);
            Assert.IsTrue(ReferenceEquals(set[2], arr[0]));

            arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                set, Comparer<AllEqual>.Default, DuplicateHandling.None));
            Assert.AreEqual(3, arr.Count);
            Assert.IsTrue(ReferenceEquals(set[0], arr[0]));
            Assert.IsTrue(ReferenceEquals(set[1], arr[1]));
            Assert.IsTrue(ReferenceEquals(set[2], arr[2]));

            try
            {
                new List<AllEqual>(
                    OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                    set, Comparer<AllEqual>.Default, DuplicateHandling.RaisesException));
                Assert.Fail();
            }
            catch(ArgumentException) { }
        }

        [Test, ExpectedException(typeof(InvalidDataException))]
        public void TestUnorderedAssertion()
        {
            new List<int>(OrderedEnumeration<int>.WithDuplicateHandling(
                new [] { 2, 1 }, Comparer<int>.Default, DuplicateHandling.RaisesException));
        }

        private static IEnumerable<byte> FailBeforeYield<T>(bool bFail)
        {
            if (bFail) throw new InvalidOperationException();
            yield break;
        }

        [Test]
        public void TestOrderedEnumProperties()
        {
            var ordered = new OrderedEnumeration<byte>(Comparer<byte>.Default, FailBeforeYield<byte>(true));

            Assert.IsTrue(ReferenceEquals(Comparer<byte>.Default, ordered.Comparer));
            ordered.Comparer = new ReverseOrder<byte>(ordered.Comparer);
            Assert.IsTrue(ordered.Comparer is ReverseOrder<byte>);

            Assert.IsNull(ordered.Serializer);
            ordered.Serializer = PrimitiveSerializer.Byte;
            Assert.IsTrue(ReferenceEquals(ordered.Serializer, PrimitiveSerializer.Byte));

            Assert.AreEqual(0x10000, ordered.InMemoryLimit);
            Assert.AreEqual(10, ordered.InMemoryLimit = 10);

            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(DuplicateHandling.FirstValueWins,
                            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins);
        }

        [Test]
        public void TestOrderedEnumDedup()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            var ordered = new OrderedEnumeration<byte>(input);
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            int last = -1, count = 0;
            byte[] test = new List<byte>(ordered).ToArray();
            foreach (byte b in test)
            {
                count++;
                Assert.IsTrue(last < b);
                last = b;
            }
            Assert.IsTrue(count <= 256);
        }

        [Test]
        public void TestOrderedEnumPaginated()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            var ordered = new OrderedEnumeration<byte>(input);
            ordered.Serializer = PrimitiveSerializer.Byte;
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            int last = -1, count = 0;
            byte[] test = new List<byte>(ordered).ToArray();
            foreach (byte b in test)
            {
                count++;
                Assert.IsTrue(last < b);
                last = b;
            }
            Assert.IsTrue(count <= 256);
        }

        [Test]
        public void TestOrderedEnumPaginatedCleanup()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            var ordered = new OrderedEnumeration<byte>(input);
            ordered.Serializer = PrimitiveSerializer.Byte;
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            using (var e = ordered.GetEnumerator())
                Assert.IsTrue(e.MoveNext());
        }
        
        [Test]
        public void TestEnumTwiceFails()
        {
            var ordered = new OrderedEnumeration<byte>(new byte[0]);
            using (var e = ordered.GetEnumerator())
                Assert.IsFalse(e.MoveNext());

            try
            {
                ((System.Collections.IEnumerable) ordered).GetEnumerator();
                Assert.Fail();
            }
            catch (InvalidOperationException) { }
        }

        [Test]
        public void TestMergeEnumerations()
        {
            char[] x = "aeiou".ToCharArray();
            char[] y = "bcdfg".ToCharArray();
            char[] z = "ez".ToCharArray();

            var order = OrderedEnumeration<char>.Merge(x, y, z);
            Assert.AreEqual("abcdeefgiouz", new string(new List<char>(order).ToArray()));

            order = OrderedEnumeration<char>.Merge(Comparer<char>.Default, DuplicateHandling.LastValueWins, x, y, z);
            Assert.AreEqual("abcdefgiouz", new string(new List<char>(order).ToArray()));

            order = OrderedEnumeration<char>.Merge(Comparer<char>.Default, x, y);
            order = OrderedEnumeration<char>.WithDuplicateHandling(order, Comparer<char>.Default,
                                                                   DuplicateHandling.FirstValueWins);
            Assert.AreEqual("abcdefgiou", new string(new List<char>(order).ToArray()));
        }

        [Test]
        public void TestEnumInvalid()
        {
            var order = new OrderedEnumeration<byte>(new byte[1]);
            System.Collections.IEnumerator e = ((System.Collections.IEnumerable)order).GetEnumerator();
            Assert.IsTrue(e.MoveNext());
            Assert.IsFalse(e.MoveNext());
            try
            {
                object val = e.Current;
                GC.KeepAlive(val);
                Assert.Fail();
            }
            catch (InvalidOperationException) { }

            try
            {
                e.Reset();
                Assert.Fail();
            }
            catch (NotSupportedException) { }
        }

        [Test]
        public void TestOrderedKeyValuePairsMerge()
        {
            var x = new[] { new KeyValuePair<int, int>(1, 1) };
            var y = new[] { new KeyValuePair<int, int>(2, 2) };

            IEnumerator<KeyValuePair<int, int>> e = 
                OrderedKeyValuePairs<int, int>
                .Merge(new ReverseOrder<int>(Comparer<int>.Default), x, y)
                .GetEnumerator();

            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(2, e.Current.Key);
            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(1, e.Current.Key);
            Assert.IsFalse(e.MoveNext());
        }

        [Test]
        public void TestOrderedKeyValuePairsMergeOnDuplicate()
        {
            var x = new[] { new KeyValuePair<int, int>(1, 1) };
            var y = new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(2, 2) };

            IEnumerator<KeyValuePair<int, int>> e =
                OrderedKeyValuePairs<int, int>
                .Merge(Comparer<int>.Default, DuplicateHandling.FirstValueWins, x, y)
                .GetEnumerator();

            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(1, e.Current.Key);
            Assert.AreEqual(1, e.Current.Value);
            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(2, e.Current.Key);
            Assert.AreEqual(2, e.Current.Value);
            Assert.IsFalse(e.MoveNext());
            
            e = OrderedKeyValuePairs<int, int>
                .Merge(Comparer<int>.Default, DuplicateHandling.LastValueWins, x, y)
                .GetEnumerator();

            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(1, e.Current.Key);
            Assert.AreEqual(2, e.Current.Value);
            Assert.IsTrue(e.MoveNext());
            Assert.AreEqual(2, e.Current.Key);
            Assert.AreEqual(2, e.Current.Value);
            Assert.IsFalse(e.MoveNext());
        }

        [Test]
        public void TestOrderedKeyValuePairsOverloads()
        {
            IEnumerable<KeyValuePair<int,int>> e = new KeyValuePair<int,int>[0];
            OrderedKeyValuePairs<int, int> ordered;

            ordered = new OrderedKeyValuePairs<int, int>(e);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(ReferenceEquals(Comparer<int>.Default, ((KeyValueComparer<int, int>)ordered.Comparer).Comparer));
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(0x10000, ordered.InMemoryLimit);
            Assert.AreEqual(null, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(((KeyValueComparer<int, int>)ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(0x10000, ordered.InMemoryLimit);
            Assert.AreEqual(null, ordered.Serializer);

            KeyValueSerializer<int,int> ser = new KeyValueSerializer<int,int>(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32);
            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, ser);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(((KeyValueComparer<int, int>)ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(0x10000, ordered.InMemoryLimit);
            Assert.AreEqual(ser, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, ser, 42);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(((KeyValueComparer<int, int>)ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(42, ordered.InMemoryLimit);
            Assert.AreEqual(ser, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, PrimitiveSerializer.Int32, PrimitiveSerializer.Int32);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(((KeyValueComparer<int, int>)ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(0x10000, ordered.InMemoryLimit);
            Assert.IsNotNull(ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, PrimitiveSerializer.Int32, PrimitiveSerializer.Int32, 42);
            Assert.IsTrue(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.IsTrue(((KeyValueComparer<int, int>)ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.AreEqual(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.AreEqual(42, ordered.InMemoryLimit);
            Assert.IsNotNull(ordered.Serializer);
        }
    }
}
