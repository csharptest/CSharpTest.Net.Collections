#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using CSharpTest.Net.Serialization;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    
    public class TestOrderedEnumeration
    {
        private static void AssertArrayEquals<T>(IComparer<T> cmp, T[] x, T[] y)
        {
            Assert.Equal(x.Length, y.Length);
            for (int i = 0; i < x.Length; i++)
                Assert.Equal(0, cmp.Compare(x[i], y[i]));
        }

        private class AllEqual : IComparable<AllEqual>
        {
            public int CompareTo(AllEqual other)
            {
                return 0;
            }
        }

        private class ReverseOrder<T> : IComparer<T>
        {
            private readonly IComparer<T> _compare;

            public ReverseOrder(IComparer<T> compare)
            {
                _compare = compare;
            }

            public int Compare(T x, T y)
            {
                return -_compare.Compare(x, y);
            }
        }

        private static IEnumerable<byte> FailBeforeYield<T>(bool bFail)
        {
            if (bFail) throw new InvalidOperationException();
            yield break;
        }

        [Fact]
        public void TestDedupFirst()
        {
            AllEqual[] set = {new AllEqual(), new AllEqual(), new AllEqual()};
            List<AllEqual> arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                    set, Comparer<AllEqual>.Default, DuplicateHandling.FirstValueWins));
            Assert.Equal(1, arr.Count);
            Assert.True(ReferenceEquals(set[0], arr[0]));

            arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                    set, Comparer<AllEqual>.Default, DuplicateHandling.LastValueWins));
            Assert.Equal(1, arr.Count);
            Assert.True(ReferenceEquals(set[2], arr[0]));

            arr = new List<AllEqual>(
                OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                    set, Comparer<AllEqual>.Default, DuplicateHandling.None));
            Assert.Equal(3, arr.Count);
            Assert.True(ReferenceEquals(set[0], arr[0]));
            Assert.True(ReferenceEquals(set[1], arr[1]));
            Assert.True(ReferenceEquals(set[2], arr[2]));

            try
            {
                new List<AllEqual>(
                    OrderedEnumeration<AllEqual>.WithDuplicateHandling(
                        set, Comparer<AllEqual>.Default, DuplicateHandling.RaisesException));
                Assert.True(false);
            }
            catch (ArgumentException)
            {
            }
        }

        [Fact]
        public void TestEnumInvalid()
        {
            OrderedEnumeration<byte> order = new OrderedEnumeration<byte>(new byte[1]);
            IEnumerator e = ((IEnumerable) order).GetEnumerator();
            Assert.True(e.MoveNext());
            Assert.False(e.MoveNext());
            try
            {
                object val = e.Current;
                GC.KeepAlive(val);
                Assert.True(false);
            }
            catch (InvalidOperationException)
            {
            }

            try
            {
                e.Reset();
                Assert.True(false);
            }
            catch (NotSupportedException)
            {
            }
        }

        [Fact]
        public void TestEnumTwiceFails()
        {
            OrderedEnumeration<byte> ordered = new OrderedEnumeration<byte>(new byte[0]);
            using (IEnumerator<byte> e = ordered.GetEnumerator())
            {
                Assert.False(e.MoveNext());
            }

            try
            {
                ((IEnumerable) ordered).GetEnumerator();
                Assert.True(false);
            }
            catch (InvalidOperationException)
            {
            }
        }

        [Fact]
        public void TestKeyValueComparer()
        {
            KeyValueComparer<int, int> cmp = new KeyValueComparer<int, int>();
            Assert.True(ReferenceEquals(Comparer<int>.Default, cmp.Comparer));
            Assert.True(ReferenceEquals(Comparer<int>.Default, KeyValueComparer<int, int>.Default.Comparer));

            Assert.Equal(-1, cmp.Compare(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(2, 1)));
            Assert.Equal(0, cmp.Compare(new KeyValuePair<int, int>(1, 1), new KeyValuePair<int, int>(1, 2)));
            Assert.Equal(1, cmp.Compare(new KeyValuePair<int, int>(2, 1), new KeyValuePair<int, int>(1, 1)));
        }

        [Fact]
        public void TestMergeEnumerations()
        {
            char[] x = "aeiou".ToCharArray();
            char[] y = "bcdfg".ToCharArray();
            char[] z = "ez".ToCharArray();

            IEnumerable<char> order = OrderedEnumeration<char>.Merge(x, y, z);
            Assert.Equal("abcdeefgiouz", new string(new List<char>(order).ToArray()));

            order = OrderedEnumeration<char>.Merge(Comparer<char>.Default, DuplicateHandling.LastValueWins, x, y, z);
            Assert.Equal("abcdefgiouz", new string(new List<char>(order).ToArray()));

            order = OrderedEnumeration<char>.Merge(Comparer<char>.Default, x, y);
            order = OrderedEnumeration<char>.WithDuplicateHandling(order, Comparer<char>.Default,
                DuplicateHandling.FirstValueWins);
            Assert.Equal("abcdefgiou", new string(new List<char>(order).ToArray()));
        }

        [Fact]
        public void TestMergeSortBasicOverloads()
        {
            Guid[] test, arrTest = new Guid[255];
            for (int i = 1; i < arrTest.Length; i++)
                arrTest[i] = Guid.NewGuid();
            Guid[] expect = (Guid[]) arrTest.Clone();
            Array.Sort(expect);

            test = (Guid[]) arrTest.Clone();
            MergeSort.Sort(test);
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);

            test = (Guid[]) arrTest.Clone();
            MergeSort.Sort(test, delegate(Guid x, Guid y) { return x.CompareTo(y); });
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);

            test = (Guid[]) arrTest.Clone();
            MergeSort.Sort(test, Comparer<Guid>.Default);
            AssertArrayEquals(Comparer<Guid>.Default, expect, test);
        }

        [Fact]
        public void TestMergeSortRangeOverloads()
        {
            char[] input = "zrogera".ToCharArray();
            MergeSort.Sort(input, 1, 5, Comparer<char>.Default);
            Assert.Equal("zegorra", new string(input));

            input = "zrogera".ToCharArray();
            MergeSort.Sort(input, 1, 5, delegate(char x, char y) { return y.CompareTo(x); });
            Assert.Equal("zrrogea", new string(input));
        }

        [Fact]
        public void TestMergeSortStable()
        {
            AllEqual[] set = {new AllEqual(), new AllEqual(), new AllEqual()};
            AllEqual[] copy = (AllEqual[]) set.Clone();
            MergeSort.Sort(copy);
            Assert.True(ReferenceEquals(set[0], copy[0]));
            Assert.True(ReferenceEquals(set[1], copy[1]));
            Assert.True(ReferenceEquals(set[2], copy[2]));
        }

        [Fact]
        public void TestOrderedEnum()
        {
            byte[] input = new byte[256];
            new Random().NextBytes(input);

            byte last = 0;
            foreach (byte b in new OrderedEnumeration<byte>(input))
            {
                Assert.True(last <= b);
                last = b;
            }
        }

        [Fact]
        public void TestOrderedEnumDedup()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            OrderedEnumeration<byte> ordered = new OrderedEnumeration<byte>(input);
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            int last = -1, count = 0;
            byte[] test = new List<byte>(ordered).ToArray();
            foreach (byte b in test)
            {
                count++;
                Assert.True(last < b);
                last = b;
            }
            Assert.True(count <= 256);
        }

        [Fact]
        public void TestOrderedEnumPaginated()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            OrderedEnumeration<byte> ordered = new OrderedEnumeration<byte>(input);
            ordered.Serializer = PrimitiveSerializer.Byte;
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            int last = -1, count = 0;
            byte[] test = new List<byte>(ordered).ToArray();
            foreach (byte b in test)
            {
                count++;
                Assert.True(last < b);
                last = b;
            }
            Assert.True(count <= 256);
        }

        [Fact]
        public void TestOrderedEnumPaginatedCleanup()
        {
            byte[] input = new byte[512];
            new Random().NextBytes(input);
            OrderedEnumeration<byte> ordered = new OrderedEnumeration<byte>(input);
            ordered.Serializer = PrimitiveSerializer.Byte;
            ordered.InMemoryLimit = 10;
            ordered.DuplicateHandling = DuplicateHandling.FirstValueWins;

            using (IEnumerator<byte> e = ordered.GetEnumerator())
            {
                Assert.True(e.MoveNext());
            }
        }

        [Fact]
        public void TestOrderedEnumProperties()
        {
            OrderedEnumeration<byte> ordered = new OrderedEnumeration<byte>(Comparer<byte>.Default, FailBeforeYield<byte>(true));

            Assert.True(ReferenceEquals(Comparer<byte>.Default, ordered.Comparer));
            ordered.Comparer = new ReverseOrder<byte>(ordered.Comparer);
            Assert.True(ordered.Comparer is ReverseOrder<byte>);

            Assert.Null(ordered.Serializer);
            ordered.Serializer = PrimitiveSerializer.Byte;
            Assert.True(ReferenceEquals(ordered.Serializer, PrimitiveSerializer.Byte));

            Assert.Equal(0x10000, ordered.InMemoryLimit);
            Assert.Equal(10, ordered.InMemoryLimit = 10);

            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(DuplicateHandling.FirstValueWins,
                ordered.DuplicateHandling = DuplicateHandling.FirstValueWins);
        }

        [Fact]
        public void TestOrderedKeyValuePairsMerge()
        {
            KeyValuePair<int, int>[] x = new[] {new KeyValuePair<int, int>(1, 1)};
            KeyValuePair<int, int>[] y = new[] {new KeyValuePair<int, int>(2, 2)};

            IEnumerator<KeyValuePair<int, int>> e =
                OrderedKeyValuePairs<int, int>
                    .Merge(new ReverseOrder<int>(Comparer<int>.Default), x, y)
                    .GetEnumerator();

            Assert.True(e.MoveNext());
            Assert.Equal(2, e.Current.Key);
            Assert.True(e.MoveNext());
            Assert.Equal(1, e.Current.Key);
            Assert.False(e.MoveNext());
        }

        [Fact]
        public void TestOrderedKeyValuePairsMergeOnDuplicate()
        {
            KeyValuePair<int, int>[] x = new[] {new KeyValuePair<int, int>(1, 1)};
            KeyValuePair<int, int>[] y = new[] {new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(2, 2)};

            IEnumerator<KeyValuePair<int, int>> e =
                OrderedKeyValuePairs<int, int>
                    .Merge(Comparer<int>.Default, DuplicateHandling.FirstValueWins, x, y)
                    .GetEnumerator();

            Assert.True(e.MoveNext());
            Assert.Equal(1, e.Current.Key);
            Assert.Equal(1, e.Current.Value);
            Assert.True(e.MoveNext());
            Assert.Equal(2, e.Current.Key);
            Assert.Equal(2, e.Current.Value);
            Assert.False(e.MoveNext());

            e = OrderedKeyValuePairs<int, int>
                .Merge(Comparer<int>.Default, DuplicateHandling.LastValueWins, x, y)
                .GetEnumerator();

            Assert.True(e.MoveNext());
            Assert.Equal(1, e.Current.Key);
            Assert.Equal(2, e.Current.Value);
            Assert.True(e.MoveNext());
            Assert.Equal(2, e.Current.Key);
            Assert.Equal(2, e.Current.Value);
            Assert.False(e.MoveNext());
        }

        [Fact]
        public void TestOrderedKeyValuePairsOverloads()
        {
            IEnumerable<KeyValuePair<int, int>> e = new KeyValuePair<int, int>[0];
            OrderedKeyValuePairs<int, int> ordered;

            ordered = new OrderedKeyValuePairs<int, int>(e);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(ReferenceEquals(Comparer<int>.Default,
                ((KeyValueComparer<int, int>) ordered.Comparer).Comparer));
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(0x10000, ordered.InMemoryLimit);
            Assert.Equal(null, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(((KeyValueComparer<int, int>) ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(0x10000, ordered.InMemoryLimit);
            Assert.Equal(null, ordered.Serializer);

            KeyValueSerializer<int, int> ser = new KeyValueSerializer<int, int>(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32);
            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, ser);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(((KeyValueComparer<int, int>) ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(0x10000, ordered.InMemoryLimit);
            Assert.Equal(ser, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e, ser, 42);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(((KeyValueComparer<int, int>) ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(42, ordered.InMemoryLimit);
            Assert.Equal(ser, ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e,
                PrimitiveSerializer.Int32, PrimitiveSerializer.Int32);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(((KeyValueComparer<int, int>) ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(0x10000, ordered.InMemoryLimit);
            Assert.NotNull(ordered.Serializer);

            ordered = new OrderedKeyValuePairs<int, int>(new ReverseOrder<int>(Comparer<int>.Default), e,
                PrimitiveSerializer.Int32, PrimitiveSerializer.Int32, 42);
            Assert.True(ordered.Comparer is KeyValueComparer<int, int>);
            Assert.True(((KeyValueComparer<int, int>) ordered.Comparer).Comparer is ReverseOrder<int>);
            Assert.Equal(DuplicateHandling.None, ordered.DuplicateHandling);
            Assert.Equal(42, ordered.InMemoryLimit);
            Assert.NotNull(ordered.Serializer);
        }

        [Fact]
        public void TestUnorderedAssertion()
        {
            Assert.Throws<InvalidDataException>(() =>
            {
                new List<int>(OrderedEnumeration<int>.WithDuplicateHandling(new[] {2, 1}, Comparer<int>.Default,
                    DuplicateHandling.RaisesException));
            });
        }
    }
}