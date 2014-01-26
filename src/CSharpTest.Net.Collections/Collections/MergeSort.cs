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
using System.Collections.Generic;

namespace CSharpTest.Net.Collections
{
    /// <summary>
    /// Provides a stable array sort based on merge-sort using O(n) additional memory.  As a release build,
    /// this routine will operate faster than Array.Sort when using a custom (non-default) comparison.  It
    /// also has the advantange of being stable, that is it preserves the order of elements that compare as
    /// being of equal value.
    /// </summary>
    public static class MergeSort
    {
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        public static void Sort<T>(T[] list)
        {
            T[] clone = (T[])list.Clone();
            Sort(clone, list, 0, list.Length, Comparer<T>.Default);
        }
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        /// <remarks> This overload also yields the working copy of the array which is unsorted. </remarks>
        internal static void Sort<T>(T[] list, out T[] working, int offset, int count, IComparer<T> compare)
        {
            working = (T[])list.Clone();
            Sort(working, list, offset, count, compare);
        }
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        public static void Sort<T>(T[] list, IComparer<T> compare)
        {
            T[] clone = (T[])list.Clone();
            Sort(clone, list, 0, list.Length, compare);
        }
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        public static void Sort<T>(T[] list, int offset, int count, IComparer<T> compare)
        {
            T[] clone = (T[])list.Clone();
            Sort(clone, list, offset, count, compare);
        }
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        public static void Sort<T>(T[] list, Comparison<T> compare)
        {
            T[] clone = (T[])list.Clone();
            Sort(clone, list, 0, list.Length, new ByComparison<T>(compare));
        }
        /// <summary> Sorts the contents of the array using a stable merge-sort with O(n) additional memory </summary>
        public static void Sort<T>(T[] list, int offset, int count, Comparison<T> compare)
        {
            T[] clone = (T[])list.Clone();
            Sort(clone, list, offset, count, new ByComparison<T>(compare));
        }

        class ByComparison<T> : IComparer<T>
        {
            private readonly Comparison<T> _compare;

            public ByComparison(Comparison<T> compare)
            {
                _compare = compare;
            }

            public int Compare(T x, T y)
            {
                return _compare(x, y);
            }
        }

        private static void Sort<T>(T[] src, T[] dest, int offset, int count, IComparer<T> compare)
        {
            if (count > 2)
            {
                int half = count >> 1;
                int c1 = half, c2 = count - half;
                Sort(dest, src, offset, c1, compare);
                Sort(dest, src, offset + half, c2, compare);

                c1 += offset;
                c2 += offset + half;
                for (int rix = offset, stop = offset + count, ix1 = offset, ix2 = offset + half; rix < stop; rix++)
                {
                    dest[rix] =
                        (ix1 < c1 && (ix2 == c2 || compare.Compare(src[ix1], src[ix2]) <= 0))
                        ? src[ix1++]
                        : src[ix2++];
                }
            }
            else if (count == 1)
                dest[offset] = src[offset];
            else if (count == 2)
            {
                if (compare.Compare(src[offset], src[offset + 1]) <= 0)
                {
                    dest[offset] = src[offset];
                    dest[offset + 1] = src[offset + 1];
                }
                else
                {
                    dest[offset] = src[offset + 1];
                    dest[offset + 1] = src[offset];
                }
            }
        }
    }
}
