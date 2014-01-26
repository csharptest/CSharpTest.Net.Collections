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

namespace CSharpTest.Net.Collections
{
    internal class HashUtilities
    {
        private static readonly int[] PrimeNumbers =
            new[]
                {
                    17, 37, 67, 131, 257, 521, // ROK - Added smaller primes
                    1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
                    17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
                    187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687,
                    1395263, 1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
                };

        internal static int SelectPrimeNumber(int hashSize)
        {
            int offset = Array.BinarySearch(PrimeNumbers, hashSize);
            if (offset < 0)
                offset = ~offset;
            return PrimeNumbers[Math.Min(offset, PrimeNumbers.Length - 1)];
        }
    }
}
