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
using System.Collections.Generic;

namespace CSharpTest.Net.BPlusTree.Test.SampleTypes
{
    class KeyInfoComparer : IComparer<KeyInfo>
    {
        public int Compare(KeyInfo x, KeyInfo y)
        {
            int result = x.UID.CompareTo(y.UID);
            if (result == 0)
                result = x.Version.CompareTo(y.Version);
            return result;
        }
    }
}