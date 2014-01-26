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
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class TestSimpleDictionary : TestDictionary<BPlusTree<int, string>, TestSimpleDictionary.BTreeFactory, int, string>
    {
        public class BTreeFactory : IFactory<BPlusTree<int, string>>
        {
            public BPlusTree<int, string> Create()
            {
                BPlusTree<int, string> tree = new BPlusTree<int, string>(
                    new BPlusTree<int, string>.Options(PrimitiveSerializer.Instance, PrimitiveSerializer.Instance)
                        {
                            BTreeOrder = 4,
                            LockingFactory = new IgnoreLockFactory()
                        }
                    );
                tree.EnableCount();
                return tree;
            }
        }

        protected override KeyValuePair<int, string>[] GetSample()
        {
            return new[] 
                       {
                           new KeyValuePair<int,string>(1, "1"),
                           new KeyValuePair<int,string>(3, "3"),
                           new KeyValuePair<int,string>(5, "5"),
                           new KeyValuePair<int,string>(7, "7"),
                           new KeyValuePair<int,string>(9, "9"),
                           new KeyValuePair<int,string>(11, "11"),
                           new KeyValuePair<int,string>(13, "13"),
                       };
        }
    }
}