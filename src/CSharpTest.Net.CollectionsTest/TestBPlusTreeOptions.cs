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

using System;
using CSharpTest.Net.Collections;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class TestBPlusTreeOptions
    {
        [Test]
        public void TestCloneWithCallLockV1()
        {
            BPlusTree<int, int>.Options options = new BPlusTree<int, int>.Options(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32)
            {
                CreateFile = CreatePolicy.IfNeeded,
                BTreeOrder = 4
            };
            BPlusTree<int, int>.Options copy = options.Clone();

            Assert.IsFalse(ReferenceEquals(options, copy));
            Assert.IsTrue(ReferenceEquals(options.CallLevelLock, copy.CallLevelLock));

            //If we get/set the lock prior to clone we will have the same lock instance.
            options.CallLevelLock = new SimpleReadWriteLocking();
            copy = options.Clone();

            Assert.IsFalse(ReferenceEquals(options, copy));
            Assert.IsTrue(ReferenceEquals(options.CallLevelLock, copy.CallLevelLock));
        }

        [Test]
        public void TestCloneWithCallLockV2()
        {
            BPlusTree<int, int>.OptionsV2 options = new BPlusTree<int, int>.OptionsV2(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32)
            {
                CreateFile = CreatePolicy.IfNeeded,
                BTreeOrder = 4
            };
            BPlusTree<int, int>.OptionsV2 copy = options.Clone();

            Assert.IsFalse(ReferenceEquals(options, copy));
            Assert.IsFalse(ReferenceEquals(options.CallLevelLock, copy.CallLevelLock));

            //If we get/set the lock prior to clone we will have the same lock instance.
            options.CallLevelLock = new SimpleReadWriteLocking();
            copy = options.Clone();

            Assert.IsFalse(ReferenceEquals(options, copy));
            Assert.IsTrue(ReferenceEquals(options.CallLevelLock, copy.CallLevelLock));
        }

        [Test]
        public void TestICloneable()
        {
            var opt = new BPlusTree<int, int>.Options(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32)
            {
                CreateFile = CreatePolicy.IfNeeded,
                BTreeOrder = 4
            };
            BPlusTree<int, int>.Options options = (BPlusTree<int, int>.Options) opt.Clone();

            Assert.AreEqual(CreatePolicy.IfNeeded, options.CreateFile);
            Assert.AreEqual(4, options.MaximumChildNodes);
            Assert.AreEqual(4, options.MaximumValueNodes);
        }

        [Test]
        public void TestReadOnly()
        {
            using (TempFile file = new TempFile())
            {
                BPlusTree<int, int>.Options opt = new BPlusTree<int, int>.Options(PrimitiveSerializer.Int32, PrimitiveSerializer.Int32)
                {
                    CreateFile = CreatePolicy.Always,
                    FileName = file.TempPath
                };
                using (BPlusTree<int, int> tree = new BPlusTree<int, int>(opt))
                {
                    tree.Add(1, 2);
                    tree.Add(3, 4);
                    tree.Add(5, 6);
                }

                opt.CreateFile = CreatePolicy.Never;
                opt.ReadOnly = true;
                using (BPlusTree<int, int> tree = new BPlusTree<int, int>(opt))
                {
                    Assert.AreEqual(tree[1], 2);
                    Assert.AreEqual(tree[3], 4);
                    Assert.AreEqual(tree[5], 6);

                    try
                    {
                        tree[1] = 0;
                        Assert.Fail();
                    }
                    catch (InvalidOperationException)
                    {
                    }

                    try
                    {
                        tree.Remove(1);
                        Assert.Fail();
                    }
                    catch (InvalidOperationException)
                    {
                    }
                }
            }
        }
    }
}