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
using System.Collections.Generic;
using System.Diagnostics;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;
using CSharpTest.Net.Synchronization;
using NUnit.Framework;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class SequenceTests
    {
        protected virtual BPlusTree<int, string>.Options Options
        {
            get
            {
                BPlusTree<int, string>.Options options = new BPlusTree<int, string>.Options(new PrimitiveSerializer(), new PrimitiveSerializer())
                {
                    BTreeOrder = 4,
                };
                options.CalcBTreeOrder(4, 4);
                options.LockingFactory = new IgnoreLockFactory();
                return options;
            }
        }

        [Test]
        public void TestForwardInsertTo100()
        {
            SequencedTest(0, 1, 100, "Forward");
        }

        [Test]
        public void TestForwardInsertTo1000()
        {
            SequencedTest(0, 1, 1000, "Forward");
        }

        [Test]
        public void TestForwardInsertTo2000()
        {
            SequencedTest(0, 1, 2000, "Forward");
        }

        [Test]
        public void TestForwardInsertTo3000()
        {
            SequencedTest(0, 1, 3000, "Forward");
        }

        [Test]
        public void TestForwardInsertTo4000()
        {
            SequencedTest(0, 1, 4000, "Forward");
        }

        [Test]
        public void TestForwardInsertTo5000()
        {
            SequencedTest(0, 1, 5000, "Forward");
        }

        [Test]
        public void TestReverseInsertTo100()
        {
            SequencedTest(100, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo1000()
        {
            SequencedTest(1000, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo2000()
        {
            SequencedTest(2000, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo3000()
        {
            SequencedTest(3000, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo4000()
        {
            SequencedTest(4000, -1, 0, "Reverse");
        }

        [Test]
        public void TestReverseInsertTo5000()
        {
            SequencedTest(5000, -1, 0, "Reverse");
        }

        void SequencedTest(int start, int incr, int stop, string name)
        {
            int count = Math.Abs(start - stop)/Math.Abs(incr);
            const string myTestValue1 = "T1", myTestValue2 = "t2";
            string test;

            using (BPlusTree<int, string> data = new BPlusTree<int, string>(Options))
            {
                Stopwatch time = new Stopwatch();
                time.Start();
                //large order-forward
                for (int i = start; i != stop; i += incr)
                    if (!data.TryAdd(i, myTestValue1)) throw new ApplicationException();

                Trace.TraceInformation("{0} insert  {1} in {2}", name, count, time.ElapsedMilliseconds);
                time.Reset();
                time.Start();

                for (int i = start; i != stop; i += incr)
                    if (!data.TryGetValue(i, out test) || test != myTestValue1) throw new ApplicationException();

                Trace.TraceInformation("{0} seek    {1} in {2}", name, count, time.ElapsedMilliseconds);
                time.Reset();
                time.Start();

                for (int i = start; i != stop; i += incr)
                    if (!data.TryUpdate(i, myTestValue2)) throw new ApplicationException();

                Trace.TraceInformation("{0} modify  {1} in {2}", name, count, time.ElapsedMilliseconds);
                time.Reset();
                time.Start();

                for (int i = start; i != stop; i += incr)
                    if (!data.TryGetValue(i, out test) || test != myTestValue2) throw new ApplicationException();

                Trace.TraceInformation("{0} seek#2  {1} in {2}", name, count, time.ElapsedMilliseconds);
                time.Reset();
                time.Start();

                int tmpCount = 0;
                foreach (KeyValuePair<int, string> tmp in data)
                    if (tmp.Value != myTestValue2) throw new ApplicationException();
                    else tmpCount++;
                if (tmpCount != count) throw new ApplicationException();

                Trace.TraceInformation("{0} foreach {1} in {2}", name, count, time.ElapsedMilliseconds);
                time.Reset();
                time.Start();

                for (int i = start; i != stop; i += incr)
                    if (!data.Remove(i)) throw new ApplicationException();

                Trace.TraceInformation("{0} delete  {1} in {2}", name, count, time.ElapsedMilliseconds);

                for (int i = start; i != stop; i += incr)
                    if (data.TryGetValue(i, out test)) throw new ApplicationException();
            }
        }
    }
}
