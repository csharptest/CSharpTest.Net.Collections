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
using System.IO;
using System.Text;
using System.Threading;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{
    
    public class TestTransactedCompoundFile
    {
        private readonly Random rand = new Random();

        private byte[] RandomBytes(int size)
        {
            byte[] bytes = new byte[size];
            rand.NextBytes(bytes);
            return bytes;
        }

        private void CompareBytes(byte[] original, int offset, int len, byte[] value)
        {
            Assert.Equal(len, value.Length);
            Assert.Equal(
                Convert.ToBase64String(original, offset, len),
                Convert.ToBase64String(value, 0, value.Length)
            );
        }

        private T RandomPick<T>(ICollection<T> items)
        {
            int ix = rand.Next(items.Count);
            foreach (T item in items)
                if (ix-- == 0) return item;
            throw new Exception();
        }

        private void TestWriteWithOptions(TransactedCompoundFile.Options options)
        {
            byte[] sample = new byte[1024];
            new Random().NextBytes(sample);
            List<uint> handles = new List<uint>();
            using (TransactedCompoundFile file = new TransactedCompoundFile(options))
            {
                for (int i = 0; i < 1000; i++)
                {
                    uint hid = file.Create();
                    handles.Add(hid);
                    file.Write(hid, sample, i, sample.Length - i);
                    CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(hid)));
                    if (i == 500)
                        file.Commit();
                }
            }
            options.CreateNew = false;
            using (TransactedCompoundFile file = new TransactedCompoundFile(options))
            {
                for (int i = 0; i < 1000; i++)
                    if (i <= 500 || options.CommitOnWrite || options.CommitOnDispose)
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                    else
                        try
                        {
                            IOStream.ReadAllBytes(file.Read(handles[i]));
                            Assert.True(false);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                        }
            }
        }

        private static void ExersizeFile(ManualResetEvent stop, TransactedCompoundFile file)
        {
            const int LIMIT = 512 * 512 / 4 - 512 * 3;
            Random r = new Random();
            Dictionary<uint, byte[]> state = new Dictionary<uint, byte[]>();
            while (true)
            {
                while (state.Count < 1000)
                {
                    uint h = file.Create();
                    byte[] bytes = new byte[r.Next(5) == 0 ? r.Next(512) : r.Next(LIMIT)];
                    r.NextBytes(bytes);
                    file.Write(h, bytes, 0, bytes.Length);
                    state.Add(h, bytes);
                    if (stop.WaitOne(0))
                        return;
                }
                foreach (KeyValuePair<uint, byte[]> kv in state)
                {
                    Assert.Equal(0, BinaryComparer.Compare(kv.Value, IOStream.ReadAllBytes(file.Read(kv.Key))));
                    if (stop.WaitOne(0))
                        return;
                }
                List<KeyValuePair<uint, byte[]>> capture = new List<KeyValuePair<uint, byte[]>>(state);
                for (int i = 0; i < capture.Count; i += r.Next(4))
                {
                    uint h = capture[i].Key;
                    byte[] bytes = new byte[r.Next(512)];
                    r.NextBytes(bytes);
                    file.Write(h, bytes, 0, bytes.Length);
                    state[h] = bytes;
                    Assert.Equal(0, BinaryComparer.Compare(bytes, IOStream.ReadAllBytes(file.Read(h))));
                    if (stop.WaitOne(0))
                        return;
                }
                for (int i = 0; i < capture.Count; i += 1 + r.Next(4))
                {
                    uint h = capture[i].Key;
                    file.Delete(h);
                    state.Remove(h);
                    if (stop.WaitOne(0))
                        return;
                }
            }
        }

        [Fact]
        public void RandomTest()
        {
            Dictionary<uint, byte[]> store = new Dictionary<uint, byte[]>();
            using (TempFile temp = new TempFile())
            {
                uint id;
                byte[] bytes;
                using (TransactedCompoundFile test =
                    new TransactedCompoundFile(
                        new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512, CreateNew = true}))
                {
                    for (int i = 0; i < 10000; i++)
                        switch (i < 1000 ? 0 : rand.Next(3))
                        {
                            case 0:
                            {
                                id = test.Create();
                                bytes = RandomBytes(rand.Next(1000));
                                store.Add(id, bytes);
                                test.Write(id, bytes, 0, bytes.Length);
                                break;
                            }
                            case 1:
                            {
                                id = RandomPick(store.Keys);
                                bytes = store[id];
                                CompareBytes(bytes, 0, bytes.Length, IOStream.ReadAllBytes(test.Read(id)));
                                break;
                            }
                            case 2:
                            {
                                id = RandomPick(store.Keys);
                                Assert.True(store.Remove(id));
                                test.Delete(id);
                                break;
                            }
                        }

                    foreach (KeyValuePair<uint, byte[]> kv in store)
                        CompareBytes(kv.Value, 0, kv.Value.Length, IOStream.ReadAllBytes(test.Read(kv.Key)));
                }
            }
        }

        [Fact]
        public void SimpleTest()
        {
            uint handle;
            byte[] bytes = RandomBytes(2140);
            using (TempFile temp = new TempFile())
            {
                using (TransactedCompoundFile test =
                    new TransactedCompoundFile(
                        new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512, CreateNew = true}))
                {
                    Assert.Equal(TransactedCompoundFile.FirstIdentity, test.Create());
                    test.Write(TransactedCompoundFile.FirstIdentity, Encoding.UTF8.GetBytes("Roger was here."), 0, 15);
                    Assert.Equal("Roger was here.",
                        Encoding.UTF8.GetString(
                            IOStream.ReadAllBytes(test.Read(TransactedCompoundFile.FirstIdentity))));

                    for (int i = 0; i < 10; i++)
                    {
                        uint id = test.Create();
                        test.Write(id, bytes, i, 300);
                        if (i % 2 == 0)
                            test.Delete(id);
                    }

                    handle = test.Create();
                    test.Write(handle, bytes, 1000, bytes.Length - 1000);
                    CompareBytes(bytes, 1000, bytes.Length - 1000, IOStream.ReadAllBytes(test.Read(handle)));

                    test.Write(handle, bytes, 0, bytes.Length);
                    CompareBytes(bytes, 0, bytes.Length, IOStream.ReadAllBytes(test.Read(handle)));

                    test.Commit();
                }
                using (TransactedCompoundFile test =
                    new TransactedCompoundFile(
                        new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512, CreateNew = false}))
                {
                    Assert.Equal("Roger was here.",
                        Encoding.UTF8.GetString(
                            IOStream.ReadAllBytes(test.Read(TransactedCompoundFile.FirstIdentity))));
                    CompareBytes(bytes, 0, bytes.Length, IOStream.ReadAllBytes(test.Read(handle)));
                }
            }
        }

        [Fact]
        public void TestClear()
        {
            using (TempFile temp = new TempFile())
            {
                byte[] sample = new byte[1024];
                new Random().NextBytes(sample);
                List<uint> handles = new List<uint>();
                using (TransactedCompoundFile file = new TransactedCompoundFile(temp.TempPath))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, sample.Length - i);
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(hid)));
                        file.Commit();
                    }
                    file.Clear();
                    for (int i = 0; i < 100; i++)
                        try
                        {
                            IOStream.ReadAllBytes(file.Read(handles[i]));
                            Assert.True(false);
                        }
                        catch (ArgumentOutOfRangeException)
                        {
                        }
                }
            }
        }

        [Fact]
        public void TestCommit()
        {
            using (TempFile temp = new TempFile())
            using (TempFile temp2 = new TempFile())
            {
                byte[] sample = new byte[1024];
                new Random().NextBytes(sample);
                List<uint> handles = new List<uint>();
                using (TransactedCompoundFile file = new TransactedCompoundFile(
                    new TransactedCompoundFile.Options(temp.TempPath) {FileOptions = FileOptions.WriteThrough}
                ))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, sample.Length - i);
                    }
                    file.Commit();
                    File.Copy(temp.TempPath, temp2.TempPath, true);
                }
                using (TransactedCompoundFile file = new TransactedCompoundFile(temp2.TempPath))
                {
                    for (int i = 0; i < 1000; i++)
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                }
            }
        }

        [Fact]
        public void TestCommitOnDispose()
        {
            using (TempFile temp = new TempFile())
            {
                TestWriteWithOptions(
                    new TransactedCompoundFile.Options(temp.TempPath)
                    {
                        BlockSize = 512,
                        CommitOnWrite = false,
                        CommitOnDispose = true
                    }
                );
            }
        }

        [Fact]
        public void TestCommitOnWrite()
        {
            using (TempFile temp = new TempFile())
            {
                TestWriteWithOptions(
                    new TransactedCompoundFile.Options(temp.TempPath)
                    {
                        BlockSize = 2048,
                        CommitOnWrite = true,
                        CommitOnDispose = true,
                        CreateNew = true
                    }
                );
            }
        }

        [Fact]
        public void TestCommitWrite()
        {
            using (TempFile temp = new TempFile())
            {
                TestWriteWithOptions(
                    new TransactedCompoundFile.Options(temp.TempPath)
                    {
                        BlockSize = 512,
                        CommitOnWrite = true,
                        CommitOnDispose = false
                    }
                );
            }
        }

        [Fact]
        public void TestExceedWriteMax()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (TempFile temp = new TempFile())
                {
                    TransactedCompoundFile.Options options =
                        new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512};
                    byte[] sample = new byte[options.MaxWriteSize + 1];
                    new Random().NextBytes(sample);
                    using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                    {
                        file.Write(file.Create(), sample, 0, sample.Length);
                    }
                }
            });
        }

        [Fact]
        public void TestLargeWrite()
        {
            using (TempFile temp = new TempFile())
            {
                TransactedCompoundFile.Options options = new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512};
                byte[] sample = new byte[options.MaxWriteSize];
                new Random().NextBytes(sample);
                List<uint> handles = new List<uint>();
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (int i = 0; i < 2; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, sample.Length - i);
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                    }
                    file.Commit();
                }
                long size = temp.Info.Length;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (int i = 0; i < 2; i++)
                    {
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                        file.Delete(handles[i]);
                    }
                    file.Commit();
                    Assert.Equal(size, temp.Info.Length);
                    for (int i = 0; i < 252; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, 300);
                    }
                    file.Commit();
                    Assert.Equal(size, temp.Info.Length);

                    file.Create();
                    Assert.NotEqual(size, temp.Info.Length);
                }
            }
        }

        [Fact]
        public void TestNoCommit()
        {
            using (TempFile temp = new TempFile())
            {
                TestWriteWithOptions(
                    new TransactedCompoundFile.Options(temp.TempPath)
                    {
                        BlockSize = 512,
                        CommitOnWrite = false,
                        CommitOnDispose = false
                    }
                );
            }
        }

        [Fact]
        public void TestNormalWrite()
        {
            using (TempFile temp = new TempFile())
            {
                TestWriteWithOptions(
                    new TransactedCompoundFile.Options(temp.TempPath)
                    {
                        BlockSize = 512,
                        CommitOnWrite = false,
                        CommitOnDispose = true
                    }
                );
            }
        }

        [Fact]
        public void TestOptions()
        {
            using (TempFile temp = new TempFile())
            {
                TransactedCompoundFile.Options o = new TransactedCompoundFile.Options(temp.TempPath);
                Assert.Equal(temp.TempPath, o.FilePath);
                Assert.Equal(4096, o.BlockSize);
                Assert.Equal(8192, o.BlockSize = 8192);
                Assert.Equal(false, o.CreateNew);
                Assert.Equal(true, o.CreateNew = true);
                Assert.Equal(false, o.ReadOnly);
                Assert.Equal(true, o.ReadOnly = true);
                Assert.Equal(false, o.CommitOnDispose);
                Assert.Equal(true, o.CommitOnDispose = true);
                Assert.Equal(false, o.CommitOnWrite);
                Assert.Equal(true, o.CommitOnWrite = true);
                Assert.Equal(FileOptions.None, o.FileOptions);
                Assert.Equal(FileOptions.WriteThrough, o.FileOptions = FileOptions.WriteThrough);
                Assert.Equal(TransactedCompoundFile.LoadingRule.Default, o.LoadingRule);
                Assert.Equal(TransactedCompoundFile.LoadingRule.Primary,
                    o.LoadingRule = TransactedCompoundFile.LoadingRule.Primary);

                TransactedCompoundFile.Options copy = ((ICloneable<TransactedCompoundFile.Options>) o).Clone();
                Assert.Equal(FileOptions.WriteThrough, copy.FileOptions);
            }
        }

        [Fact]
        public void TestRevertChanges()
        {
            using (TempFile temp = new TempFile())
            {
                byte[] sample = new byte[1024];
                new Random().NextBytes(sample);
                List<uint> handles = new List<uint>();
                using (TransactedCompoundFile file = new TransactedCompoundFile(temp.TempPath))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, sample.Length - i);
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(hid)));
                        if (i == 500)
                            file.Commit();
                    }
                    file.Rollback();
                    for (int i = 0; i < 1000; i++)
                        if (i <= 500)
                            CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                        else
                            try
                            {
                                IOStream.ReadAllBytes(file.Read(handles[i]));
                                Assert.True(false);
                            }
                            catch (ArgumentOutOfRangeException)
                            {
                            }
                }
            }
        }

        [Fact]
        public void TestRevertWithChanges()
        {
            using (TempFile temp = new TempFile())
            {
                byte[] sample = new byte[1024];
                new Random().NextBytes(sample);
                List<uint> handles = new List<uint>();
                using (TransactedCompoundFile file = new TransactedCompoundFile(temp.TempPath))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        uint hid = file.Create();
                        handles.Add(hid);
                        file.Write(hid, sample, i, sample.Length - i);
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(hid)));
                    }
                    file.Commit();
                }
                using (TransactedCompoundFile file = new TransactedCompoundFile(temp.TempPath))
                {
                    byte[] dummy = new byte[1024];
                    for (int i = 0; i < 1000; i++)
                    {
                        file.Write(handles[i], dummy, 0, dummy.Length);
                        file.Delete(handles[i]);
                    }
                    file.Rollback();
                    for (int i = 0; i < 1000; i++)
                        CompareBytes(sample, i, sample.Length - i, IOStream.ReadAllBytes(file.Read(handles[i])));
                }
            }
        }

        [Fact]
        public void VerifyCommitAsTransaction()
        {
            using (TempFile temp1 = new TempFile())
            using (TempFile temp2 = new TempFile())
            {
                TransactedCompoundFile.Options options = new TransactedCompoundFile.Options(temp1.TempPath) {BlockSize = 512};

                const int count = 4;
                byte[] sample1 = new byte[options.MaxWriteSize / 3];
                byte[] sample2 = (byte[]) sample1.Clone();
                new Random().NextBytes(sample1);
                new Random().NextBytes(sample2);

                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        Assert.Equal(h, file.Create());
                    for (uint h = 1u; h < count; h++)
                        file.Write(h, sample1, 0, sample1.Length);
                    file.Commit(); //persist.
                    for (uint h = 1u; h < count; h++)
                        file.Write(h, sample2, 0, sample2.Length);

                    file.Commit(x => temp1.CopyTo(temp2.TempPath, true), 0);
                }

                options = new TransactedCompoundFile.Options(temp2.TempPath) {BlockSize = 512, CommitOnDispose = false};

                //Verify primary has sample2 data
                options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        CompareBytes(sample2, 0, sample2.Length, IOStream.ReadAllBytes(file.Read(h)));
                }
                //Verify secondary has sample1 data
                options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        CompareBytes(sample1, 0, sample1.Length, IOStream.ReadAllBytes(file.Read(h)));
                }
            }
        }

        [Fact]
        public void VerifyLoadRulesWithBothCorruptions()
        {
            using (TempFile temp = new TempFile())
            {
                TransactedCompoundFile.Options options = new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512};

                const int count = 4;
                byte[] sample = new byte[options.MaxWriteSize / 3];
                new Random().NextBytes(sample);

                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        Assert.Equal(h, file.Create());
                    for (uint h = 1u; h < count; h++)
                        file.Write(h, sample, 0, sample.Length);
                    file.Commit();
                }

                //Corrupts the primary of the first block, and secondary of the last block:
                using (Stream f = temp.Open())
                {
                    f.Write(sample, 0, 100);
                    f.Seek(-100, SeekOrigin.End);
                    f.Write(sample, 0, 100);
                }
                try
                {
                    options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                    new TransactedCompoundFile(options).Dispose();
                    Assert.True(false,"Should not load");
                }
                catch (InvalidDataException)
                {
                }
                try
                {
                    options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                    new TransactedCompoundFile(options).Dispose();
                    Assert.True(false,"Should not load");
                }
                catch (InvalidDataException)
                {
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Default;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        CompareBytes(sample, 0, sample.Length, IOStream.ReadAllBytes(file.Read(h)));
                    //Commit fixes corruption
                    file.Commit();
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                new TransactedCompoundFile(options).Dispose();
                options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                new TransactedCompoundFile(options).Dispose();
            }
        }

        [Fact]
        public void VerifyLoadRulesWithCorruptPrimary()
        {
            using (TempFile temp = new TempFile())
            {
                TransactedCompoundFile.Options options = new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512};

                const int count = 4;
                byte[] sample = new byte[options.MaxWriteSize / 3];
                new Random().NextBytes(sample);

                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        Assert.Equal(h, file.Create());
                    for (uint h = 1u; h < count; h++)
                        file.Write(h, sample, 0, sample.Length);
                    file.Commit();
                }

                //Corrupts the primary storage:
                using (Stream f = temp.Open())
                {
                    f.Write(sample, 0, 100);
                }
                try
                {
                    options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                    new TransactedCompoundFile(options).Dispose();
                    Assert.True(false,"Should not load");
                }
                catch (InvalidDataException)
                {
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        CompareBytes(sample, 0, sample.Length, IOStream.ReadAllBytes(file.Read(h)));
                    //Commit fixes corruption
                    file.Commit();
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                new TransactedCompoundFile(options).Dispose();
            }
        }

        [Fact]
        public void VerifyLoadRulesWithCorruptSecondary()
        {
            using (TempFile temp = new TempFile())
            {
                TransactedCompoundFile.Options options = new TransactedCompoundFile.Options(temp.TempPath) {BlockSize = 512};

                const int count = 4;
                byte[] sample = new byte[options.MaxWriteSize / 3];
                new Random().NextBytes(sample);

                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        Assert.Equal(h, file.Create());
                    for (uint h = 1u; h < count; h++)
                        file.Write(h, sample, 0, sample.Length);
                    file.Commit();
                }

                //Corrupts the secondary storage:
                using (Stream f = temp.Open())
                {
                    f.Seek(-100, SeekOrigin.End);
                    f.Write(sample, 0, 100);
                }
                try
                {
                    options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                    new TransactedCompoundFile(options).Dispose();
                    Assert.True(false,"Should not load");
                }
                catch (InvalidDataException)
                {
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Primary;
                using (TransactedCompoundFile file = new TransactedCompoundFile(options))
                {
                    for (uint h = 1u; h < count; h++)
                        CompareBytes(sample, 0, sample.Length, IOStream.ReadAllBytes(file.Read(h)));
                    //Commit fixes corruption
                    file.Commit();
                }

                options.LoadingRule = TransactedCompoundFile.LoadingRule.Secondary;
                new TransactedCompoundFile(options).Dispose();
            }
        }
    }
}