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
using System.Threading;
using CSharpTest.Net.Collections.Test.IO;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{

    public class TestFragmentedFile
    {
        private readonly Random _random = new Random();

        public byte[] MakeBytes(int size)
        {
            byte[] bytes = new byte[size];
            _random.NextBytes(bytes);
            return bytes;
        }

        private void TestCrud(FragmentedFile ff, int blockCount, int blockSize)
        {
            Dictionary<long, byte[]> data = new Dictionary<long, byte[]>();

            //Create:
            for (int i = 0; i < blockCount; i++)
                data.Add(ff.Create(), null);
            //Write:
            foreach (long id in new List<long>(data.Keys))
                using (Stream io = ff.Open(id, FileAccess.Write))
                {
                    io.Write(data[id] = MakeBytes(blockSize), 0, blockSize);
                }
            //Read:
            foreach (KeyValuePair<long, byte[]> kv in data)
                using (Stream io = ff.Open(kv.Key, FileAccess.Read))
                {
                    Assert.Equal(kv.Value, IOStream.ReadAllBytes(io));
                }
            //Enumerate:
            Dictionary<long, byte[]> copy = new Dictionary<long, byte[]>(data);
            foreach (KeyValuePair<long, Stream> fragment in ff.ForeachBlock(true, true, null))
            {
                Assert.Equal(copy[fragment.Key], IOStream.ReadAllBytes(fragment.Value));
                Assert.True(copy.Remove(fragment.Key));
            }
            //Update:
            foreach (long id in new List<long>(data.Keys))
            {
                Assert.Equal(data[id], IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));

                using (Stream io = ff.Open(id, FileAccess.Write))
                {
                    io.Write(data[id] = MakeBytes(blockSize * 2), 0, blockSize * 2);
                }
                Assert.Equal(data[id], IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));

                using (Stream io = ff.Open(id, FileAccess.Write))
                {
                    io.Write(data[id] = MakeBytes(blockSize / 2), 0, blockSize / 2);
                }
                Assert.Equal(data[id], IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));
            }
            //Delete:
            foreach (long id in new List<long>(data.Keys))
                ff.Delete(id);
            //Empty?
            foreach (KeyValuePair<long, Stream> fragment in ff.ForeachBlock(true, true, null))
                Assert.True(false);
        }

        public void AssertThrows<TException>(ThreadStart proc) where TException : Exception
        {
            try
            {
                proc();
            }
            catch (Exception error)
            {
                Assert.True(error is TException, $"Unexpected Exception type {error.GetType()} thrown from {proc.Target}");
                return;
            }

            Assert.True(false, $"Expected Exception of type {typeof(TException)} from {proc.Target}");
        }

        [Fact]
        public void TestClear()
        {
            Dictionary<long, byte[]> data = new Dictionary<long, byte[]>();
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault))
            {
                //Create:
                for (int i = 0; i < 256; i++)
                    data.Add(ff.Create(), null);
                //Enumerate:
                int count = 0;
                foreach (KeyValuePair<long, Stream> fragment in ff.ForeachBlock(true, true, null))
                    count++;
                Assert.Equal(256, count);
                ff.Clear();
                //Empty?
                foreach (KeyValuePair<long, Stream> fragment in ff.ForeachBlock(true, true, null))
                    Assert.True(false);
            }
        }

        [Fact]
        public void TestCloseAndReopen()
        {
            using (TempFile file = new TempFile())
            {
                Guid guid = Guid.NewGuid();
                long id;

                using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512))
                {
                    using (Stream io = ff.Create(out id))
                    {
                        PrimitiveSerializer.Guid.WriteTo(guid, io);
                    }
                    Assert.Equal(id, ff.FirstIdentity);
                }
                using (FragmentedFile ff = new FragmentedFile(file.TempPath, 512))
                {
                    Assert.Equal(id, ff.FirstIdentity);
                    using (Stream io = ff.Open(id, FileAccess.Read))
                    {
                        Assert.Equal(guid, PrimitiveSerializer.Guid.ReadFrom(io));
                    }
                }
                using (FragmentedFile ff = new FragmentedFile(file.TempPath, 512, 10, 10, FileAccess.Read, FileShare.None,
                    FileOptions.None))
                {
                    Assert.Equal(id, ff.FirstIdentity);
                    using (Stream io = ff.Open(id, FileAccess.Read))
                    {
                        Assert.Equal(guid, PrimitiveSerializer.Guid.ReadFrom(io));
                    }

                    AssertThrows<InvalidOperationException>(delegate { ff.Open(id, FileAccess.Write).Dispose(); });
                }
            }
        }

        [Fact]
        public void TestCreateAndRead()
        {
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512))
            {
                Guid guid = Guid.NewGuid();
                long id;
                using (Stream io = ff.Create(out id))
                {
                    PrimitiveSerializer.Guid.WriteTo(guid, io);
                }

                Assert.Equal(id, ff.FirstIdentity);
                using (Stream io = ff.Open(id, FileAccess.Read))
                {
                    Assert.Equal(guid, PrimitiveSerializer.Guid.ReadFrom(io));
                }
            }
        }

        [Fact]
        public void TestMultiBlocks()
        {
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault))
            {
                TestCrud(ff, 50, 2000);
            }
        }

        [Fact]
        public void TestOptionsNoBuffering()
        {
            // If you have issues with this test, your hardware may not support it, or your sector size is larger than 4096
            long id;
            byte[] bytes;
            Dictionary<long, byte[]> data = new Dictionary<long, byte[]>();
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 4096, 10, 10, FragmentedFile.OptionsNoBuffering))
            {
                for (int i = 0; i < 256; i++)
                {
                    using (Stream io = ff.Create(out id))
                    {
                        io.Write(bytes = MakeBytes(256), 0, bytes.Length);
                    }
                    data.Add(id, bytes);
                }
                foreach (KeyValuePair<long, byte[]> kv in data)
                    using (Stream io = ff.Open(kv.Key, FileAccess.Read))
                    {
                        Assert.Equal(kv.Value, IOStream.ReadAllBytes(io));
                    }
            }
        }

        [Fact]
        public void TestOptionsWriteThrough()
        {
            long id;
            byte[] bytes;
            Dictionary<long, byte[]> data = new Dictionary<long, byte[]>();
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 10, 10, FragmentedFile.OptionsWriteThrough))
            {
                for (int i = 0; i < 256; i++)
                {
                    using (Stream io = ff.Create(out id))
                    {
                        io.Write(bytes = MakeBytes(256), 0, bytes.Length);
                    }
                    data.Add(id, bytes);
                }
                foreach (KeyValuePair<long, byte[]> kv in data)
                    using (Stream io = ff.Open(kv.Key, FileAccess.Read))
                    {
                        Assert.Equal(kv.Value, IOStream.ReadAllBytes(io));
                    }
            }
        }

        [Fact]
        public void TestReaderStream()
        {
            using (SharedMemoryStream shared = new SharedMemoryStream())
            using (FragmentedFile ff = FragmentedFile.CreateNew(shared, 512, 100, 2))
            {
                long id;
                using (Stream write = ff.Create(out id))
                {
                    PrimitiveSerializer.Int64.WriteTo(id, write);
                }

                using (Stream read = ff.Open(id, FileAccess.Read))
                {
                    Assert.True(read.CanRead);
                    Assert.False(read.CanWrite);
                    Assert.False(read.CanSeek);
                    Assert.Equal(id, PrimitiveSerializer.Int64.ReadFrom(read));
                    read.Flush(); //no-op

                    AssertThrows<NotSupportedException>(delegate { read.Position = 0; });
                    AssertThrows<NotSupportedException>(delegate { GC.KeepAlive(read.Position); });
                    AssertThrows<NotSupportedException>(delegate { GC.KeepAlive(read.Length); });
                    AssertThrows<NotSupportedException>(delegate { read.SetLength(1); });
                    AssertThrows<NotSupportedException>(delegate { read.Seek(1, SeekOrigin.Begin); });
                    AssertThrows<NotSupportedException>(delegate { read.WriteByte(1); });
                }
            }
        }

        [Fact]
        public void TestRecoverBlocks()
        {
            long idFirst, idSecond, idThird;
            using (TempFile file = new TempFile())
            {
                if (true)
                {
                    FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault);
                    idFirst = ff.Create();
                    idSecond = ff.Create();
                    idThird = ff.Create();
                    ff.Delete(idFirst);

                    //Dangerous, only used for testing the case when ff was never disposed, nor GC'd
                    GC.SuppressFinalize(ff);
                    ff = null;
                }

                GC.Collect(0, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();

                using (FragmentedFile f2 = new FragmentedFile(file.TempPath, 512))
                {
                    Assert.True(f2.Create() < idSecond);
                    Assert.True(f2.Create() > idThird);
                }
            }
        }

        [Fact]
        public void TestRollbackCreate()
        {
            SharedMemoryStream shared = new SharedMemoryStream();
            FragmentedFile.CreateNew(shared, 512, 100, 2).Dispose();

            using (FragmentedFile ff = new FragmentedFile(shared, 512, 100, 2))
            {
                long id;
                byte[] bytes = MakeBytes(255);
                using (Stream write = ff.Create(out id))
                using (ITransactable trans = (ITransactable)write)
                {
                    write.Write(bytes, 0, bytes.Length);
                    trans.Commit();
                    Assert.Equal(bytes, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));
                    trans.Rollback();
                }

                AssertThrows<InvalidDataException>(delegate { ff.Open(id, FileAccess.Read); });
            }
        }

        [Fact]
        public void TestSingleBlockCrud()
        {
            using (TempFile file = new TempFile())
            {
                using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault))
                {
                    TestCrud(ff, 256, 256);
                }
                using (FragmentedFile ff = new FragmentedFile(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault))
                {
                    TestCrud(ff, 256, 256);
                }
            }
        }

        [Fact]
        public void TestTransactBlock()
        {
            SharedMemoryStream shared = new SharedMemoryStream();
            FragmentedFile.CreateNew(shared, 512, 100, 2).Dispose();

            using (FragmentedFile ff = new FragmentedFile(shared, 512, 100, 2))
            {
                long id;
                byte[] orig = MakeBytes(255);
                using (Stream write = ff.Create(out id))
                {
                    write.Write(orig, 0, orig.Length);
                }

                Assert.Equal(orig, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));

                byte[] change = MakeBytes(800);
                using (Stream write = ff.Open(id, FileAccess.Write))
                using (ITransactable trans = (ITransactable)write) //the Fragmented File Streams are ITransactable
                {
                    write.Write(change, 0, change.Length);
                    Assert.Equal(orig, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));

                    trans.Commit(); //commit changes so that readers can read
                    Assert.Equal(change, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));

                    trans.Rollback(); //rollback even after commit to 'undo' the changes
                    Assert.Equal(orig, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));
                } //once disposed you can no longer rollback, if rollback has not been called commit is implied.

                Assert.Equal(orig, IOStream.ReadAllBytes(ff.Open(id, FileAccess.Read)));
            }
        }

        [Fact]
        public void TestTransactFail()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                SharedMemoryStream shared = new SharedMemoryStream();
                FragmentedFile.CreateNew(shared, 512, 100, 2).Dispose();

                using (FragmentedFile ff = new FragmentedFile(shared, 512, 100, 2))
                {
                    long id;
                    byte[] bytes = MakeBytes(255);
                    using (Stream write = ff.Create(out id))
                    using (ITransactable trans = (ITransactable)write)
                    {
                        write.Write(bytes, 0, bytes.Length);
                        ff.Dispose();
                        trans.Commit();
                    }
                }
            });
        }

        [Fact]
        public void TestTransactWriteAfterCommit()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                SharedMemoryStream shared = new SharedMemoryStream();
                FragmentedFile.CreateNew(shared, 512, 100, 2).Dispose();

                using (FragmentedFile ff = new FragmentedFile(shared, 512, 100, 2))
                {
                    long id;
                    byte[] bytes = MakeBytes(255);
                    using (Stream write = ff.Create(out id))
                    using (ITransactable trans = (ITransactable)write)
                    {
                        write.Write(bytes, 0, bytes.Length);
                        trans.Commit();
                        write.Write(bytes, 0, bytes.Length);
                    }
                }
            });
        }

        [Fact]
        public void TestTwoBlocks()
        {
            using (TempFile file = new TempFile())
            using (FragmentedFile ff = FragmentedFile.CreateNew(file.TempPath, 512, 100, 2, FragmentedFile.OptionsDefault))
            {
                TestCrud(ff, 100, 768);
            }
        }

        [Fact]
        public void TestWriterStream()
        {
            using (SharedMemoryStream shared = new SharedMemoryStream())
            using (FragmentedFile ff = FragmentedFile.CreateNew(shared, 512, 100, 2))
            {
                long id = ff.Create();
                using (Stream write = ff.Open(id, FileAccess.Write))
                {
                    Assert.False(write.CanRead);
                    Assert.True(write.CanWrite);
                    Assert.False(write.CanSeek);
                    PrimitiveSerializer.Int64.WriteTo(id, write);
                    write.Flush(); //no-op

                    AssertThrows<NotSupportedException>(delegate { write.Position = 0; });
                    AssertThrows<NotSupportedException>(delegate { GC.KeepAlive(write.Position); });
                    AssertThrows<NotSupportedException>(delegate { GC.KeepAlive(write.Length); });
                    AssertThrows<NotSupportedException>(delegate { write.SetLength(1); });
                    AssertThrows<NotSupportedException>(delegate { write.Seek(1, SeekOrigin.Begin); });
                    AssertThrows<NotSupportedException>(delegate { write.ReadByte(); });
                }
            }
        }
    }
}