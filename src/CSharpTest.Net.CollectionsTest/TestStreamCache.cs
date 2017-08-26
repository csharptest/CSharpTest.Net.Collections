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
using System.IO;
using System.Threading;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using NUnit.Framework;

namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestStreamCache
    {
        [Test]
        public void TestCacheLimit()
        {
            bool finished = false;
            using (SharedMemoryStream shared = new SharedMemoryStream())
            using (StreamCache cache = new StreamCache(shared, 1))
            {
                ManualResetEvent ready = new ManualResetEvent(false);
                ManualResetEvent release = new ManualResetEvent(false);
                Thread t = new Thread(
                    delegate()
                    {
                        using (Stream stream = cache.Open(FileAccess.ReadWrite))
                        {
                            stream.Write(new byte[50], 0, 50);
                            ready.Set();
                            release.WaitOne();
                            stream.Write(new byte[50], 0, 50);
                            GC.KeepAlive(stream);
                            finished = true;
                        }
                    }
                );
                t.IsBackground = true;
                t.Start();

                Assert.IsTrue(ready.WaitOne());
                Assert.AreEqual(50, shared.Length);

                new Thread(
                    delegate()
                    {
                        Thread.Sleep(10);
                        release.Set();
                    }
                ).Start();

                Assert.IsFalse(finished);
                using (Stream stream = cache.Open())
                {
                    Assert.IsTrue(finished);
                    Assert.IsTrue(release.WaitOne(0));
                    Assert.AreEqual(100, stream.Read(new byte[100], 0, 100));
                }
                Assert.IsTrue(t.Join(1000));
            }
        }

        [Test]
        public void TestCacheRecoverAbandondMutex()
        {
            Stream stream = null;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                Thread t = new Thread(delegate() { stream = cache.Open(FileAccess.Read); });
                t.Start();
                t.Join(); //The exit of this thread releases the stream...

                using (Stream another = cache.Open())
                {
                    Assert.AreEqual(0, another.Position);
                    // Another thread can then objtain the same underlying stream... we can demonstrate
                    // this by the fact that the Position property affects both streams.
                    stream.Position = 100;
                    Assert.AreEqual(100, another.Position);
                }
            }
        }

        [Test]
        public void TestCacheRecoverAbandondStream()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                if (true)
                {
                    stream = cache.Open();
                    stream.Write(new byte[100], 25, 55);
                    stream = null; //simulated "accidental" object abandonment... i.e. someone does something stupid.
                }

                GC.Collect(0, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();

                Thread t = new Thread(
                    delegate()
                    {
                        using (stream = cache.Open(FileAccess.Read))
                        {
                            Assert.AreEqual(new byte[55], IOStream.ReadAllBytes(stream));
                        }
                    }
                );
                t.IsBackground = true;
                t.Start();
                Assert.IsTrue(t.Join(1000));
            }
        }

        [Test]
        public void TestFileStreamCache()
        {
            Stream stream;
            using (TempFile tempFile = new TempFile())
            {
                using (StreamCache cache = new StreamCache(new FileStreamFactory(tempFile.TempPath, FileMode.Open)))
                {
                    using (stream = cache.Open())
                    {
                        stream.SetLength(100);
                        stream.WriteByte(1);
                    }
                }

                Assert.AreEqual(100, tempFile.Length);
                using (stream = tempFile.Open())
                {
                    Assert.AreEqual(1, stream.ReadByte());
                }
            }
        }

        [Test]
        public void TestFileStreamFactoryCreateABunch()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                    FileShare.ReadWrite);
                using (DisposingList<Stream> open = new DisposingList<Stream>())
                {
                    for (int i = 0; i < 50; i++)
                        open.Add(factory.Create());
                }
            }
        }

        [Test]
        public void TestFileStreamFactoryReturnsFileStream()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.ReadWrite,
                    FileShare.None, 1024, FileOptions.Asynchronous);
                Assert.AreEqual(tempFile.TempPath, factory.FileName);
                using (FileStream s = (FileStream) factory.Create())
                {
                    Assert.IsTrue(s.CanRead && s.CanWrite && s.IsAsync);
                }
            }
        }

        [Test]
        //[ExpectedException(typeof(ArgumentException))]
        public void TestFileStreamInvalidAccessWithMode()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.Read);
                Assert.AreEqual(tempFile.TempPath, factory.FileName);
                factory.Create().Dispose();
                Assert.Fail();
            }
        }

        [Test]
        //[ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void TestFileStreamInvalidBufferSize()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.ReadWrite,
                    FileShare.None, 0);
                Assert.AreEqual(tempFile.TempPath, factory.FileName);
                factory.Create().Dispose();
                Assert.Fail();
            }
        }

        [Test]
        //[ExpectedException(typeof(IOException))]
        public void TestFileStreamInvalidFileShare()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                    FileShare.None);
                using (factory.Create())
                {
                    factory.Create().Dispose();
                    Assert.Fail();
                }
            }
        }

        [Test]
        public void TestStreamCacheDisposes()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                stream = cache.Open();
                Assert.IsTrue(stream.CanRead && stream.CanWrite);
            }

            Assert.IsFalse(stream.CanRead || stream.CanWrite);
            try
            {
                stream.ReadByte();
                Assert.Fail();
            } /* why InvalidOperation?, the underlying stream was disposed, not the stream itself */
            catch (InvalidOperationException)
            {
            }

            stream.Dispose();
            try
            {
                stream.WriteByte(1);
                Assert.Fail();
            } /* Now it's been disposed */
            catch (ObjectDisposedException)
            {
            }
        }

        [Test]
        public void TestStreamCanI()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                using (stream = cache.Open())
                {
                    Assert.IsTrue(stream.CanRead);
                    Assert.IsTrue(stream.CanWrite);
                    Assert.IsTrue(stream.CanSeek);
                }
                using (stream = ((IFactory<Stream>) cache).Create())
                {
                    Assert.IsTrue(stream.CanRead);
                    Assert.IsTrue(stream.CanWrite);
                    Assert.IsTrue(stream.CanSeek);
                }
                using (stream = cache.Open(FileAccess.Read))
                {
                    Assert.IsTrue(stream.CanRead);
                    Assert.IsFalse(stream.CanWrite);
                    Assert.IsTrue(stream.CanSeek);
                }
                using (stream = cache.Open(FileAccess.Write))
                {
                    Assert.IsFalse(stream.CanRead);
                    Assert.IsTrue(stream.CanWrite);
                    Assert.IsTrue(stream.CanSeek);
                }
            }
        }

        [Test]
        public void TestStreamReadWrite()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                using (stream = cache.Open())
                {
                    stream.Write(new byte[100], 25, 55);
                }
                using (stream = cache.Open(FileAccess.Read))
                {
                    Assert.AreEqual(new byte[55], IOStream.ReadAllBytes(stream));
                }
            }
        }
    }
}