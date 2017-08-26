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
using CSharpTest.Net.Collections.Test.IO;
using CSharpTest.Net.Interfaces;
using CSharpTest.Net.IO;
using Xunit;

namespace CSharpTest.Net.Collections.Test
{

    public class TestStreamCache
    {
        [Fact]
        public void TestCacheLimit()
        {
            bool finished = false;
            using (SharedMemoryStream shared = new SharedMemoryStream())
            using (StreamCache cache = new StreamCache(shared, 1))
            {
                ManualResetEvent ready = new ManualResetEvent(false);
                ManualResetEvent release = new ManualResetEvent(false);
                Thread t = new Thread(
                    delegate ()
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

                Assert.True(ready.WaitOne());
                Assert.Equal(50, shared.Length);

                new Thread(
                    delegate ()
                    {
                        Thread.Sleep(10);
                        release.Set();
                    }
                ).Start();

                Assert.False(finished);
                using (Stream stream = cache.Open())
                {
                    Assert.True(finished);
                    Assert.True(release.WaitOne(0));
                    Assert.Equal(100, stream.Read(new byte[100], 0, 100));
                }
                Assert.True(t.Join(1000));
            }
        }

        [Fact]
        public void TestCacheRecoverAbandondMutex()
        {
            Stream stream = null;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                Thread t = new Thread(delegate () { stream = cache.Open(FileAccess.Read); });
                t.Start();
                t.Join(); //The exit of this thread releases the stream...

                using (Stream another = cache.Open())
                {
                    Assert.Equal(0, another.Position);
                    // Another thread can then objtain the same underlying stream... we can demonstrate
                    // this by the fact that the Position property affects both streams.
                    stream.Position = 100;
                    Assert.Equal(100, another.Position);
                }
            }
        }

        [Fact]
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
                    delegate ()
                    {
                        using (stream = cache.Open(FileAccess.Read))
                        {
                            Assert.Equal(new byte[55], IOStream.ReadAllBytes(stream));
                        }
                    }
                );
                t.IsBackground = true;
                t.Start();
                Assert.True(t.Join(1000));
            }
        }

        [Fact]
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

                Assert.Equal(100, tempFile.Length);
                using (stream = tempFile.Open())
                {
                    Assert.Equal(1, stream.ReadByte());
                }
            }
        }

        [Fact]
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

        [Fact]
        public void TestFileStreamFactoryReturnsFileStream()
        {
            using (TempFile tempFile = new TempFile())
            {
                FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.ReadWrite,
                    FileShare.None, 1024, FileOptions.Asynchronous);
                Assert.Equal(tempFile.TempPath, factory.FileName);
                using (FileStream s = (FileStream)factory.Create())
                {
                    Assert.True(s.CanRead && s.CanWrite && s.IsAsync);
                }
            }
        }

        [Fact]
        public void TestFileStreamInvalidAccessWithMode()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                using (TempFile tempFile = new TempFile())
                {
                    FileStreamFactory factory =
                        new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.Read);
                    Assert.Equal(tempFile.TempPath, factory.FileName);
                    factory.Create().Dispose();
                    Assert.True(false);
                }
            });
        }

        [Fact]
        public void TestFileStreamInvalidBufferSize()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (TempFile tempFile = new TempFile())
                {
                    FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.Create, FileAccess.ReadWrite,
                        FileShare.None, 0);
                    Assert.Equal(tempFile.TempPath, factory.FileName);
                    factory.Create().Dispose();
                    Assert.True(false);
                }
            });
        }

        [Fact]
        public void TestFileStreamInvalidFileShare()
        {
            Assert.Throws<IOException>(() =>
            {
                using (TempFile tempFile = new TempFile())
                {
                    FileStreamFactory factory = new FileStreamFactory(tempFile.TempPath, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                        FileShare.None);
                    using (factory.Create())
                    {
                        factory.Create().Dispose();
                        Assert.True(false);
                    }
                }
            });
        }

        [Fact]
        public void TestStreamCacheDisposes()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                stream = cache.Open();
                Assert.True(stream.CanRead && stream.CanWrite);
            }

            Assert.False(stream.CanRead || stream.CanWrite);
            try
            {
                stream.ReadByte();
                Assert.True(false);
            } /* why InvalidOperation?, the underlying stream was disposed, not the stream itself */
            catch (InvalidOperationException)
            {
            }

            stream.Dispose();
            try
            {
                stream.WriteByte(1);
                Assert.True(false);
            } /* Now it's been disposed */
            catch (ObjectDisposedException)
            {
            }
        }

        [Fact]
        public void TestStreamCanI()
        {
            Stream stream;
            using (StreamCache cache = new StreamCache(new SharedMemoryStream(), 1))
            {
                using (stream = cache.Open())
                {
                    Assert.True(stream.CanRead);
                    Assert.True(stream.CanWrite);
                    Assert.True(stream.CanSeek);
                }
                using (stream = ((IFactory<Stream>)cache).Create())
                {
                    Assert.True(stream.CanRead);
                    Assert.True(stream.CanWrite);
                    Assert.True(stream.CanSeek);
                }
                using (stream = cache.Open(FileAccess.Read))
                {
                    Assert.True(stream.CanRead);
                    Assert.False(stream.CanWrite);
                    Assert.True(stream.CanSeek);
                }
                using (stream = cache.Open(FileAccess.Write))
                {
                    Assert.False(stream.CanRead);
                    Assert.True(stream.CanWrite);
                    Assert.True(stream.CanSeek);
                }
            }
        }

        [Fact]
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
                    Assert.Equal(new byte[55], IOStream.ReadAllBytes(stream));
                }
            }
        }
    }
}