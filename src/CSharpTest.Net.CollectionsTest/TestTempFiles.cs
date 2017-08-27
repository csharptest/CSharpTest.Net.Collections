#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Text;
using CSharpTest.Net.IO;
using Xunit;

#pragma warning disable 1591
namespace CSharpTest.Net.Collections.Test
{

    public class TestTempFiles
    {
        [Fact]
        public void TestAttach()
        {
            string path = Path.GetTempFileName();
            Assert.True(File.Exists(path));
            File.WriteAllText(path, "Test");

            TempFile filea = TempFile.Attach(path);
            Assert.Equal(path, filea.TempPath);
            Assert.Equal("Test", File.ReadAllText(filea.TempPath));

            filea.Dispose();
            Assert.False(File.Exists(path));

            using (TempFile filec = new TempFile(path))
            {
                Assert.False(File.Exists(path));
            }
        }


        [Fact]
        public void TestBadPathOnAttach()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                TempFile f = TempFile.Attach("@~+_(%!&($_~!(&*+%_~&^%^|||&&&\\\\ THIS IS AN INVALID FILE NAME.*");
                f.Dispose();
            });
        }

        [Fact]
        public void TestCopyTo()
        {
            TempFile filea = new TempFile();

            File.WriteAllText(filea.TempPath, "Test");
            Assert.Equal("Test", File.ReadAllText(filea.TempPath));

            TempFile fileb = new TempFile();
            Assert.NotEqual(filea.TempPath, fileb.TempPath);

            filea.CopyTo(fileb.TempPath, true);
            Assert.Equal("Test", File.ReadAllText(fileb.TempPath));

            File.Delete(filea.TempPath);
            Assert.False(File.Exists(filea.TempPath));

            fileb.CopyTo(filea.TempPath);
            Assert.Equal("Test", File.ReadAllText(filea.TempPath));

            filea.Dispose();
            fileb.Dispose();
        }

        [Fact]
        public void TestDetach()
        {
            TempFile filea = new TempFile();
            string path = filea.TempPath;
            Assert.True(File.Exists(path));

            Assert.Equal(path, filea.Detatch());
            Assert.True(File.Exists(path));
            filea.Dispose();
            Assert.True(File.Exists(path));

            File.Delete(path);
        }

        [Fact]
        public void TestDispose()
        {
            TempFile file = new TempFile();
            Assert.True(File.Exists(file.TempPath));
            Assert.True(file.Exists);

            file.Dispose();
            Assert.False(file.Exists);
        }

        [Fact]
        public void TestFileCreateAccess()
        {
            TempFile file = new TempFile();

            Stream c = file.Create();
            Assert.True(c.CanWrite);
            Assert.False(c.CanRead);

            Stream r = file.Read();
            Assert.False(r.CanWrite);
            Assert.True(r.CanRead);

            c.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Fact]
        public void TestFileDelete()
        {
            TempFile file = new TempFile();

            Assert.True(File.Exists(file.TempPath));
            TempFile.Delete(file.TempPath);

            Assert.False(File.Exists(file.TempPath));
            file.Dispose();

            //ignres bad paths:
            TempFile.Delete("@~+_(%!&($_~!(&*+%_~&^%^|||&&&\\\\ THIS IS AN INVALID FILE NAME.*");
        }

        [Fact]
        public void TestFileOpenAccess()
        {
            TempFile file = new TempFile();

            Stream o = file.Open();
            Assert.True(o.CanWrite);
            Assert.True(o.CanRead);

            Stream r = file.Read();
            Assert.False(r.CanWrite);
            Assert.True(r.CanRead);

            o.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Fact]
        public void TestFileReadAccess()
        {
            TempFile file = new TempFile();

            Stream r = file.Read();
            Assert.False(r.CanWrite);
            Assert.True(r.CanRead);

            Stream o = file.Open();
            Assert.True(o.CanWrite);
            Assert.True(o.CanRead);

            o.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Fact]
        public void TestFinalizer()
        {
            TempFile file = new TempFile();
            string filename = file.TempPath;
            Assert.True(File.Exists(file.TempPath));

            IDisposable flock = file.Open();
            file.Dispose();

            Assert.True(File.Exists(file.TempPath)); //dua, it's still open

            flock.Dispose();
            file = null;

            //wait for GC to collect tempfile
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.False(File.Exists(filename));
        }

        [Fact]
        public void TestFinalizerReschedule()
        {
            IDisposable flock;
            string filename;
            try
            {
                TempFile file = new TempFile();
                filename = file.TempPath;
                Assert.True(File.Exists(file.TempPath));

                flock = file.Open();
                file.Dispose();

                Assert.True(File.Exists(file.TempPath)); //dua, it's still open
                file = null;
            }
            finally
            {
            }

            //wait for GC to collect tempfile
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.True(File.Exists(filename));

            //now the finalizer should have fire, as proven by TestFinalizer(), see if the
            //rescheduled object will finalize...

            flock.Dispose();
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.False(File.Exists(filename));
        }

        [Fact]
        public void TestFromCopyWithExtension()
        {
            using (TempFile a = TempFile.FromExtension(".test"))
            {
                a.WriteAllText("a");
                Assert.Equal("a", a.ReadAllText());
                Assert.Equal(".test", Path.GetExtension(a.TempPath));

                using (TempFile b = TempFile.FromCopy(a.TempPath))
                {
                    Assert.Equal("a", b.ReadAllText());
                    Assert.Equal(".test", Path.GetExtension(b.TempPath));
                }
            }
        }

        [Fact]
        public void TestInfo()
        {
            TempFile f = new TempFile();
            f.Length = 5;
            Assert.Equal(f.Length, f.Info.Length);
            f.Dispose();

            Assert.Equal(0, f.Length);
        }

        [Fact]
        public void TestInfoOnDisposed()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                TempFile f = new TempFile();
                f.Dispose();
                f.Info.OpenText();
            });
        }

        [Fact]
        public void TestLength()
        {
            using (TempFile f = new TempFile())
            {
                Assert.Equal(0, f.Length);
                f.Delete();
                Assert.Equal(0, f.Length);
                f.Length = 255;
                Assert.Equal(255, f.Length);
                f.Length = 0;
                Assert.Equal(0, f.Length);
                Assert.True(f.Exists);
            }
        }

        [Fact]
        public void TestPathOnDisposed()
        {
            Assert.Throws<ObjectDisposedException>(() =>
            {
                TempFile f = new TempFile();
                f.Dispose();
                Assert.True(false, f.TempPath);
            });
        }

        [Fact]
        public void TestReadWrite()
        {
            string test = "Hello World!\u1255";
            TempFile file = new TempFile();
            File.WriteAllBytes(file.TempPath, Encoding.UTF8.GetBytes(test));

            Assert.Equal(Encoding.UTF8.GetBytes(test), file.ReadAllBytes());
            Assert.Equal(test, file.ReadAllText());

            file.Delete();
            Assert.False(File.Exists(file.TempPath));
            Assert.False(file.Exists);
            file.WriteAllBytes(Encoding.UTF8.GetBytes(test));
            Assert.Equal(test, file.ReadAllText());

            file.Delete();
            Assert.False(File.Exists(file.TempPath));
            Assert.False(file.Exists);
            file.WriteAllText(test);
            Assert.Equal(test, file.ReadAllText());
        }
    }
}