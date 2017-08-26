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
using NUnit.Framework;

#pragma warning disable 1591
namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public class TestTempFiles
    {
        [TestFixtureSetUp]
        public virtual void Setup()
        {
        }

        [TestFixtureTearDown]
        public virtual void Teardown()
        {
        }

        [Test]
        public void TestAttach()
        {
            string path = Path.GetTempFileName();
            Assert.IsTrue(File.Exists(path));
            File.WriteAllText(path, "Test");

            TempFile filea = TempFile.Attach(path);
            Assert.AreEqual(path, filea.TempPath);
            Assert.AreEqual("Test", File.ReadAllText(filea.TempPath));

            filea.Dispose();
            Assert.IsFalse(File.Exists(path));

            using (TempFile filec = new TempFile(path))
            {
                Assert.IsFalse(File.Exists(path));
            }
        }


        [Test]
        [ExpectedException(typeof(ArgumentException))]
        public void TestBadPathOnAttach()
        {
            TempFile f = TempFile.Attach("@~+_(%!&($_~!(&*+%_~&^%^|||&&&\\\\ THIS IS AN INVALID FILE NAME.*");
            f.Dispose();
        }

        [Test]
        public void TestCopyTo()
        {
            TempFile filea = new TempFile();

            File.WriteAllText(filea.TempPath, "Test");
            Assert.AreEqual("Test", File.ReadAllText(filea.TempPath));

            TempFile fileb = new TempFile();
            Assert.AreNotEqual(filea.TempPath, fileb.TempPath);

            filea.CopyTo(fileb.TempPath, true);
            Assert.AreEqual("Test", File.ReadAllText(fileb.TempPath));

            File.Delete(filea.TempPath);
            Assert.IsFalse(File.Exists(filea.TempPath));

            fileb.CopyTo(filea.TempPath);
            Assert.AreEqual("Test", File.ReadAllText(filea.TempPath));

            filea.Dispose();
            fileb.Dispose();
        }

        [Test]
        public void TestDetach()
        {
            TempFile filea = new TempFile();
            string path = filea.TempPath;
            Assert.IsTrue(File.Exists(path));

            Assert.AreEqual(path, filea.Detatch());
            Assert.IsTrue(File.Exists(path));
            filea.Dispose();
            Assert.IsTrue(File.Exists(path));

            File.Delete(path);
        }

        [Test]
        public void TestDispose()
        {
            TempFile file = new TempFile();
            Assert.IsTrue(File.Exists(file.TempPath));
            Assert.IsTrue(file.Exists);

            file.Dispose();
            Assert.IsFalse(file.Exists);
        }

        [Test]
        public void TestFileCreateAccess()
        {
            TempFile file = new TempFile();

            Stream c = file.Create();
            Assert.IsTrue(c.CanWrite);
            Assert.IsFalse(c.CanRead);

            Stream r = file.Read();
            Assert.IsFalse(r.CanWrite);
            Assert.IsTrue(r.CanRead);

            c.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Test]
        public void TestFileDelete()
        {
            TempFile file = new TempFile();

            Assert.IsTrue(File.Exists(file.TempPath));
            TempFile.Delete(file.TempPath);

            Assert.IsFalse(File.Exists(file.TempPath));
            file.Dispose();

            //ignres bad paths:
            TempFile.Delete("@~+_(%!&($_~!(&*+%_~&^%^|||&&&\\\\ THIS IS AN INVALID FILE NAME.*");
        }

        [Test]
        public void TestFileOpenAccess()
        {
            TempFile file = new TempFile();

            Stream o = file.Open();
            Assert.IsTrue(o.CanWrite);
            Assert.IsTrue(o.CanRead);

            Stream r = file.Read();
            Assert.IsFalse(r.CanWrite);
            Assert.IsTrue(r.CanRead);

            o.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Test]
        public void TestFileReadAccess()
        {
            TempFile file = new TempFile();

            Stream r = file.Read();
            Assert.IsFalse(r.CanWrite);
            Assert.IsTrue(r.CanRead);

            Stream o = file.Open();
            Assert.IsTrue(o.CanWrite);
            Assert.IsTrue(o.CanRead);

            o.Dispose();
            r.Dispose();
            file.Dispose();
        }

        [Test]
        public void TestFinalizer()
        {
            string filename;
            try
            {
                TempFile file = new TempFile();
                filename = file.TempPath;
                Assert.IsTrue(File.Exists(file.TempPath));

                IDisposable flock = file.Open();
                file.Dispose();

                Assert.IsTrue(File.Exists(file.TempPath)); //dua, it's still open

                flock.Dispose();
                file = null;
            }
            finally
            {
            }

            //wait for GC to collect tempfile
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.IsFalse(File.Exists(filename));
        }

        [Test]
        public void TestFinalizerReschedule()
        {
            IDisposable flock;
            string filename;
            try
            {
                TempFile file = new TempFile();
                filename = file.TempPath;
                Assert.IsTrue(File.Exists(file.TempPath));

                flock = file.Open();
                file.Dispose();

                Assert.IsTrue(File.Exists(file.TempPath)); //dua, it's still open
                file = null;
            }
            finally
            {
            }

            //wait for GC to collect tempfile
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.IsTrue(File.Exists(filename));

            //now the finalizer should have fire, as proven by TestFinalizer(), see if the
            //rescheduled object will finalize...

            flock.Dispose();
            GC.Collect(0, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();

            Assert.IsFalse(File.Exists(filename));
        }

        [Test]
        public void TestFromCopyWithExtension()
        {
            using (TempFile a = TempFile.FromExtension(".test"))
            {
                a.WriteAllText("a");
                Assert.AreEqual("a", a.ReadAllText());
                Assert.AreEqual(".test", Path.GetExtension(a.TempPath));

                using (TempFile b = TempFile.FromCopy(a.TempPath))
                {
                    Assert.AreEqual("a", b.ReadAllText());
                    Assert.AreEqual(".test", Path.GetExtension(b.TempPath));
                }
            }
        }

        [Test]
        public void TestInfo()
        {
            TempFile f = new TempFile();
            f.Length = 5;
            Assert.AreEqual(f.Length, f.Info.Length);
            f.Dispose();

            Assert.AreEqual(0, f.Length);
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestInfoOnDisposed()
        {
            TempFile f = new TempFile();
            f.Dispose();
            f.Info.OpenText();
        }

        [Test]
        public void TestLength()
        {
            using (TempFile f = new TempFile())
            {
                Assert.AreEqual(0, f.Length);
                f.Delete();
                Assert.AreEqual(0, f.Length);
                f.Length = 255;
                Assert.AreEqual(255, f.Length);
                f.Length = 0;
                Assert.AreEqual(0, f.Length);
                Assert.IsTrue(f.Exists);
            }
        }

        [Test]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void TestPathOnDisposed()
        {
            TempFile f = new TempFile();
            f.Dispose();
            Assert.Fail(f.TempPath);
        }

        [Test]
        public void TestReadWrite()
        {
            string test = "Hello World!\u1255";
            TempFile file = new TempFile();
            File.WriteAllBytes(file.TempPath, Encoding.UTF8.GetBytes(test));

            Assert.AreEqual(Encoding.UTF8.GetBytes(test), file.ReadAllBytes());
            Assert.AreEqual(test, file.ReadAllText());

            file.Delete();
            Assert.IsFalse(File.Exists(file.TempPath));
            Assert.IsFalse(file.Exists);
            file.WriteAllBytes(Encoding.UTF8.GetBytes(test));
            Assert.AreEqual(test, file.ReadAllText());

            file.Delete();
            Assert.IsFalse(File.Exists(file.TempPath));
            Assert.IsFalse(file.Exists);
            file.WriteAllText(test);
            Assert.AreEqual(test, file.ReadAllText());
        }
    }
}