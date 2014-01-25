#region Copyright 2010-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Collections.Generic;
using NUnit.Framework;
using CSharpTest.Net.IO;
using CSharpTest.Net.Collections;

#pragma warning disable 1591
namespace CSharpTest.Net.Library.Test
{
    [TestFixture]
    public partial class TestReplaceFile
    {
        #region TestFixture SetUp/TearDown
        [TestFixtureSetUp]
        public virtual void Setup()
        {
        }

        [TestFixtureTearDown]
        public virtual void Teardown()
        {
        }
        #endregion

        [Test]
        public void TestIsFullPath()
        {
            string tmpname = Guid.NewGuid().ToString();
            Assert.IsFalse(File.Exists(tmpname));
            using (ReplaceFile f = new ReplaceFile(tmpname))
            {
                Assert.IsTrue(Path.IsPathRooted(f.TargetFile));
                Assert.AreEqual(Environment.CurrentDirectory, Path.GetDirectoryName(f.TargetFile));
            }
            Assert.IsFalse(File.Exists(tmpname));
        }

        [Test]
        public void TestReplaceFiles()
        {
            string testdata = Guid.NewGuid().ToString();
            string tmpPath;

            TempFile replace = new TempFile();
            using (ReplaceFile temp = new ReplaceFile(replace.TempPath))
            {
                Assert.AreEqual(temp.TargetFile, replace.TempPath);
                tmpPath = temp.TempPath;
                Assert.IsTrue(temp.Exists);
                temp.WriteAllText(testdata);
                //missing commit:
                //temp.Commit();
            }
            Assert.AreEqual(0, replace.Length);
            Assert.IsFalse(File.Exists(tmpPath));

            string backupfile = Path.ChangeExtension(replace.TempPath, ".bak");
            File.Delete(Path.ChangeExtension(replace.TempPath, ".bak"));
            Assert.IsFalse(File.Exists(backupfile));
            replace.WriteAllText("backup");

            //now for real
            using (ReplaceFile temp = new ReplaceFile(replace.TempPath, ".bak"))
            {
                tmpPath = temp.TempPath;
                Assert.IsTrue(temp.Exists);
                temp.WriteAllText(testdata);
                temp.Commit();
            }
            Assert.IsFalse(File.Exists(tmpPath));
            Assert.AreEqual(testdata, replace.ReadAllText());
            
            Assert.IsTrue(File.Exists(backupfile));
            using (TempFile fbackup = TempFile.Attach(backupfile))
                Assert.AreEqual("backup", fbackup.ReadAllText());
        }

        [Test]
        public void TestReplaceRollback()
        {
            string testdata = Guid.NewGuid().ToString();

            using (TempFile replace = new TempFile())
            using (ReplaceFile temp = new ReplaceFile(replace.TempPath))
            {
                temp.WriteAllText(testdata);
                Assert.IsTrue(temp.Exists);
                temp.Rollback();
                Assert.IsFalse(temp.Exists);
            }
        }

        [Test]
        public void TestDerivedFileName()
        {
            string path;
            using(DisposingList l = new DisposingList())
            {
                TempFile ftarget = new TempFile();
                ftarget.Delete();

                l.Add(ftarget);
                TempFile fbigone = TempFile.Attach(String.Format("{0}.~{1:x4}", ftarget.TempPath, 0x10008 - 25));
                fbigone.Create().Dispose();
                Assert.IsTrue(fbigone.Exists);
                l.Add(fbigone);
                path = ftarget.TempPath;

                string tmpName;
                Dictionary<string, object> names = new Dictionary<string,object>(StringComparer.OrdinalIgnoreCase);

                for( int i=0; i < 25; i++ )
                {
                    Stream s = ReplaceFile.CreateDerivedFile(ftarget.TempPath, out tmpName);
                    l.Add(TempFile.Attach(tmpName));
                    l.Add(s);
                    names.Add(tmpName, null);
                }
                
                fbigone.Delete();
                Assert.AreEqual(25, Directory.GetFiles(Path.GetDirectoryName(path), Path.GetFileName(path) + "*").Length);
            }
            Assert.AreEqual(0, Directory.GetFiles(Path.GetDirectoryName(path), Path.GetFileName(path) + "*").Length);
        }

        [Test, ExpectedException(typeof(ArgumentException))]
        public void TestBadBackupExtension()
        {
            string fname = Guid.NewGuid().ToString();
            try
            {
                using (new ReplaceFile(fname, "Not an extension"))
                { }
            }
            finally
            {
                Assert.IsFalse(File.Exists(fname));
            }
        }
    }
}
