#region Copyright 2012-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using NUnit.Framework;
using CSharpTest.Net.Collections;
using CSharpTest.Net.IO;
using CSharpTest.Net.Serialization;
using System.IO;

namespace CSharpTest.Net.BPlusTree.Test
{
    [TestFixture]
    public class TestTransactionLog
    {
        TransactionLogOptions<int, string> Options(TempFile file)
        {
            return new TransactionLogOptions<int, string>(file.TempPath,
                                                          PrimitiveSerializer.Int32,
                                                          PrimitiveSerializer.String)
                                                          {
                                                              FileOptions = FileOptions.WriteThrough,
                                                          };
        }
        [Test]
        public void TestTransactionLogOptions()
        {
            using (TempFile temp = new TempFile())
            {
                temp.Delete();
                var opt = new TransactionLogOptions<int, string>(temp.TempPath,
                                                              PrimitiveSerializer.Int32,
                                                              PrimitiveSerializer.String);
                //FileName
                Assert.AreEqual(temp.TempPath, opt.FileName);
                //Key/Value serializers
                Assert.IsTrue(ReferenceEquals(opt.KeySerializer, PrimitiveSerializer.Int32));
                Assert.IsTrue(ReferenceEquals(opt.ValueSerializer, PrimitiveSerializer.String));
                //FileOptions
                Assert.AreEqual(FileOptions.WriteThrough, opt.FileOptions);
                Assert.AreEqual(FileOptions.WriteThrough | FileOptions.Asynchronous, opt.FileOptions |= FileOptions.Asynchronous);
                //Read Only
                Assert.AreEqual(false, opt.ReadOnly);
                Assert.AreEqual(true, opt.ReadOnly = true);
                //File Buffer
                Assert.AreEqual(8, opt.FileBuffer);
                Assert.AreEqual(0x40000, opt.FileBuffer = 0x40000);
                //Clone
                Assert.IsFalse(ReferenceEquals(opt, opt.Clone()));

                using(TransactionLog<int, string> log = new TransactionLog<int,string>(opt))
                    Assert.AreEqual(0, log.Size);
            }
        }
        [Test]
        public void TestAddOperation()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);

                Assert.AreEqual(1, test.Count);
                Assert.IsTrue(test.ContainsKey(1));
                Assert.AreEqual("test", test[1]);
            }
        }
        [Test]
        public void TestUpdateOperation()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.UpdateValue(ref token, 1, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                test.Add(1, null);
                log.ReplayLog(test);

                Assert.AreEqual(1, test.Count);
                Assert.IsTrue(test.ContainsKey(1));
                Assert.AreEqual("test", test[1]);
            }
        }
        [Test]
        public void TestRemoveOperation()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.RemoveValue(ref token, 1);
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                test.Add(1, null);
                log.ReplayLog(test);

                Assert.AreEqual(0, test.Count);
            }
        }
        [Test]
        public void TestMultipleWriteAndReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.AddValue(ref token, 2, "test");
                log.AddValue(ref token, 3, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);
                Assert.AreEqual(3, test.Count);
                for (int i = 1; i <= 3; i++)
                    Assert.AreEqual("test", test[i]);
            }
        }
        [Test]
        public void TestMultipleTransAndReplay()
        {

            using (TempFile tmp = new TempFile())
            {
                var opts = Options(tmp);
                using (var log = new TransactionLog<int, string>(opts))
                {
                    var token = log.BeginTransaction();
                    log.AddValue(ref token, 1, "test");
                    log.CommitTransaction(ref token);
                    token = log.BeginTransaction();
                    log.AddValue(ref token, 2, "test");
                    log.CommitTransaction(ref token);
                    token = log.BeginTransaction();
                    log.AddValue(ref token, 3, "test");
                    log.CommitTransaction(ref token);
                    log.Close();
                }

                using (var log = new TransactionLog<int, string>(opts))
                {
                    var test = new Dictionary<int, string>();
                    log.ReplayLog(test);
                    Assert.AreEqual(3, test.Count);
                    for (int i = 1; i <= 3; i++)
                        Assert.AreEqual("test", test[i]);
                }
            }
        }
        [Test, Explicit]
        public void TestBenchmarkWriteSpeed()
        {
            //Write 2,147,483,776 bytes in: 00:02:09.7934237 (in chunks of 128 bytes)
            //Write 4,295,032,832 bytes in: 00:00:18.4990581 (in chunks of 65536 bytes)
            //Logged 2,398,000,000 bytes in: 00:00:36.7621027


            string newpath = Path.Combine(@"C:\Temp\LogTest\", Guid.NewGuid() + ".tmp");
            using (TempFile tmp = TempFile.Attach(newpath))
            {
                byte[] bytes;
                DateTime start;
                //bytes = new byte[128];
                //new Random().NextBytes(bytes);

                //start = DateTime.UtcNow;
                //using (var io = new FileStream(tmp.TempPath, FileMode.Append, FileAccess.Write, FileShare.Read, 8))
                //{
                //    for (int i = 0; i <= 16777216; i++)
                //        io.Write(bytes, 0, 128);
                //}
                //Console.WriteLine("Write {0:n0} bytes in: {1}", tmp.Length, DateTime.UtcNow - start);
                //tmp.Delete();

                var options = new TransactionLogOptions<Guid, byte[]>(
                    tmp.TempPath, PrimitiveSerializer.Guid, PrimitiveSerializer.Bytes) 
                    {
                        FileBuffer = ushort.MaxValue,
                        FileOptions = FileOptions.None | FileOptions.SequentialScan,
                    };

                Guid[] ids = new Guid[1000000];
                for (int i = 0; i < ids.Length; i++)
                    ids[i] = Guid.NewGuid();

                bytes = new byte[100];
                new Random().NextBytes(bytes);

                start = DateTime.UtcNow;

                using (var log = new TransactionLog<Guid, byte[]>(options))
                {
                    foreach(Guid id in ids)
                    {
                        var token = log.BeginTransaction();
                        for (int i = 0; i < 20; i++)
                            log.AddValue(ref token, id, bytes);
                        log.CommitTransaction(ref token);
                    }
                }

                Console.WriteLine("Logged {0:n0} bytes in: {1}", tmp.Length, DateTime.UtcNow - start);
            }
        }
        [Test]
        public void TestLargeWriteAndReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                string testdata = new String('*', 512);
                var token = log.BeginTransaction();
                for (int i = 0; i < 20; i++)
                    log.AddValue(ref token, i, testdata);
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);
                Assert.AreEqual(20, test.Count);
                for (int i = 0; i < 20; i++)
                    Assert.AreEqual(testdata, test[i]);
            }
        }
        [Test]
        public void TestProgressiveReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                long position = 0;
                log.ReplayLog(test, ref position);
                Assert.AreEqual(1, test.Count);
                test.Clear();

                log.ReplayLog(test, ref position);
                Assert.AreEqual(0, test.Count);

                token = log.BeginTransaction();
                log.AddValue(ref token, 2, "test");
                log.CommitTransaction(ref token);

                log.ReplayLog(test, ref position);
                Assert.AreEqual(1, test.Count);
                Assert.IsTrue(test.ContainsKey(2));
                Assert.AreEqual("test", test[2]);
            }
        }
        [Test]
        public void TestSingleRollbackAndReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.RollbackTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);
                Assert.AreEqual(0, test.Count);
            }
        }
        [Test]
        public void TestPositionAndReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.CommitTransaction(ref token);

                long size = long.MaxValue;
                log.ReplayLog(null, ref size);

                token = log.BeginTransaction();
                log.AddValue(ref token, 2, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test, ref size);
                Assert.AreEqual(1, test.Count);
                Assert.AreEqual("test", test[2]);
            }
        }
        [Test]
        public void TestCommitEmptyAndReplay()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.CommitTransaction(ref token);//commit empty

                token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.CommitTransaction(ref token);//add value

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);
                Assert.AreEqual(1, test.Count);
            }
        }
        [Test]
        public void TestReplayEmpty()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var test = new Dictionary<int, string>();
                log.ReplayLog(test);
                Assert.AreEqual(0, test.Count);
            }
        }
        [Test]
        public void TestTruncateLog()
        {
            using (TempFile tmp = new TempFile())
            using (var log = new TransactionLog<int, string>(Options(tmp)))
            {
                var token = log.BeginTransaction();
                log.AddValue(ref token, 1, "test");
                log.CommitTransaction(ref token);

                var test = new Dictionary<int, string>();
                log.ReplayLog(test);

                Assert.AreEqual(1, test.Count);
                Assert.IsTrue(test.ContainsKey(1));
                Assert.AreEqual("test", test[1]);

                log.TruncateLog();
                test.Clear();
                log.ReplayLog(test);

                Assert.AreEqual(0, test.Count);
            }
        }
        [Test]
        public void TestLogWithJunkAppended()
        {
            string testdata = Guid.NewGuid().ToString();
            using (TempFile tmp = new TempFile())
            {
                using (var log = new TransactionLog<int, string>(Options(tmp)))
                {
                    var token = log.BeginTransaction();
                    for (int i = 0; i < 20; i++)
                        log.AddValue(ref token, i, testdata);
                    log.CommitTransaction(ref token);
                }
                using (var log = new TransactionLog<int, string>(Options(tmp)))
                {
                    var test = new Dictionary<int, string>();
                    log.ReplayLog(test);
                    Assert.AreEqual(20, test.Count);
                    for (int i = 0; i < 20; i++)
                        Assert.AreEqual(testdata, test[i]);
                }

                long flength;
                byte[] junk = new byte[512];
                new Random().NextBytes(junk);
                using(Stream io = File.OpenWrite(tmp.TempPath))
                {
                    flength = io.Seek(0, SeekOrigin.End);
                    io.Write(junk, 0, junk.Length);
                }

                using (var log = new TransactionLog<int, string>(Options(tmp)))
                {
                    var test = new Dictionary<int, string>();
                    log.ReplayLog(test);
                    Assert.AreEqual(20, test.Count);
                    for (int i = 0; i < 20; i++)
                        Assert.AreEqual(testdata, test[i]);
                }
                // The file will be truncated to a valid position
                Assert.AreEqual(flength, new FileInfo(tmp.TempPath).Length);
            }
        }

        [Test]
        public void TestLogCorruption()
        {
            using (TempFile tmp = new TempFile())
            {
                using (var log = new TransactionLog<int, string>(Options(tmp)))
                {
                    var token = log.BeginTransaction();
                    log.AddValue(ref token, 1, Guid.NewGuid().ToString());
                    log.CommitTransaction(ref token);
                    var test = new Dictionary<int, string>();
                    long offset = 0;
                    log.ReplayLog(test, ref offset);
                    Assert.AreEqual(1, test.Count);
                }
                byte[] bytes = tmp.ReadAllBytes();

                var TestVariants =
                    new Converter<KeyValuePair<int, byte[]>, byte[]>[]
                        {
                            kv => { kv.Value[kv.Key] ^= 0xff; return kv.Value; },
                            kv => { kv.Value[kv.Key] = 0xff; return kv.Value; },
                            kv => { byte[] b = kv.Value; Array.Resize(ref b, kv.Key); return b; },
                        };

                for (int corruptionIx = 0; corruptionIx < bytes.Length; corruptionIx++)
                {
                    foreach (var testcase in TestVariants)
                    {
                        byte[] corrupt = testcase(new KeyValuePair<int, byte[]>(corruptionIx, (byte[]) bytes.Clone()));
                        tmp.WriteAllBytes(corrupt);

                        using (var log = new TransactionLog<int, string>(Options(tmp)))
                        {
                            var test = new Dictionary<int, string>();
                            log.ReplayLog(test);
                            Assert.AreEqual(0, test.Count);
                        }
                        Assert.IsFalse(File.Exists(tmp.TempPath));
                    }
                }
            }
        }
    }
}
