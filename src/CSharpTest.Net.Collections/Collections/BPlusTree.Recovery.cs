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
using CSharpTest.Net.IO;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        /// <summary>
        /// Directly enumerates the contents of BPlusTree from disk in read-only mode.
        /// </summary>
        /// <param name="options"> The options normally used to create the <see cref="BPlusTree{TKey, TValue}"/> instance </param>
        /// <returns> Yields the Key/Value pairs found in the file </returns>
        public static IEnumerable<KeyValuePair<TKey, TValue>> EnumerateFile(BPlusTreeOptions<TKey, TValue> options)
        {
            options = options.Clone();
            options.CreateFile = CreatePolicy.Never;
            options.ReadOnly = true;

            using (INodeStorage store = options.CreateStorage())
            {
                bool isnew;
                Node root;
                IStorageHandle hroot = store.OpenRoot(out isnew);
                if (isnew)
                    yield break;

                NodeSerializer nodeReader = new NodeSerializer(options, new NodeHandleSerializer(store));
                if (isnew || !store.TryGetNode(hroot, out root, nodeReader))
                    throw new InvalidDataException();

                Stack<KeyValuePair<Node, int>> todo = new Stack<KeyValuePair<Node, int>>();
                todo.Push(new KeyValuePair<Node, int>(root, 0));

                while (todo.Count > 0)
                {
                    KeyValuePair<Node, int> cur = todo.Pop();
                    if (cur.Value == cur.Key.Count)
                        continue;

                    todo.Push(new KeyValuePair<Node, int>(cur.Key, cur.Value + 1));

                    Node child;
                    if (!store.TryGetNode(cur.Key[cur.Value].ChildNode.StoreHandle, out child, nodeReader))
                        throw new InvalidDataException();

                    if (child.IsLeaf)
                    {
                        for (int ix = 0; ix < child.Count; ix++)
                            yield return child[ix].ToKeyValuePair();
                    }
                    else
                    {
                        todo.Push(new KeyValuePair<Node, int>(child, 0));
                    }
                }
            }
        }

        /// <summary>
        /// Recovers as much file content as possible into a newly created <see cref="BPlusTree{TKey, TValue}"/>, if the operation returns
        /// a non-zero result it was successful and the file has been replaced with a new database containing
        /// the recovered data.  The original file remains in-tact but was renamed with a '.deleted' extension.
        /// </summary>
        /// <remarks> 
        /// If an exception occurs during the parsing of the file and one or more records were recovered, they will
        /// be stored in a file by the same name with an added extension of '.recovered'.  This recovered file can be
        /// opened as a normal <see cref="BPlusTree{TKey, TValue}"/> to view it's contents.  During the restore it is possible that
        /// a single Key was found multiple times, in this case the first occurrence found will be used.
        /// </remarks>
        /// <param name="options"> The options normally used to create the <see cref="BPlusTree{TKey, TValue}"/> instance </param>
        /// <returns>Returns 0 on failure, or the number of records successfully retrieved from the original file </returns>
        public static int RecoverFile(Options options)
        {
            int recoveredCount = 0;
            string filename = options.FileName;

            if (String.IsNullOrEmpty(filename))
                throw new InvalidConfigurationValueException("FileName", "The FileName property was not specified.");
            if (!File.Exists(filename))
                throw new InvalidConfigurationValueException("FileName", "The FileName specified does not exist.");
            if (options.StorageType != StorageType.Disk)
                throw new InvalidConfigurationValueException("StorageType", "The storage type is not set to 'Disk'.");

            int ix = 0;
            string tmpfilename = filename + ".recovered";
            while (File.Exists(tmpfilename))
                tmpfilename = filename + ".recovered" + ix++;

            try
            {
                BPlusTreeOptions<TKey, TValue> tmpoptions = options.Clone();
                tmpoptions.CreateFile = CreatePolicy.Always;
                tmpoptions.FileName = tmpfilename;
                tmpoptions.LockingFactory = new LockFactory<IgnoreLocking>();
                
                using (BPlusTree<TKey, TValue> tmpFile = new BPlusTree<TKey, TValue>(tmpoptions))
                {
                    BulkInsertOptions bulkOptions = new BulkInsertOptions();
                    bulkOptions.DuplicateHandling = DuplicateHandling.LastValueWins;

                    recoveredCount = tmpFile.BulkInsert(RecoveryScan(options, FileShare.None), bulkOptions);
                }
            }
            finally
            {
                if (recoveredCount == 0 && File.Exists(tmpfilename))
                    File.Delete(tmpfilename);
            }

            if (recoveredCount > 0)
            {
                ix = 0;
                string backupName = filename + ".deleted";
                while (File.Exists(backupName))
                    backupName = filename + ".deleted" + ix++;

                File.Move(filename, backupName);
                try { File.Move(tmpfilename, filename); }
                catch
                {
                    File.Move(backupName, filename);
                    throw;
                }
            }

            return recoveredCount;
        }

        /// <summary>
        /// Performs a low-level scan of the storage file to yield all Key/Value pairs it was able to read from the file.
        /// </summary>
        /// <param name="options"> The options normally used to create the <see cref="BPlusTree{TKey, TValue}"/> instance </param>
        /// <param name="sharing"> <see cref="FileShare"/> options used to open the file </param>
        /// <returns> Yields the Key/Value pairs found in the file </returns>
        public static IEnumerable<KeyValuePair<TKey, TValue>> RecoveryScan(Options options, FileShare sharing)
        {
            options = options.Clone();
            options.CreateFile = CreatePolicy.Never;
            string filename = options.FileName;
            if (String.IsNullOrEmpty(filename))
                throw new InvalidConfigurationValueException("FileName", "The FileName property was not specified.");
            if (!File.Exists(filename))
                throw new InvalidConfigurationValueException("FileName", "The FileName specified does not exist.");
            if (options.StorageType != StorageType.Disk)
                throw new InvalidConfigurationValueException("StorageType", "The storage type is not set to 'Disk'.");

            using (FragmentedFile file = new FragmentedFile(filename, options.FileBlockSize, 1, 1, FileAccess.Read, sharing, FileOptions.None))
            {
                NodeSerializer nodeReader = new NodeSerializer(options, new NodeHandleSerializer(new Storage.BTreeFileStore.HandleSerializer()));

                foreach (KeyValuePair<long, Stream> block in file.ForeachBlock(true, false, IngoreDataInvalid))
                {
                    List<KeyValuePair<TKey, TValue>> found = new List<KeyValuePair<TKey, TValue>>();
                    try
                    {
                        foreach (KeyValuePair<TKey, TValue> entry in nodeReader.RecoverLeaf(block.Value))
                            found.Add(entry);
                    }
                    catch
                    { /* Serialization error: Ignore and continue */ }

                    foreach (KeyValuePair<TKey, TValue> entry in found)
                        yield return entry;
                }
            }
        }

        private static bool IngoreDataInvalid(Exception input)
        { return input is InvalidDataException; }
    }
}
