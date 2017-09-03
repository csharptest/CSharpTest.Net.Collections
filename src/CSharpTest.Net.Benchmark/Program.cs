using System;
using System.Collections.Generic;
using System.Diagnostics;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Benchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            string filename = @"D:\Temp\btree.dat";

            BPlusTree<long, int>.OptionsV2 options = new BPlusTree<long, int>.OptionsV2(PrimitiveSerializer.Int64, PrimitiveSerializer.Int32);
            options.CalcBTreeOrder(4, 100); //we can simply just guess close
            options.FileName = filename;
            options.CreateFile = CreatePolicy.IfNeeded; //obviously this is just for testing
            options.StoragePerformance = StoragePerformance.Fastest;
            options.TransactionLogLimit = 100 * 1024 * 1024;
            options.CachePolicy = CachePolicy.None;

            Stopwatch sw = Stopwatch.StartNew();

            BulkInsertOptions bulkOptions = new BulkInsertOptions() { DuplicateHandling = DuplicateHandling.None };

            using (BPlusTree<long, int> tree = new BPlusTree<long, int>(options))
            {
                //tree.BulkInsert(GenerateTestData(1_000_000), bulkOptions);

                foreach (var pair in GenerateTestData(1_000_000))
                {
                    tree.TryGetValue(pair.Key, out _);
                }
            }

            Console.WriteLine(sw.Elapsed);
        }

        private static IEnumerable<KeyValuePair<long, int>> GenerateTestData(int count)
        {
            Random r = new Random(666);

            for (int i = 0; i < count; i++)
            {
                yield return new KeyValuePair<long, int>(r.Next(), r.Next());
            }
        }
    }
}
