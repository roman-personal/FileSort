using System;
using System.Collections.Generic;
using System.Linq;
using FileSort.Utils;

namespace FileSort {
    internal class FileSorterM3 : IFileSorter {
        const int MaxNumber = 100000;
        readonly Dictionary<string, int[]> dataDictionary;

        public FileSorterM3() {
            dataDictionary = new Dictionary<string, int[]>(StringComparer.InvariantCultureIgnoreCase);
        }

        public void Execute(FileSortOptions options) {
            ReadData(options.SourceFileName);
            WriteData(options.TargetFileName);
        }

        void ReadData(string fileName) {
            using var reader = new RecordReader(fileName);
            var record = reader.ReadRecord();
            while (record != null) {
                if (!dataDictionary.TryGetValue(record.Text, out int[] numbers)) {
                    numbers = new int[MaxNumber];
                    dataDictionary.Add(record.Text, numbers);
                }
                numbers[record.Num - 1]++;
                record = reader.ReadRecord();
            }
        }

        void WriteData(string fileName) {
            using var writer = new RecordWriter(fileName);
            var orderedItems = dataDictionary.OrderBy(x => x.Key, StringComparer.InvariantCultureIgnoreCase);
            foreach (var item in orderedItems) {
                var numbers = item.Value;
                for (int i = 0; i < numbers.Length; i++) {
                    int count = numbers[i];
                    for (int j = 0; j < count; j++)
                        writer.Write(i + 1, item.Key);
                }
            }
        }
    }
}
