using System;
using System.Collections.Generic;
using System.Linq;

namespace FileSort {
    internal class FileQueueItem {
        public FileQueueItem(int generation, string fileName) {
            Generation = generation;
            FileName = fileName;
        }

        public int Generation { get; }
        public string FileName { get; }
    }

    internal class FileQueueItemComparer : IComparer<FileQueueItem> {
        public int Compare(FileQueueItem x, FileQueueItem y) => 
            x.Generation.CompareTo(y.Generation);
    }

    internal class FileQueue {
        readonly List<FileQueueItem> items = new List<FileQueueItem>();
        readonly IComparer<FileQueueItem> comparer = new FileQueueItemComparer();

        public FileQueue() { }

        public bool IsEmpty => items.Count == 0;
        public int Count => items.Count;

        public void Enqueue(FileQueueItem item) {
            int index = items.BinarySearch(item, comparer);
            if (index < 0)
                index = ~index;
            items.Insert(index, item);
        }

        public void Enqueue(int generation, string fileName) => 
            Enqueue(new FileQueueItem(generation, fileName));

        public FileQueueItem Dequeue() {
            if (IsEmpty)
                throw new InvalidOperationException();
            var result = items[0];
            items.RemoveAt(0);
            return result;
        }

        public IEnumerable<FileQueueItem> BulkDequeue(int maxGeneration, int count) {
            var result = new List<FileQueueItem>();
            for (int i = 0; i < count; i++) {
                if (items[i].Generation > maxGeneration)
                    break;
                result.Add(items[i]);
            }
            if (result.Count < 2)
                result.Clear();
            else
                items.RemoveRange(0, result.Count);
            return result;
        }

        public int GetCount(int maxGeneration) =>
            items.Where(x => x.Generation <= maxGeneration).Count();
    }
}
