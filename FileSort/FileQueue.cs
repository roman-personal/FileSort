using System;
using System.Collections.Generic;

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
    }
}
