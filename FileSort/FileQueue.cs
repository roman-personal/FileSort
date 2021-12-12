using System;
using System.Collections.Generic;
using System.Linq;

namespace FileSort {
    internal class FileSet {
        public FileSet() {
            FileNames = new List<string>();
        }

        public int Generation { get; set; }
        public List<string> FileNames {  get; }
    }

    internal class FileQueue {
        readonly List<List<string>> generations = new List<List<string>>();

        public FileQueue() { }

        public bool IsEmpty => Count == 0;
        public int Count => generations.Sum(x => x.Count);

        public void Enqueue(int generation, string fileName) {
            while (generations.Count <= generation)
                generations.Add(new List<string>());
            generations[generation].Add(fileName);
        }

        public FileSet Dequeue() {
            if (IsEmpty)
                return null;
            var result = new FileSet();
            for (int i = 0; i < generations.Count; i++) {
                var generation = generations[i];
                if (generation.Count > 0) {
                    result.Generation = i;
                    result.FileNames.AddRange(generation);
                    generation.Clear();
                }
            }
            return result;
        }
        
        public FileSet Dequeue(int requiredItemsCount, int maxGeneration) {
            for (int i = 0; i < Math.Min(generations.Count, maxGeneration); i++) {
                var generation = generations[i];
                if (generation.Count >= requiredItemsCount) {
                    var result = new FileSet();
                    result.Generation = i;
                    result.FileNames.AddRange(generation.Take(requiredItemsCount));
                    generation.RemoveRange(0, requiredItemsCount);
                    return result;
                }
            }
            return null;
        }
    }
}
