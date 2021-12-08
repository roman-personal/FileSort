using System;

namespace FileSort {
    internal class FileSortOptions {
        public static FileSortOptions Parse(string[] args) {
            var result = new FileSortOptions();
            result.SourceFileName = args.Length > 0 ? args[0] : "sample.txt";
            result.TargetFileName = args.Length > 1 ? args[1] : "sorted.txt";
            int maxThreadCount;
            if (!int.TryParse(args.Length > 2 ? args[2] : "0", out maxThreadCount) || 
                maxThreadCount < 1 || maxThreadCount > Environment.ProcessorCount)
                maxThreadCount = Environment.ProcessorCount;
            result.MaxThreadCount = maxThreadCount;
            return result;
        }

        protected FileSortOptions() { }

        public string SourceFileName { get; private set; }
        public string TargetFileName { get; private set; }
        public int MaxThreadCount { get; private set; }
    }
}
