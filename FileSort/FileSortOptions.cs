using System;

namespace FileSort {
    internal enum SortMode {
        M1,
        M2
    }

    internal class FileSortOptions {
        public static FileSortOptions Parse(string[] args) {
            var result = new FileSortOptions();
            result.SourceFileName = args.Length > 0 ? args[0] : "sample.txt";
            result.TargetFileName = args.Length > 1 ? args[1] : "sorted.txt";
            if (!Enum.TryParse(args.Length > 2 ? args[2].ToUpper() : "M1", out SortMode mode))
                mode = SortMode.M1;
            result.Mode = mode;
            return result;
        }

        protected FileSortOptions() { }

        public string SourceFileName { get; private set; }
        public string TargetFileName { get; private set; }
        public SortMode Mode { get; private set; }
    }
}
