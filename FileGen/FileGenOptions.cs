using System;

namespace FileGen {
    internal enum GenMode {
        M1,
        M2
    }

    internal class FileGenOptions {
        public static FileGenOptions Parse(string[] args) {
            var result = new FileGenOptions();
            result.FileName = args.Length > 0 ? args[0] : "sample.txt";
            int targetSizeInMegabytes;
            if (!int.TryParse(args.Length > 1 ? args[1] : "1024", out targetSizeInMegabytes) || targetSizeInMegabytes < 1)
                targetSizeInMegabytes = 1024;
            result.TargetFileSize = targetSizeInMegabytes;
            GenMode mode;
            if (!Enum.TryParse(args.Length > 2 ? args[2] : "M2", out mode))
                mode = GenMode.M2;
            result.Mode = mode;
            return result;
        }

        protected FileGenOptions() { }

        public string FileName { get; private set; }
        public int TargetFileSize { get; private set; }
        public GenMode Mode { get; private set; }
    }
}
