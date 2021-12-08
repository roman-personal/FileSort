using System;
using System.Collections.Generic;
using System.IO;

namespace FileGen {
    internal abstract class FileGenBase {
        protected const int Megabyte = 1024 * 1024;

        protected FileGenBase(IList<string> textItems, int maxNumber) {
            if (maxNumber < 1)
                throw new ArgumentException("maxNumber should be greater than zero");
            if (textItems == null)
                throw new ArgumentNullException("textItems should not be null");
            if (textItems.Count == 0)
                throw new ArgumentException("textItems should not be empty");
            MaxNumber = maxNumber;
            TextItems = textItems;
        }

        public int MaxNumber { get; }
        public IList<string> TextItems { get; }

        public void Execute(FileGenOptions options) {
            if (options == null)
                throw new ArgumentNullException("options should not be null");
            using var stream = new FileStream(options.FileName, FileMode.Create, FileAccess.Write, FileShare.None, 32768);
            using var writer = new StreamWriter(stream);
            ExecuteCore(writer, options.TargetFileSize);
        }

        protected abstract void ExecuteCore(StreamWriter writer, int targetSizeInMegabytes);
    }
}
