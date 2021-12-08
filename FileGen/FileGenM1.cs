using System;
using System.Collections.Generic;
using System.IO;

namespace FileGen {
    internal class FileGenM1 : FileGenBase {
        public FileGenM1(IList<string> textItems, int maxNumber) : base(textItems, maxNumber) { }

        protected override void ExecuteCore(StreamWriter writer, int targetSizeInMegabytes) {
            var random = new Random();
            long targetSize = (long)targetSizeInMegabytes * Megabyte;
            while (writer.BaseStream.Length < targetSize)
                writer.WriteLine($"{random.Next(MaxNumber) + 1}. {TextItems[random.Next(TextItems.Count)]}");
        }
    }
}
