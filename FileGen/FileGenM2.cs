using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FileGen {
    internal class FileGenM2 : FileGenBase {
        public FileGenM2(IList<string> textItems, int maxNumber) : base(textItems, maxNumber) { }

        protected override void ExecuteCore(StreamWriter writer, int targetSizeInMegabytes) {
            var random = new Random();
            int chunkSize = Megabyte;
            var sb = new StringBuilder(chunkSize + 1024);
            long targetSize = (long)targetSizeInMegabytes * Megabyte;
            long currentSize = 0;
            while (currentSize < targetSize) {
                int length = (int)Math.Min(chunkSize, targetSize - currentSize);
                while (sb.Length < length) {
                    sb.Append(random.Next(MaxNumber) + 1);
                    sb.Append(". ");
                    sb.AppendLine(TextItems[random.Next(TextItems.Count)]);
                }
                writer.Write(sb);
                currentSize += sb.Length;
                sb.Clear();
            }
        }
    }
}
