using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileGen {
    internal class FileGenM3 : FileGenBase {
        readonly int chunkSize = Megabyte;
        ManualResetEventSlim readyToWrite;
        ManualResetEventSlim readyToGen;
        StringBuilder sbToWrite;
        StringBuilder sbToGen;
        int lengthToGen;

        public FileGenM3(IList<string> textItems, int maxNumber) : base(textItems, maxNumber) { }

        protected override void ExecuteCore(StreamWriter writer, int targetSizeInMegabytes) {
            sbToGen = new StringBuilder(chunkSize + 1024);
            sbToWrite = new StringBuilder(chunkSize + 1024);
            using (readyToWrite = new ManualResetEventSlim(false))
            using (readyToGen = new ManualResetEventSlim(false)) {
                var tasks = new Task[] { 
                    Task.Run(GenerateText), 
                    Task.Run(() => WriteText(writer, targetSizeInMegabytes))
                };
                Task.WaitAll(tasks);
            }
        }

        void GenerateText() {
            var random = new Random();
            bool goAhead = true;
            while (goAhead) {
                readyToGen.Wait();
                readyToGen.Reset();
                while (sbToGen.Length < lengthToGen) {
                    sbToGen.Append(random.Next(MaxNumber) + 1);
                    sbToGen.Append(". ");
                    sbToGen.AppendLine(TextItems[random.Next(TextItems.Count)]);
                }
                if (lengthToGen < chunkSize)
                    goAhead = false;
                readyToWrite.Set();
            }
        }

        void WriteText(StreamWriter writer, int targetSizeInMegabytes) {
            long currentSize = 0;
            long targetSize = (long)targetSizeInMegabytes * Megabyte;
            lengthToGen = (int)Math.Min(chunkSize, targetSize - currentSize);
            readyToGen.Set();
            while (currentSize < targetSize) {
                readyToWrite.Wait();
                readyToWrite.Reset();
                SwapStringBuilders();
                currentSize += sbToWrite.Length;
                lengthToGen = (int)Math.Min(chunkSize, targetSize - currentSize);
                readyToGen.Set();
                foreach (var chunk in sbToWrite.GetChunks())
                    writer.Write(chunk);
                sbToWrite.Clear();
            }
        }

        void SwapStringBuilders() {
            var tmp = sbToGen;
            sbToGen = sbToWrite;
            sbToWrite = tmp;
        }
    }
}
