using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileSort.Utils {
    internal class RecordWriter : IDisposable {
        const int chunkSize = 1024 * 1024;
        Stream stream;
        StreamWriter writer;
        StringBuilder sb;

        public RecordWriter(string fileName) {
            stream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, 32768);
            writer = new StreamWriter(stream);
            sb = new StringBuilder(chunkSize + 1024);
        }

        public void Write(FileRecord record) => Write(record.Num, record.Text);

        public void Write(int num, string text) {
            sb.Append(num);
            sb.Append(". ");
            sb.AppendLine(text);
            if (sb.Length > chunkSize) {
                writer.Write(sb);
                sb.Clear();
            }
        }

        public void Flush() {
            if (sb.Length > 0) {
                writer.Write(sb);
                sb.Clear();
            }
        }

        public void Dispose() {
            Flush();
            writer?.Flush();
            writer?.Dispose();
            writer = null;
            stream?.Dispose();
            stream = null;
            sb = null;
        }
    }
}
