using System;
using System.IO;
using System.Text;

namespace FileSort.Utils {
    internal class RecordWriter : IDisposable {
        const int chunkSize = 1024 * 1024;
        const int bufferSize = 32768;
        Stream stream;
        StreamWriter writer;
        StringBuilder sb;

        public RecordWriter(string fileName) {
            stream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize);
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
            if (sb != null && sb.Length > 0) {
                writer.Write(sb);
                sb.Clear();
            }
        }

        public void Dispose() {
            Flush();
            writer?.Dispose();
            writer = null;
            stream?.Dispose();
            stream = null;
            sb = null;
        }
    }
}
