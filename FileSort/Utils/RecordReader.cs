using System;
using System.IO;
using System.Text;

namespace FileSort.Utils {
    internal class RecordReader : IDisposable {
        Stream stream;
        StreamReader reader;
        StringBuilder sb;

        public RecordReader(string fileName) {
            stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.None, 32768);
            reader = new StreamReader(stream);
            sb = new StringBuilder();
        }

        public FileRecord ReadRecord() {
            int num = ReadNumber();
            if (num < 0)
                return null;
            string line = reader.ReadLine();
            if (line == null)
                return null;
            return new FileRecord(num, line);
        }

        int ReadNumber() {
            sb.Clear();
            while (true) {
                int c = reader.Read();
                if (c == -1)
                    return -1;
                if (c == '.') {
                    if (!int.TryParse(sb.ToString(), out int result) || result < 0)
                        throw new InvalidDataException();
                    c = reader.Read();
                    if (c != ' ')
                        throw new InvalidDataException();
                    return result;
                }
                else
                    sb.Append((char)c);
            }
        }

        public void Dispose() {
            reader?.Dispose();
            reader = null;
            stream?.Dispose();
            stream = null;
            sb = null;
        }
    }
}
