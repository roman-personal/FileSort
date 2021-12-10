using System;

namespace FileSort {
    internal class FileRecord : IComparable<FileRecord>{
        public FileRecord(int num, string text) {
            Num = num;
            Text = text;
        }

        public int Num { get; }
        public string Text {  get; }

        public int CompareTo(FileRecord other) {
            int result = string.Compare(Text, other.Text);
            if (result == 0)
                result = Num.CompareTo(other.Num);
            return result;
        }
    }
}
