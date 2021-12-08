﻿using System;

namespace FileSort {
    internal class FileRecord : IComparable<FileRecord>{
        public FileRecord(int id, string text) {
            Id = id;
            Text = text;
        }

        public int Id { get; }
        public string Text {  get; }

        public int CompareTo(FileRecord other) {
            int result = string.Compare(Text, other.Text, StringComparison.InvariantCultureIgnoreCase);
            if (result == 0)
                result = Id.CompareTo(other.Id);
            return result;
        }
    }
}
