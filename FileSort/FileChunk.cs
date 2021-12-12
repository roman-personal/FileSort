using System.Collections.Generic;

namespace FileSort {
    internal class FileChunk : List<FileRecord> {
        public const int Size = 1024000;

        public FileChunk() : base (Size) { }
    }
}
