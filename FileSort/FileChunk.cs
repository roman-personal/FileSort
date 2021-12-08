using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileSort {
    internal class FileChunk : List<FileRecord> {
        public const int Size = 100000;

        public FileChunk() : base (Size) { }
    }
}
