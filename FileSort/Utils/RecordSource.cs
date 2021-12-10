namespace FileSort.Utils {
    internal class RecordSource : RecordReader {
        public RecordSource(string fileName) : base(fileName) { }
        
        public FileRecord Record { get; private set; }

        public void NextRecord() => Record = ReadRecord();
    }
}
