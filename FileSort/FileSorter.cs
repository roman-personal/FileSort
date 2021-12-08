using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FileSort {
    internal class FileSorter {
        readonly ConcurrentQueue<FileChunk> filledChunks = new();
        readonly ConcurrentQueue<FileChunk> freeChunks = new();
        readonly ConcurrentQueue<string> filesToMerge = new();
        string tempDirPath;
        SemaphoreSlim chunksThrottle;
        ManualResetEventSlim readComplete;
        ManualResetEventSlim sortComplete;

        public FileSorter() { }

        public void Execute(FileSortOptions options) {
            PrepareTempDir();
            try {
                using (sortComplete = new ManualResetEventSlim(false))
                using (readComplete = new ManualResetEventSlim(false))
                using (chunksThrottle = new SemaphoreSlim(options.MaxThreadCount)) {
                    var mergeTasks = CreateMergeTasks(2);
                    var sortTasks = CreateSortTasks(Math.Max(2, options.MaxThreadCount - 3));
                    ReadSourceFile(options.SourceFileName);
                    Task.WaitAll(sortTasks);
                    sortComplete.Set();
                    Task.WaitAll(mergeTasks);
                }
            }
            finally {
                RemoveTempDir();
            }
        }

        Task[] CreateSortTasks(int count) {
            var tasks = new List<Task>();
            for (int i = 0; i < count; i++)
                tasks.Add(Task.Run(SortChunk));
            return tasks.ToArray();
        }

        Task[] CreateMergeTasks(int count) {
            var tasks = new List<Task>();
            for (int i = 0; i < count; i++)
                tasks.Add(Task.Run(MergeFiles));
            return tasks.ToArray();
        }

        string GetTempDirPath() =>
            Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "Temp");

        void PrepareTempDir() {
            tempDirPath = GetTempDirPath();
            if (!Directory.Exists(tempDirPath))
                Directory.CreateDirectory(tempDirPath);
            else {
                foreach (string fileName in Directory.EnumerateFiles(tempDirPath))
                    File.Delete(fileName);
            }
        }

        void RemoveTempDir() {
            if (!Directory.Exists(tempDirPath))
                Directory.Delete(tempDirPath, true);
        }

        void ReadSourceFile(string fileName) {
            try {
                using var stream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 32768);
                using var reader = new StreamReader(stream);
                while (true) {
                    chunksThrottle.Wait();
                    //Console.WriteLine($"free {freeChunks.Count} filled {filledChunks.Count} trottle {chunksThrottle.CurrentCount}");
                    var chunk = GetChunk();
                    FillChunk(reader, chunk);
                    filledChunks.Enqueue(chunk);
                    if (chunk.Count < FileChunk.Size)
                        break;
                }
            }
            finally {
                readComplete.Set();
            }
        }

        FileChunk GetChunk() {
            FileChunk chunk;
            if (!freeChunks.TryDequeue(out chunk))
                chunk = new FileChunk();
            return chunk;
        }

        void FillChunk(StreamReader reader, FileChunk chunk) {
            while (chunk.Count < FileChunk.Size) {
                var record = ReadRecord(reader);
                if (record == null)
                    break;
                chunk.Add(record);
            }
        }

        FileRecord ReadRecord(StreamReader reader) {
            int id = ReadNumber(reader);
            if (id < 0)
                return null;
            string line = reader.ReadLine();
            if (line == null)
                return null;
            return new FileRecord(id, line);
        }

        int ReadNumber(StreamReader reader) {
            var sb = new StringBuilder();
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

        void SortChunk() {
            while(!readComplete.IsSet || !filledChunks.IsEmpty) {
                if (filledChunks.TryDequeue(out FileChunk chunk)) {
                    chunk.Sort();
                    chunksThrottle.Release();
                    string fileName = Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt");
                    WriteChunk(chunk, fileName);
                    filesToMerge.Enqueue(fileName);
                    chunk.Clear();
                    freeChunks.Enqueue(chunk);
                }
                else
                    Thread.Sleep(50);
            }
        }

        void WriteChunk(FileChunk chunk, string fileName) {
            using var stream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.None, 32768);
            using var writer = new StreamWriter(stream);
            int chunkSize = 1024 * 1024;
            var sb = new StringBuilder(chunkSize + 1024);
            foreach (var record in chunk) {
                sb.Append(record.Id);
                sb.Append(". ");
                sb.AppendLine(record.Text);
                if (sb.Length > chunkSize) {
                    foreach (var c in sb.GetChunks())
                        writer.Write(c);
                    sb.Clear();
                }
            }
            if (sb.Length > 0) {
                foreach (var c in sb.GetChunks())
                    writer.Write(c);
            }
        }

        void MergeFiles() {
            while (!sortComplete.IsSet || !filesToMerge.IsEmpty) {
                if (filesToMerge.TryDequeue(out string fileName)) {
                    // TODO
                }
                Thread.Sleep(50);
            }
        }
    }
}
