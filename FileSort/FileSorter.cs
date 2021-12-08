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
        string tempDirPath;
        SemaphoreSlim chunksThrottle;
        ManualResetEventSlim readComplete;

        public FileSorter() { }

        public void Execute(FileSortOptions options) {
            PrepareTempDir();
            try {
                using (readComplete = new ManualResetEventSlim(false))
                using (chunksThrottle = new SemaphoreSlim(Math.Max(4, options.MaxThreadCount))) {
                    var tasks = CreateSortTasks(Math.Max(2, options.MaxThreadCount - 2));
                    ReadSourceFile(options.SourceFileName);
                    Task.WaitAll(tasks);
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
                    var chunk = GetChunk();
                    FillChunk(reader, chunk);
                    filledChunks.Enqueue(chunk);
                    if (chunk.Count < FileChunk.ChunkSize)
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
            while (chunk.Count < FileChunk.ChunkSize) {
                string line = reader.ReadLine();
                if (line == null)
                    break;
                int pos = line.IndexOf(". ");
                if (pos == -1)
                    throw new InvalidDataException();
                int id;
                if (!int.TryParse(line.Substring(0, pos), out id) || id < 0)
                    throw new InvalidDataException();
                var record = new FileRecord(id, line.Substring(pos + 2));
                chunk.Add(record);
            }
        }

        void SortChunk() {
            while(!readComplete.IsSet) {
                if (filledChunks.IsEmpty)
                    Thread.Sleep(10);
                else {
                    FileChunk chunk;
                    if (filledChunks.TryDequeue(out chunk)) {
                        chunk.Sort();
                        chunksThrottle.Release();
                        WriteChunk(chunk, Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt"));
                        chunk.Clear();
                        freeChunks.Enqueue(chunk);
                    }
                }
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
    }
}
