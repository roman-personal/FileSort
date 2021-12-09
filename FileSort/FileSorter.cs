using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FileSort.Utils;

namespace FileSort {
    internal class FileSorter {
        readonly ConcurrentQueue<FileChunk> filledChunks = new ConcurrentQueue<FileChunk>();
        readonly ConcurrentQueue<FileChunk> freeChunks = new ConcurrentQueue<FileChunk>();
        readonly Queue<string> filesToMerge = new Queue<string>();
        readonly object syncFilesToMerge = new object();
        string tempDirPath;
        string sortedFilePath;
        SemaphoreSlim chunksThrottle;
        ManualResetEventSlim readComplete;
        ManualResetEventSlim sortComplete;

        public FileSorter() { }

        public void Execute(FileSortOptions options) {
            sortedFilePath = options.TargetFileName;
            tempDirPath = GetTempDirPath();
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
            if (!Directory.Exists(tempDirPath))
                Directory.CreateDirectory(tempDirPath);
            else {
                foreach (string fileName in Directory.EnumerateFiles(tempDirPath))
                    File.Delete(fileName);
            }
        }

        void RemoveTempDir() {
            if (Directory.Exists(tempDirPath))
                Directory.Delete(tempDirPath, true);
        }

        void ReadSourceFile(string fileName) {
            try {
                using var reader = new RecordReader(fileName);
                while (true) {
                    chunksThrottle.Wait();
                    Console.WriteLine($"free {freeChunks.Count} filled {filledChunks.Count} trottle {chunksThrottle.CurrentCount}");
                    var chunk = GetFreeChunk();
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

        FileChunk GetFreeChunk() {
            if (!freeChunks.IsEmpty) {
                int attempt = 3;
                while (attempt-- > 0) {
                    if (freeChunks.TryDequeue(out FileChunk chunk))
                        return chunk;
                    else
                        Thread.Sleep(50);
                }
            }
            return new FileChunk();
        }

        void FillChunk(RecordReader reader, FileChunk chunk) {
            while (chunk.Count < FileChunk.Size) {
                var record = reader.ReadRecord();
                if (record == null)
                    break;
                chunk.Add(record);
            }
        }

        void SortChunk() {
            while(!readComplete.IsSet || !filledChunks.IsEmpty) {
                if (filledChunks.TryDequeue(out FileChunk chunk)) {
                    chunk.Sort();
                    string fileName = Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt");
                    WriteChunk(chunk, fileName);
                    lock (syncFilesToMerge)
                        filesToMerge.Enqueue(fileName);
                    chunk.Clear();
                    freeChunks.Enqueue(chunk);
                    chunksThrottle.Release();
                }
                else
                    Thread.Sleep(50);
            }
        }

        void WriteChunk(FileChunk chunk, string fileName) {
            using var writer = new RecordWriter(fileName);
            foreach (var record in chunk)
                writer.Write(record);
            writer.Flush();
        }

        void MergeFiles() {
            while (true) {
                bool lastMerge = false;
                string firstFileName = null;
                string secondFileName = null;
                lock (syncFilesToMerge) {
                    if (filesToMerge.Count == 0 && sortComplete.IsSet)
                        return;
                    if (filesToMerge.Count > 2) {
                        firstFileName = filesToMerge.Dequeue();
                        secondFileName = filesToMerge.Dequeue();
                    }
                    else if (filesToMerge.Count <= 2 && sortComplete.IsSet) {
                        firstFileName = filesToMerge.Dequeue();
                        if (filesToMerge.Count > 0)
                            secondFileName = filesToMerge.Dequeue();
                        lastMerge = true;
                    }
                }
                if (!string.IsNullOrEmpty(firstFileName)) {
                    string fileName = lastMerge ? sortedFilePath : Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt");
                    if (string.IsNullOrEmpty(secondFileName))
                        File.Move(firstFileName, fileName, true);
                    else {
                        MergeFiles(firstFileName, secondFileName, fileName);
                        File.Delete(firstFileName);
                        File.Delete(secondFileName);
                        if (!lastMerge) {
                            lock (syncFilesToMerge)
                                filesToMerge.Enqueue(fileName);
                        }
                    }
                }
                else
                    Thread.Sleep(50);
            }
        }

        void MergeFiles(string firstFileName, string secondFileName, string targetFileName) {
            using var writer = new RecordWriter(targetFileName);
            using var firstReader = new RecordReader(firstFileName);
            using var secondReader = new RecordReader(firstFileName);
            // TODO
            writer.Flush();
        }
    }
}
