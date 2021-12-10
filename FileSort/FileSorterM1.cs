using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FileSort.Utils;

namespace FileSort {
    internal class FileSorterM1 : IFileSorter {
        readonly ConcurrentQueue<FileChunk> filledChunks = new ConcurrentQueue<FileChunk>();
        readonly ConcurrentQueue<FileChunk> freeChunks = new ConcurrentQueue<FileChunk>();
        readonly FileQueue filesToMerge = new FileQueue();
        readonly object syncFilesToMerge = new object();
        int filesInMerge = 0;
        string tempDirPath;
        string sortedFilePath;
        SemaphoreSlim chunksThrottle;
        ManualResetEventSlim readComplete;
        ManualResetEventSlim sortComplete;

        public FileSorterM1() { }

        public void Execute(FileSortOptions options) {
            sortedFilePath = options.TargetFileName;
            tempDirPath = GetTempDirPath();
            PrepareTempDir();
            try {
                using (sortComplete = new ManualResetEventSlim(false))
                using (readComplete = new ManualResetEventSlim(false))
                using (chunksThrottle = new SemaphoreSlim(8)) {
                    var mergeTasks = CreateMergeTasks(4);
                    var sortTasks = CreateSortTasks(4);
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
                    //Console.WriteLine($"free {freeChunks.Count} filled {filledChunks.Count} trottle {chunksThrottle.CurrentCount}");
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
                        filesToMerge.Enqueue(1, fileName);
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
            bool lastMerge = false;
            while (!lastMerge) {
                FileQueueItem first = null;
                FileQueueItem second = null;
                lock (syncFilesToMerge) {
                    if (filesToMerge.Count == 0 && sortComplete.IsSet)
                        return;
                    if (filesToMerge.Count > 2) {
                        first = filesToMerge.Dequeue();
                        second = filesToMerge.Dequeue();
                        filesInMerge += 2;
                    }
                    else if (filesToMerge.Count <= 2 && filesInMerge == 0 && sortComplete.IsSet) {
                        first = filesToMerge.Dequeue();
                        if (filesToMerge.Count > 0)
                            second = filesToMerge.Dequeue();
                        lastMerge = true;
                    }
                }
                if (first != null) {
                    string fileName = lastMerge ? sortedFilePath : Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt");
                    if (second == null) {
                        File.Move(first.FileName, fileName, true);
                    }
                    else {
                        MergeFiles(first.FileName, second.FileName, fileName);
                        File.Delete(first.FileName);
                        File.Delete(second.FileName);
                        if (!lastMerge) {
                            lock (syncFilesToMerge) {
                                filesToMerge.Enqueue(Math.Max(first.Generation, second.Generation), fileName);
                                filesInMerge -= 2;
                            }
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
            using var secondReader = new RecordReader(secondFileName);
            var firstRecord = firstReader.ReadRecord();
            var secondRecord = secondReader.ReadRecord();
            while (firstRecord != null || secondRecord != null) {
                if (firstRecord == null) {
                    writer.Write(secondRecord);
                    secondRecord = secondReader.ReadRecord();
                }
                else if (secondRecord == null) {
                    writer.Write(firstRecord);
                    firstRecord = firstReader.ReadRecord();
                }
                else if (firstRecord.CompareTo(secondRecord) <= 0) {
                    writer.Write(firstRecord);
                    firstRecord = firstReader.ReadRecord();
                }
                else {
                    writer.Write(secondRecord);
                    secondRecord = secondReader.ReadRecord();
                }
            }
            writer.Flush();
        }
    }
}
