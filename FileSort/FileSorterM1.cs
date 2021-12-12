using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FileSort.Utils;

namespace FileSort {
    internal class FileSorterM1 : IFileSorter {
        const int NumberOfSortingTasks = 4;
        const int NumberOfMergingTasks = 6;
        const int MaxNumberOfFilledChunks = 12;
        const int NumberOfFilesToMerge = 2;
        const int MaxGeneration = 5;
        readonly ConcurrentQueue<FileChunk> filledChunks = new ConcurrentQueue<FileChunk>();
        readonly ConcurrentQueue<FileChunk> freeChunks = new ConcurrentQueue<FileChunk>();
        readonly FileQueue filesToMerge = new FileQueue();
        readonly object syncFilesToMerge = new object();
        int filesInMerge = 0;
        string tempDirPath;
        string sortedFilePath;
        SemaphoreSlim chunkThrottle;
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
                using (chunkThrottle = new SemaphoreSlim(MaxNumberOfFilledChunks)) {
                    var mergeTasks = CreateMergeTasks(NumberOfMergingTasks);
                    var sortTasks = CreateSortTasks(NumberOfSortingTasks);
                    ReadSourceFile(options.SourceFileName);
                    Task.WaitAll(sortTasks);
                    sortComplete.Set();
                    Console.WriteLine("Read/sort completed");
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
                    chunkThrottle.Wait();
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
                        filesToMerge.Enqueue(0, fileName);
                    chunk.Clear();
                    freeChunks.Enqueue(chunk);
                    chunkThrottle.Release();
                }
                else
                    Thread.Sleep(50);
            }
        }

        void WriteChunk(FileChunk chunk, string fileName) {
            using var writer = new RecordWriter(fileName);
            foreach (var record in chunk)
                writer.Write(record);
        }

        void MergeFiles() {
            bool lastMerge = false;
            while (!lastMerge) {
                FileSet fileSet = null;
                lock (syncFilesToMerge) {
                    if (sortComplete.IsSet) {
                        if (filesToMerge.Count == 0 && filesInMerge == 0)
                            return;
                        if (filesInMerge == 0) {
                            fileSet = filesToMerge.Dequeue();
                            lastMerge = true;
                            Console.WriteLine("Last merge");
                        }
                        else {
                            fileSet = filesToMerge.Dequeue(NumberOfFilesToMerge, MaxGeneration);
                            if (fileSet != null)
                                filesInMerge += fileSet.FileNames.Count;
                        }
                    }
                    else {
                        fileSet = filesToMerge.Dequeue(NumberOfFilesToMerge, MaxGeneration);
                        if (fileSet != null)
                            filesInMerge += fileSet.FileNames.Count;
                    }
                }
                if (fileSet != null) {
                    string fileName = lastMerge ? sortedFilePath : Path.Combine(tempDirPath, Guid.NewGuid().ToString() + ".txt");
                    if (fileSet.FileNames.Count == 1) {
                        File.Move(fileSet.FileNames[0], fileName, true);
                    }
                    else {
                        MergeFiles(fileSet.FileNames, fileName);
                        fileSet.FileNames.ForEach(x => File.Delete(x));
                        if (!lastMerge) {
                            lock (syncFilesToMerge) {
                                filesToMerge.Enqueue(fileSet.Generation + 1, fileName);
                                filesInMerge -= fileSet.FileNames.Count;
                            }
                        }
                    }
                }
                else
                    Thread.Sleep(100);
            }
        }

        void MergeFiles(IEnumerable<string> sourceFileNames, string targetFileName) {
            using var writer = new RecordWriter(targetFileName);
            var recordSources = new List<RecordSource>();
            try {
                foreach (var sourceFileName in sourceFileNames)
                    recordSources.Add(new RecordSource(sourceFileName));
                foreach (var source in recordSources)
                    source.NextRecord();
                while (true) {
                    var source = GetSourceWithLowestRecord(recordSources);
                    if (source == null)
                        break;
                    FileRecord current;
                    do {
                        current = source.Record;
                        writer.Write(current);
                        source.NextRecord();
                    }
                    while (source.Record != null && current.CompareTo(source.Record) == 0);
                    //writer.Write(source.Record);
                    //source.NextRecord();
                }
                writer.Flush();
            }
            finally {
                foreach (var source in recordSources)
                    source.Dispose();
            }
        }

        RecordSource GetSourceWithLowestRecord(IEnumerable<RecordSource> recordSources) {
            RecordSource result = null;
            foreach (var recordSource in recordSources.Where(x => x.Record != null)) {
                if (result == null || recordSource.Record.CompareTo(result.Record) < 0)
                    result = recordSource;
            }
            return result;
        }
    }
}
