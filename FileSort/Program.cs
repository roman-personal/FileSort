using System;
using System.Diagnostics;

namespace FileSort {
    internal class Program {
        static void Main(string[] args) {
            try {
                var options = FileSortOptions.Parse(args);
                Console.WriteLine($"Sorting: {options.SourceFileName}");
                var sorter = CreateSorter(options.Mode);
                var sw = new Stopwatch();
                sw.Start();
                sorter.Execute(options);
                sw.Stop();
                Console.WriteLine($"Done! Elapsed: {sw.Elapsed }");
            }
            catch (Exception ex) {
                Console.WriteLine("Failed!");
                Console.WriteLine(ex.Message);
            }
        }

        static IFileSorter CreateSorter(SortMode mode) {
            // TODO
            return new FileSorterM1();
        }
    }
}
