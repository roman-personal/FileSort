using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace FileGen {
    internal class Program {
        const int MaxNumber = 100000;

        static void Main(string[] args) {
            try {
                var options = FileGenOptions.Parse(args);
                Console.WriteLine($"Generating: {options.FileName}, target size {options.TargetFileSize}MB");
                var generator = CreateGenerator(options.Mode);
                var sw = new Stopwatch();
                sw.Start();
                generator.Excecute(options);
                sw.Stop();
                Console.WriteLine($"Done! Elapsed: {sw.Elapsed }");
            }
            catch (Exception ex) {
                Console.WriteLine("Failed!");
                Console.WriteLine(ex.Message);
            }
        }

        static FileGenBase CreateGenerator(GenMode mode) {
            var textItems = LoadTextItems();
            return mode switch {
                GenMode.M1 => new FileGenM1(textItems, MaxNumber),
                GenMode.M3 => new FileGenM3(textItems, MaxNumber),
                _ => new FileGenM2(textItems, MaxNumber),
            };
        }

        static IList<string> LoadTextItems() {
            string fileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "sentences.txt");
            var result = new List<string>();
            using var reader = File.OpenText(fileName);
            while (true) {
                string line = reader.ReadLine();
                if (line == null)
                    break;
                result.Add(line);
            }
            return result;
        }
    }
}
