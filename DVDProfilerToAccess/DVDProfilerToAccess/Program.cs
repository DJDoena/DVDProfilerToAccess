namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.IO;
    using System.Reflection;
    using System.Windows.Forms;
    using DVDProfilerHelper;

    public static class Program
    {
        private static readonly WindowHandle _windowHandle;

        static Program()
        {
            _windowHandle = new WindowHandle();
        }

        [STAThread]
        public static void Main(string[] args)
        {
            try
            {
                var errorFile = Path.Combine(Environment.CurrentDirectory, "error.xml");

                if (File.Exists(errorFile))
                {
                    File.Delete(errorFile);
                }
            }
            catch
            { }

            if (args?.Length > 0)
            {
                var found = false;

                for (var argIndex = 0; argIndex < args.Length; argIndex++)
                {
                    if (args[argIndex] == "/skipversioncheck")
                    {
                        break;
                    }
                }

                if (found == false)
                {
                    CheckForNewVersion();
                }
            }

            Process();
        }

        private static void Process()
        {
            //Phase 1: Ask For File Locations
            Console.WriteLine("Welcome to the DVDProfiler to MS Access Transformer!");
            Console.WriteLine("Version: " + Assembly.GetExecutingAssembly().GetName().Version.ToString());
            Console.WriteLine();
            Console.WriteLine("Please select a \"collection.xml\" and a target location for the Access database!");
            Console.WriteLine("(You should see a file dialog. If not, please minimize your other programs.)");

            using (var ofd = new OpenFileDialog())
            {
                ofd.Filter = "Collection.xml|*.xml";
                ofd.CheckFileExists = true;
                ofd.Multiselect = false;
                ofd.Title = "Select Source File";
                ofd.RestoreDirectory = true;

                if (ofd.ShowDialog(_windowHandle) == DialogResult.Cancel)
                {
                    Console.WriteLine();
                    Console.WriteLine("Aborted.");
                }
                else
                {
                    Process(ofd.FileName);
                }
            }

            Console.WriteLine();
            Console.WriteLine("Press <Enter> to exit.");
            Console.ReadLine();
        }

        private static void Process(string sourceFile)
        {
            using (var sfd = new SaveFileDialog())
            {
                var fi = new FileInfo(sourceFile);

                sfd.InitialDirectory = fi.DirectoryName;
                sfd.FileName = fi.Name.Replace(fi.Extension, ".mdb");
                sfd.Filter = "Access Database|*.mdb";
                sfd.Title = "Select Target File";
                sfd.RestoreDirectory = true;

                if (sfd.ShowDialog(_windowHandle) == DialogResult.Cancel)
                {
                    Console.WriteLine();
                    Console.WriteLine("Aborted.");
                }
                else
                {
                    var originalDatabase = Path.Combine(Environment.CurrentDirectory, "DVDProfiler.mdb");

                    if (sfd.FileName == originalDatabase)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Error: You cannot overwrite default database. Abort.");
                    }
                    else
                    {
                        Process(sourceFile, sfd.FileName);
                    }
                }
            }
        }

        private static void Process(string sourceFile, string targetFile)
        {
            var start = DateTime.Now;

            Console.WriteLine();
            Console.WriteLine("Tranforming data:");

            SqlProcessor sqlProcessor = SqlProcessor.Instance;

            sqlProcessor.ProgressMaxChanged += OnSqlProcessorProgressMaxChanged;
            sqlProcessor.ProgressValueChanged += OnSqlProcessorProgressValueChanged;
            sqlProcessor.Feedback += OnSqlProcessorFeedback;

            sqlProcessor.Process(sourceFile, targetFile);

            sqlProcessor.Feedback -= OnSqlProcessorFeedback;
            sqlProcessor.ProgressValueChanged -= OnSqlProcessorProgressValueChanged;
            sqlProcessor.ProgressMaxChanged -= OnSqlProcessorProgressMaxChanged;

            var end = DateTime.Now;

            var elapsed = new TimeSpan(end.Ticks - start.Ticks);

            Console.WriteLine();
            Console.WriteLine($"Time elapsed: {elapsed.Minutes}m {elapsed.Seconds}s");
        }

        private static void OnSqlProcessorProgressMaxChanged(object sender, EventArgs<int> e)
        {
            if (e.Value == 0)
            {
                Console.WriteLine();
            }
        }

        private static void OnSqlProcessorFeedback(object sender, EventArgs<string> e) => Console.WriteLine(e.Value);

        private static void OnSqlProcessorProgressValueChanged(object sender, EventArgs<int> e)
        {
            var progress = e.Value;

            if (progress > 0)
            {
                if ((progress % 100000) == 0)
                {
                    Console.Write("#");
                }
                else if ((progress % 1000) == 0)
                {
                    Console.Write("-");
                }
                else if ((progress % 500) == 0)
                {
                    Console.Write("|");
                }
            }
            else
            {
                Console.Write("+");
            }
        }

        private static void CheckForNewVersion()
        {
            OnlineAccess.Init("Doena Soft.", "DVD Profiler to Access");
            OnlineAccess.CheckForNewVersion("http://doena-soft.de/dvdprofiler/3.9.0/versions.xml", null, "DVDProfilerToAccess", typeof(SqlProcessor).Assembly);
        }
    }
}