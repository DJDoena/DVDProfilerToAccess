using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using System;
using System.IO;
using System.Reflection;
using System.Windows.Forms;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    public static class Program
    {
        private static WindowHandle WindowHandle = new WindowHandle();

        [STAThread]
        public static void Main(String[] args)
        {
            String workingDirectory;
            String saveFile;

            workingDirectory = Environment.CurrentDirectory;

            try
            {
                if (File.Exists(workingDirectory + @"\error.xml"))
                {
                    File.Delete(workingDirectory + @"\error.xml");
                }
            }
            catch
            {
            }
            if ((args != null) && (args.Length > 0))
            {
                Boolean found;

                found = false;
                for (Int32 i = 0; i < args.Length; i++)
                {
                    if (args[i] == "/skipversioncheck")
                    {
                        break;
                    }
                }
                if (found == false)
                {
                    CheckForNewVersion();
                }
            }
            saveFile = null;
            //Phase 1: Ask For File Locations
            Console.WriteLine("Welcome to the DVDProfiler to MS Access Transformer!");
            Console.WriteLine("Version: " + Assembly.GetExecutingAssembly().GetName().Version.ToString());
            Console.WriteLine();
            Console.WriteLine("Please select a \"collection.xml\" and a target location for the Access database!");
            Console.WriteLine("(You should see a file dialog. If not, please minimize your other programs.)");
            using (OpenFileDialog ofd = new OpenFileDialog())
            {
                ofd.Filter = "Collection.xml|*.xml";
                ofd.CheckFileExists = true;
                ofd.Multiselect = false;
                ofd.Title = "Select Source File";
                ofd.RestoreDirectory = true;
                if (ofd.ShowDialog(WindowHandle) == DialogResult.Cancel)
                {
                    Console.WriteLine();
                    Console.WriteLine("Aborted.");
                }
                else
                {
                    using (SaveFileDialog sfd = new SaveFileDialog())
                    {
                        FileInfo fi;

                        fi = new FileInfo(ofd.FileName);
                        sfd.InitialDirectory = fi.DirectoryName;
                        sfd.FileName = fi.Name.Replace(fi.Extension, ".mdb");
                        sfd.Filter = "Access Database|*.mdb";
                        sfd.Title = "Select Target File";
                        sfd.RestoreDirectory = true;
                        if (sfd.ShowDialog(WindowHandle) == DialogResult.Cancel)
                        {
                            Console.WriteLine();
                            Console.WriteLine("Aborted.");
                        }
                        else
                        {
                            if (sfd.FileName == workingDirectory + @"\DVDProfiler.mdb")
                            {
                                Console.WriteLine();
                                Console.WriteLine("Error: You cannot overwrite default database. Abort.");
                            }
                            else
                            {
                                SqlProcessor sqlProcessor;

                                saveFile = sfd.FileName;
                                //Phase 2: Fill Hashtables
                                Console.WriteLine();
                                Console.WriteLine("Tranforming data:");

                                sqlProcessor = SqlProcessor.Instance;

                                sqlProcessor.ProgressMaxChanged += OnSqlProcessorProgressMaxChanged;
                                sqlProcessor.ProgressValueChanged += OnSqlProcessorProgressValueChanged;
                                sqlProcessor.Feedback += OnSqlProcessorFeedback;
                                
                                sqlProcessor.Process(ofd.FileName, sfd.FileName);
                                
                                sqlProcessor.Feedback -= OnSqlProcessorFeedback;
                                sqlProcessor.ProgressValueChanged -= OnSqlProcessorProgressValueChanged;
                                sqlProcessor.ProgressMaxChanged -= OnSqlProcessorProgressMaxChanged;
                            }
                        }
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Press <Enter> to exit.");
            Console.ReadLine();
        }

        static void OnSqlProcessorProgressMaxChanged(Object sender, EventArgs<Int32> e)
        {
            if (e.Value == 0)
            {
                Console.WriteLine();
            }
        }

        static void OnSqlProcessorFeedback(Object sender, EventArgs<String> e)
        {
            Console.WriteLine(e.Value);
        }

        static void OnSqlProcessorProgressValueChanged(Object sender, EventArgs<Int32> e)
        {
            Int32 progress;

            progress = e.Value;
            if (progress > 0)
            {
                if ((progress % 1000) == 0)
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