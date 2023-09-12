using System.Collections.Generic;
using System.Text;
using DDI = DoenaSoft.DVDProfiler.DigitalDownloadInfo;
using EF = DoenaSoft.DVDProfiler.EnhancedFeatures;
using EN = DoenaSoft.DVDProfiler.EnhancedNotes;
using EPI = DoenaSoft.DVDProfiler.EnhancedPurchaseInfo;
using ET = DoenaSoft.DVDProfiler.EnhancedTitles;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class PluginDataProcessor
    {
        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            switch (pluginData.ClassID)
            {
                case EPI.ClassGuid.ClassIDBraced:
                    {
                        EnhancePurchaseInfoProcessor.AddInsertCommand(commands, dvd, pluginData);

                        break;
                    }
                case EN.ClassGuid.ClassIDBraced:
                    {
                        EnhancedNotesProcessor.AddInsertCommand(commands, dvd, pluginData);

                        break;
                    }
                case ET.ClassGuid.ClassIDBraced:
                    {
                        EnhancedTitlesProcessor.AddInsertCommand(commands, dvd, pluginData);

                        break;
                    }
                case DDI.ClassGuid.ClassIDBraced:
                    {
                        DigitalDownloadInfoProcessor.AddInsertCommand(commands, dvd, pluginData);

                        break;
                    }
                case EF.ClassGuid.ClassIDBraced:
                    {
                        EnhancedFeaturesProcessor.AddInsertCommand(commands, dvd, pluginData);

                        break;
                    }
            }
        }
    }
}