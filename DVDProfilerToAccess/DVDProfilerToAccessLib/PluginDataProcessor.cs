namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System.Collections.Generic;
    using System.Text;
    using DDI = DigitalDownloadInfo;
    using EF = EnhancedFeatures;
    using EN = EnhancedNotes;
    using EPI = EnhancedPurchaseInfo;
    using ET = EnhancedTitles;
    using Profiler = DVDProfilerXML.Version400;

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