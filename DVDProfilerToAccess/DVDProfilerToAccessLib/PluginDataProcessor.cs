namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System.Collections.Generic;
    using DDI = DigitalDownloadInfo;
    using EF = EnhancedFeatures;
    using EN = EnhancedNotes;
    using EPI = EnhancedPurchaseInfo;
    using ET = EnhancedTitles;
    using Profiler = DVDProfilerXML.Version400;

    internal static class PluginDataProcessor
    {
        internal static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            switch (pluginData.ClassID)
            {
                case EPI.ClassGuid.ClassIDBraced:
                    {
                        EnhancePurchaseInfoProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case EN.ClassGuid.ClassIDBraced:
                    {
                        EnhancedNotesProcessor.GetInsertEnhancedNotesCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case ET.ClassGuid.ClassIDBraced:
                    {
                        EnhancedTitlesProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case DDI.ClassGuid.ClassIDBraced:
                    {
                        DigitalDownloadInfoProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case EF.ClassGuid.ClassIDBraced:
                    {
                        EnhancedFeaturesProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
            }
        }
    }
}