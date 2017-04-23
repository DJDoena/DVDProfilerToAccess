using System;
using System.Collections.Generic;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using DDI = DoenaSoft.DVDProfiler.DigitalDownloadInfo;
using EF = DoenaSoft.DVDProfiler.EnhancedFeatures;
using EN = DoenaSoft.DVDProfiler.EnhancedNotes;
using EPI = DoenaSoft.DVDProfiler.EnhancedPurchaseInfo;
using ET = DoenaSoft.DVDProfiler.EnhancedTitles;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class PluginDataProcessor
    {
        internal static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            switch (pluginData.ClassID)
            {
                case (EPI.ClassGuid.ClassIDBraced):
                    {
                        EnhancePurchaseInfoProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case (EN.ClassGuid.ClassIDBraced):
                    {
                        EnhancedNotesProcessor.GetInsertEnhancedNotesCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case (ET.ClassGuid.ClassIDBraced):
                    {
                        EnhancedTitlesProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case (DDI.ClassGuid.ClassIDBraced):
                    {
                        DigitalDownloadInfoProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
                case (EF.ClassGuid.ClassIDBraced):
                    {
                        EnhancedFeaturesProcessor.GetInsertCommand(sqlCommands, dvd, pluginData);

                        break;
                    }
            }
        }
    }
}
