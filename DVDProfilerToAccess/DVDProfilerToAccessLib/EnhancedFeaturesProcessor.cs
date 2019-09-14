using System;
using System.Collections.Generic;
using System.Text;
using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;
using EF = DoenaSoft.DVDProfiler.EnhancedFeatures;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancedFeaturesProcessor
    {
        private const Byte FeatureCount = 40;

        internal static void GetInsertCommand(List<string> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                EF.EnhancedFeatures ef = DVDProfilerSerializer<EF.EnhancedFeatures>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, ef);
            }
        }

        private static void GetInsertCommand(List<string> sqlCommands
            , DVD dvd
            , EF.EnhancedFeatures ef)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tEnhancedFeatures VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            bool[] features = GetFeatures(ef);

            for (Byte featureIndex = 1; featureIndex < FeatureCount; featureIndex++)
            {
                insertCommand.Append(features[featureIndex - 1]);
                insertCommand.Append(", ");
            }

            insertCommand.Append(features[FeatureCount - 1]);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static bool[] GetFeatures(EF.EnhancedFeatures ef)
        {
            bool[] features = new bool[FeatureCount];

            if (ef.Feature != null)
            {
                foreach (EF.Feature feature in ef.Feature)
                {
                    features[feature.Index - 1] = feature.Value;
                }
            }

            return (features);
        }
    }
}