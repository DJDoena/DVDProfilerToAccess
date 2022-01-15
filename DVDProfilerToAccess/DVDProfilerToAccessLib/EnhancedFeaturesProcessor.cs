namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System.Collections.Generic;
    using System.Text;
    using DVDProfilerHelper;
    using EF = EnhancedFeatures;
    using Profiler = DVDProfilerXML.Version400;

    internal static class EnhancedFeaturesProcessor
    {
        private const byte FeatureCount = 40;

        internal static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var ef = DVDProfilerSerializer<EF.EnhancedFeatures>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, ef);
            }
        }

        private static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, EF.EnhancedFeatures ef)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tEnhancedFeatures VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            var features = GetFeatures(ef);

            for (var featureIndex = 1; featureIndex < FeatureCount; featureIndex++)
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
            var features = new bool[FeatureCount];

            if (ef.Feature != null)
            {
                foreach (var feature in ef.Feature)
                {
                    features[feature.Index - 1] = feature.Value;
                }
            }

            return features;
        }
    }
}