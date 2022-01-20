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

        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var ef = DVDProfilerSerializer<EF.EnhancedFeatures>.FromString(pluginData.Any[0].OuterXml);

                AddInsertCommand(commands, profile, ef);
            }
        }

        private static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, EF.EnhancedFeatures feature)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tEnhancedFeatures VALUES(");
            commandText.Append(SqlProcessor.PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            var features = GetFeatures(feature);

            for (var featureIndex = 1; featureIndex < FeatureCount; featureIndex++)
            {
                commandText.Append(features[featureIndex - 1]);
                commandText.Append(", ");
            }

            commandText.Append(features[FeatureCount - 1]);

            commandText.Append(")");

            commands.Add(commandText);
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