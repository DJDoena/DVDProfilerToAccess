namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DVDProfilerHelper;
    using ET = EnhancedTitles;
    using Profiler = DVDProfilerXML.Version400;

    internal static class EnhancedTitlesProcessor
    {
        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var et = DVDProfilerSerializer<ET.EnhancedTitles>.FromString(pluginData.Any[0].OuterXml);

                AddInsertCommand(commands, profile, et);
            }
        }

        private static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, ET.EnhancedTitles title)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tEnhancedTitles VALUES(");
            commandText.Append(SqlProcessor.PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            GetTitle(commandText, title.InternationalEnglishTitle);

            commandText.Append(", ");

            GetTitle(commandText, title.AlternateOriginalTitle);

            commandText.Append(", ");

            GetTitle(commandText, title.NonLatinLettersTitle);

            commandText.Append(", ");

            GetTitle(commandText, title.AdditionalTitle1);

            commandText.Append(", ");

            GetTitle(commandText, title.AdditionalTitle2);

            commandText.Append(")");

            commands.Add(commandText);
        }

        private static void GetTitle(StringBuilder commandText, ET.Text text)
        {
            if (text != null)
            {
                var title = string.IsNullOrEmpty(text.Base64Title)
                    ? text.Value
                    : Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Title));

                commandText.Append(SqlProcessor.PrepareOptionalTextForDb(title));
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
            }
        }
    }
}