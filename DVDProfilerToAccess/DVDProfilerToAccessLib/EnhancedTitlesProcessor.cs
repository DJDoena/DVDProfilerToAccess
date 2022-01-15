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
        internal static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var et = DVDProfilerSerializer<ET.EnhancedTitles>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, et);
            }
        }

        private static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, ET.EnhancedTitles et)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tEnhancedTitles VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetTitle(insertCommand, et.InternationalEnglishTitle);

            insertCommand.Append(", ");

            GetTitle(insertCommand, et.AlternateOriginalTitle);

            insertCommand.Append(", ");

            GetTitle(insertCommand, et.NonLatinLettersTitle);

            insertCommand.Append(", ");

            GetTitle(insertCommand, et.AdditionalTitle1);

            insertCommand.Append(", ");

            GetTitle(insertCommand, et.AdditionalTitle2);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static void GetTitle(StringBuilder insertCommand, ET.Text text)
        {
            if (text != null)
            {
                var title = (string.IsNullOrEmpty(text.Base64Title)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Title)));

                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(title));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}