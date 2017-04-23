using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using ET = DoenaSoft.DVDProfiler.EnhancedTitles;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancedTitlesProcessor
    {
        internal static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                using (StringReader sr = new StringReader(pluginData.Any[0].OuterXml))
                {
                    ET.EnhancedTitles et = (ET.EnhancedTitles)(ET.EnhancedTitles.XmlSerializer.Deserialize(sr));

                    GetInsertCommand(sqlCommands, dvd, et);
                }
            }
        }

        private static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , ET.EnhancedTitles et)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private static void GetTitle(StringBuilder insertCommand
            , ET.Text text)
        {
            if (text != null)
            {
                String title = (String.IsNullOrEmpty(text.Base64Title)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Title)));

                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(title));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}