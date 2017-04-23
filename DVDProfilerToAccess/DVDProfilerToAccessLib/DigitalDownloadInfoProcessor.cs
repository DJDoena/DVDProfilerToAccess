using System;
using System.Collections.Generic;
using System.Text;
using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using DDI = DoenaSoft.DVDProfiler.DigitalDownloadInfo;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class DigitalDownloadInfoProcessor
    {
        internal static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                DDI.DigitalDownloadInfo ddi = Serializer<DDI.DigitalDownloadInfo>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, ddi);
            }
        }

        private static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , DDI.DigitalDownloadInfo ddi)
        {
            StringBuilder insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDigitalDownloadInfo VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetText(insertCommand, ddi.Company);

            insertCommand.Append(", ");

            GetText(insertCommand, ddi.Code);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static void GetText(StringBuilder insertCommand
            , DDI.Text text)
        {
            if (text != null)
            {
                String title = (String.IsNullOrEmpty(text.Base64Text)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Text)));

                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(title));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}