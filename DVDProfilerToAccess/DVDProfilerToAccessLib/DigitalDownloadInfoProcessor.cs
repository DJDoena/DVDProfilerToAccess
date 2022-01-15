namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DVDProfilerHelper;
    using DDI = DigitalDownloadInfo;
    using Profiler = DVDProfilerXML.Version400;

    internal static class DigitalDownloadInfoProcessor
    {
        internal static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var ddi = DVDProfilerSerializer<DDI.DigitalDownloadInfo>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, ddi);
            }
        }

        private static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, DDI.DigitalDownloadInfo ddi)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tDigitalDownloadInfo VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetText(insertCommand, ddi.Company);

            insertCommand.Append(", ");

            GetText(insertCommand, ddi.Code);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static void GetText(StringBuilder insertCommand, DDI.Text text)
        {
            if (text != null)
            {
                var title = (string.IsNullOrEmpty(text.Base64Text)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Text)));

                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(title));
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}