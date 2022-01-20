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
        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var ddi = DVDProfilerSerializer<DDI.DigitalDownloadInfo>.FromString(pluginData.Any[0].OuterXml);

                AddInsertCommand(commands, profile, ddi);
            }
        }

        private static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, DDI.DigitalDownloadInfo downloadInfo)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tDigitalDownloadInfo VALUES(");
            commandText.Append(SqlProcessor.PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            GetText(commandText, downloadInfo.Company);

            commandText.Append(", ");

            GetText(commandText, downloadInfo.Code);

            commandText.Append(")");

            commands.Add(commandText);
        }

        private static void GetText(StringBuilder commandText, DDI.Text text)
        {
            if (text != null)
            {
                var info = string.IsNullOrEmpty(text.Base64Text)
                    ? text.Value
                    : Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Text));

                commandText.Append(SqlProcessor.PrepareOptionalTextForDb(info));
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
            }
        }
    }
}