namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DVDProfilerHelper;
    using EN = EnhancedNotes;
    using Profiler = DVDProfilerXML.Version400;

    internal static class EnhancedNotesProcessor
    {
        internal static void GetInsertEnhancedNotesCommand(List<string> sqlCommands, Profiler.DVD dvd, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var en = DVDProfilerSerializer<EN.EnhancedNotes>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, en);
            }
        }

        private static void GetInsertCommand(List<string> sqlCommands, Profiler.DVD dvd, EN.EnhancedNotes en)
        {
            var insertCommand = new StringBuilder();

            insertCommand.Append("INSERT INTO tEnhancedNotes VALUES(");
            insertCommand.Append(SqlProcessor.PrepareTextForDb(dvd.ID));
            insertCommand.Append(", ");

            GetNote(insertCommand, en.Note1);

            insertCommand.Append(", ");

            GetNote(insertCommand, en.Note2);

            insertCommand.Append(", ");

            GetNote(insertCommand, en.Note3);

            insertCommand.Append(", ");

            GetNote(insertCommand, en.Note4);

            insertCommand.Append(", ");

            GetNote(insertCommand, en.Note5);

            insertCommand.Append(")");

            sqlCommands.Add(insertCommand.ToString());
        }

        private static void GetNote(StringBuilder insertCommand, EN.Text text)
        {
            if (text != null)
            {
                var note = (string.IsNullOrEmpty(text.Base64Note)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Note)));

                insertCommand.Append(SqlProcessor.PrepareOptionalTextForDb(note));
                insertCommand.Append(", ");
                insertCommand.Append(text.IsHtml);
            }
            else
            {
                insertCommand.Append(SqlProcessor.NULL);
                insertCommand.Append(", ");
                insertCommand.Append(SqlProcessor.NULL);
            }
        }
    }
}