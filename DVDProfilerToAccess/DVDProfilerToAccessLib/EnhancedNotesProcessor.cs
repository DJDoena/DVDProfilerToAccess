using System;
using System.Collections.Generic;
using System.Text;
using DoenaSoft.DVDProfiler.DVDProfilerHelper;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using EN = DoenaSoft.DVDProfiler.EnhancedNotes;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancedNotesProcessor
    {
        internal static void GetInsertEnhancedNotesCommand(List<String> sqlCommands
            , DVD dvd
            , PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                EN.EnhancedNotes en = Serializer<EN.EnhancedNotes>.FromString(pluginData.Any[0].OuterXml);

                GetInsertCommand(sqlCommands, dvd, en);
            }
        }

        private static void GetInsertCommand(List<String> sqlCommands
            , DVD dvd
            , EN.EnhancedNotes en)
        {
            StringBuilder insertCommand = new StringBuilder();

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

        private static void GetNote(StringBuilder insertCommand
            , EN.Text text)
        {
            if (text != null)
            {
                String note = (String.IsNullOrEmpty(text.Base64Note)) ? (text.Value) : (Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Note)));

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