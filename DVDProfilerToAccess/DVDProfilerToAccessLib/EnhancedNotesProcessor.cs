using System;
using System.Collections.Generic;
using System.Text;
using DoenaSoft.ToolBox.Generics;
using EN = DoenaSoft.DVDProfiler.EnhancedNotes;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal static class EnhancedNotesProcessor
    {
        internal static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, Profiler.PluginData pluginData)
        {
            if (pluginData.Any?.Length == 1)
            {
                var en = XmlSerializer<EN.EnhancedNotes>.FromString(pluginData.Any[0].OuterXml);

                AddInsertCommand(commands, profile, en);
            }
        }

        private static void AddInsertCommand(List<StringBuilder> commands, Profiler.DVD profile, EN.EnhancedNotes notes)
        {
            var commandText = new StringBuilder();

            commandText.Append("INSERT INTO tEnhancedNotes VALUES(");
            commandText.Append(SqlProcessor.PrepareTextForDb(profile.ID));
            commandText.Append(", ");

            GetNote(commandText, notes.Note1);

            commandText.Append(", ");

            GetNote(commandText, notes.Note2);

            commandText.Append(", ");

            GetNote(commandText, notes.Note3);

            commandText.Append(", ");

            GetNote(commandText, notes.Note4);

            commandText.Append(", ");

            GetNote(commandText, notes.Note5);

            commandText.Append(")");

            commands.Add(commandText);
        }

        private static void GetNote(StringBuilder commandText, EN.Text text)
        {
            if (text != null)
            {
                var note = string.IsNullOrEmpty(text.Base64Note)
                    ? text.Value
                    : Encoding.UTF8.GetString(Convert.FromBase64String(text.Base64Note));

                commandText.Append(SqlProcessor.PrepareOptionalTextForDb(note));
                commandText.Append(", ");
                commandText.Append(text.IsHtml);
            }
            else
            {
                commandText.Append(SqlProcessor.NULL);
                commandText.Append(", ");
                commandText.Append(SqlProcessor.NULL);
            }
        }
    }
}