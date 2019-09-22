using System;
using System.Diagnostics;
using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    [DebuggerDisplay("{ClassId}: {Name}")]
    internal sealed class PluginDataKey : IEquatable<PluginDataKey>
    {
        private readonly int _hashCode;

        public Guid ClassId { get; }

        public string Name { get; }

        public PluginDataKey(Profiler.PluginData pluginData)
        {
            ClassId = new Guid(pluginData.ClassID);

            Name = pluginData.Name;

            _hashCode = ClassId.GetHashCode();
        }

        public override int GetHashCode() => _hashCode;

        public override bool Equals(object obj) => Equals(obj as PluginDataKey);

        public bool Equals(PluginDataKey other)
        {
            if (other == null)
            {
                return false;
            }

            var equals = ClassId == other.ClassId;

            return equals;
        }
    }
}