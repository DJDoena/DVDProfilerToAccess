namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System;
    using System.Diagnostics;
    using Profiler = DVDProfilerXML.Version400;

    [DebuggerDisplay("{ClassId}: {Name}")]
    internal sealed class PluginDataKey : IEquatable<PluginDataKey>
    {
        private readonly int _hashCode;

        public Guid ClassId { get; }

        public string Name { get; }

        public PluginDataKey(Profiler.PluginData pluginData)
        {
            this.ClassId = new Guid(pluginData.ClassID);

            this.Name = pluginData.Name;

            _hashCode = this.ClassId.GetHashCode();
        }

        public override int GetHashCode() => _hashCode;

        public override bool Equals(object obj) => this.Equals(obj as PluginDataKey);

        public bool Equals(PluginDataKey other)
        {
            if (other == null)
            {
                return false;
            }

            var equals = this.ClassId == other.ClassId;

            return equals;
        }
    }
}