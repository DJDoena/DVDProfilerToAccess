using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginDictionary : Dictionary<PluginDataKey>
    {
        internal PluginDictionary(int capacity) : base(capacity)
        { }

        internal void Add(Profiler.PluginData pluginData) => this.Add(new PluginDataKey(pluginData));

        internal bool ContainsKey(Profiler.PluginData pluginData) => this.ContainsKey(new PluginDataKey(pluginData));

        internal int this[Profiler.PluginData pluginData] => base[new PluginDataKey(pluginData)];
    }
}