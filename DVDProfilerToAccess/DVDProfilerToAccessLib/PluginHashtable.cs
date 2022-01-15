namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using Profiler = DVDProfilerXML.Version400;

    internal sealed class PluginHashtable : Hashtable<PluginDataKey>
    {
        internal PluginHashtable(int capacity) : base(capacity)
        { }

        internal void Add(Profiler.PluginData pluginData) => this.Add(new PluginDataKey(pluginData));

        internal bool ContainsKey(Profiler.PluginData pluginData) => this.ContainsKey(new PluginDataKey(pluginData));

        internal int this[Profiler.PluginData pluginData] => base[new PluginDataKey(pluginData)];
    }
}