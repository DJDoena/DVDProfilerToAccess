using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginHashtable : Hashtable<PluginDataKey>
    {
        internal PluginHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(PluginData pluginData)
        {
            Add(new PluginDataKey(pluginData));
        }

        internal bool ContainsKey(PluginData pluginData)
            => (ContainsKey(new PluginDataKey(pluginData)));

        internal int this[PluginData pluginData]
            => (base[new PluginDataKey(pluginData)]);
    }
}