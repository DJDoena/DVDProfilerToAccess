using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginHashtable : Hashtable<PluginKey>
    {
        internal PluginHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(PluginData pluginData)
        {
            Add(new PluginKey(pluginData));
        }

        internal bool ContainsKey(PluginData pluginData)
            => (ContainsKey(new PluginKey(pluginData)));

        internal int this[PluginData pluginData]
            => (base[new PluginKey(pluginData)]);
    }
}