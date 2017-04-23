using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginHashtable : Hashtable<PluginKey>
    {
        internal PluginHashtable(Int32 capacity)
            : base(capacity)
        { }

        internal void Add(PluginData pluginData)
        {
            Add(new PluginKey(pluginData));
        }

        internal Boolean ContainsKey(PluginData pluginData)
            => (ContainsKey(new PluginKey(pluginData)));

        internal Int32 this[PluginData pluginData]
            => (base[new PluginKey(pluginData)]);
    }
}