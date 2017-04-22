using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginHashtable : Hashtable<PluginKey>
    {
        internal PluginHashtable(Int32 capacity)
            : base(capacity)
        {
        }

        internal void Add(PluginData pluginData)
        {
            base.Add(new PluginKey(pluginData));
        }

        internal Boolean ContainsKey(PluginData pluginData)
        {
            return (base.ContainsKey(new PluginKey(pluginData)));
        }

        internal Int32 this[PluginData pluginData]
        {
            get
            {
                return (base[new PluginKey(pluginData)]);
            }
        }
    }
}