using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class PluginKey
    {
        private PluginData m_PluginData;

        private Int32 m_HashCode;

        internal PluginData PluginData
        {
            get
            {
                PluginData pluginData;

                pluginData = new PluginData();
                pluginData.ClassID = m_PluginData.ClassID;
                pluginData.Name = m_PluginData.Name;
                return (pluginData);
            }
        }

        internal PluginKey(PluginData pluginData)
        {            
            m_PluginData = new PluginData();
            m_PluginData.ClassID = pluginData.ClassID;
            m_PluginData.Name = pluginData.Name;
            m_HashCode = m_PluginData.ClassID.GetHashCode();
        }

        public override Int32 GetHashCode()
        {
            return (m_HashCode);
        }

        public override Boolean Equals(Object obj)
        {
            PluginKey other;

            other = obj as PluginKey;
            if (other == null)
            {
                return (false);
            }
            else
            {
                return (m_PluginData.ClassID == other.m_PluginData.ClassID);
            }
        }
    }
}