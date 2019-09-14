using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagKey
    {
        private Tag m_Tag;

        private int m_HashCode;

        internal Tag Tag
        {
            get
            {
                Tag tag = new Tag();

                tag.FullName = m_Tag.FullName;
                tag.Name = m_Tag.Name;

                return (tag);
            }
        }

        internal TagKey(Tag tag)
        {
            m_Tag = new Tag();

            m_Tag.FullName = tag.FullName;
            m_Tag.Name = tag.Name;

            m_HashCode = Tag.FullName.GetHashCode();
        }

        public override int GetHashCode()
            => (m_HashCode);

        public override bool Equals(object obj)
        {
            TagKey other = obj as TagKey;

            if (other == null)
            {
                return (false);
            }
            else
            {
                return (m_Tag.FullName == other.m_Tag.FullName);
            }
        }
    }
}