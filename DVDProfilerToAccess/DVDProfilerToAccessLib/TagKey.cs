using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagKey
    {
        private Tag m_Tag;

        private Int32 m_HashCode;

        internal Tag Tag
        {
            get
            {
                Tag tag;

                tag = new Tag();
                tag.FullName = this.m_Tag.FullName;
                tag.Name = this.m_Tag.Name;
                return (tag);
            }
        }

        internal TagKey(Tag tag)
        {
            this.m_Tag = new Tag();
            this.m_Tag.FullName = tag.FullName;
            this.m_Tag.Name = tag.Name;
            this.m_HashCode = this.Tag.FullName.GetHashCode();
        }

        public override Int32 GetHashCode()
        {
            return (this.m_HashCode);
        }

        public override Boolean Equals(Object obj)
        {
            TagKey other;

            other = obj as TagKey;
            if (other == null)
            {
                return (false);
            }
            else
            {
                return (this.m_Tag.FullName == other.m_Tag.FullName);
            }
        }
    }
}