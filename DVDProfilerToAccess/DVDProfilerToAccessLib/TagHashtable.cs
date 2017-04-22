using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagHashtable : Hashtable<TagKey>
    {
        internal TagHashtable(Int32 capacity)
            : base(capacity)
        {
        }

        internal void Add(Tag tag)
        {
            base.Add(new TagKey(tag));
        }

        internal Boolean ContainsKey(Tag tag)
        {
            return (base.ContainsKey(new TagKey(tag)));
        }

        internal Int32 this[Tag tag]
        {
            get
            {
                return (base[new TagKey(tag)]);
            }
        }
    }
}