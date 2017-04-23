using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagHashtable : Hashtable<TagKey>
    {
        internal TagHashtable(Int32 capacity)
            : base(capacity)
        { }

        internal void Add(Tag tag)
        {
            Add(new TagKey(tag));
        }

        internal Boolean ContainsKey(Tag tag)
            => (ContainsKey(new TagKey(tag)));

        internal Int32 this[Tag tag]
            => (base[new TagKey(tag)]);
    }
}