using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagHashtable : Hashtable<TagKey>
    {
        internal TagHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(Tag tag)
        {
            Add(new TagKey(tag));
        }

        internal bool ContainsKey(Tag tag)
            => (ContainsKey(new TagKey(tag)));

        internal int this[Tag tag]
            => (base[new TagKey(tag)]);
    }
}