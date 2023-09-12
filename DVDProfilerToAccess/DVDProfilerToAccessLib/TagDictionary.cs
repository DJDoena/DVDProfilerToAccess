using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class TagDictionary : Dictionary<TagKey>
    {
        internal TagDictionary(int capacity) : base(capacity)
        { }

        internal void Add(Profiler.Tag tag) => this.Add(new TagKey(tag));

        internal bool ContainsKey(Profiler.Tag tag) => this.ContainsKey(new TagKey(tag));

        internal int this[Profiler.Tag tag] => base[new TagKey(tag)];
    }
}