namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using Profiler = DVDProfilerXML.Version400;

    internal sealed class TagHashtable : Hashtable<TagKey>
    {
        internal TagHashtable(int capacity)
            : base(capacity)
        { }

        internal void Add(Profiler.Tag tag) => this.Add(new TagKey(tag));

        internal bool ContainsKey(Profiler.Tag tag) => this.ContainsKey(new TagKey(tag));

        internal int this[Profiler.Tag tag] => base[new TagKey(tag)];
    }
}