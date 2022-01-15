namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using Profiler = DVDProfilerXML.Version400;

    internal sealed class CollectionTypeHashtable : Hashtable<Profiler.CollectionType>
    {
        internal CollectionTypeHashtable(int capacity) : base(capacity)
        { }
    }
}