namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using Profiler = DVDProfilerXML.Version400;

    internal sealed class CollectionTypeDictionary : Dictionary<Profiler.CollectionType>
    {
        internal CollectionTypeDictionary(int capacity) : base(capacity)
        { }
    }
}