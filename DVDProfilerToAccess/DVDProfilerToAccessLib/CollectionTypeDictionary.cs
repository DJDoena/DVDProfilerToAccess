using Profiler = DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal sealed class CollectionTypeDictionary : Dictionary<Profiler.CollectionType>
    {
        internal CollectionTypeDictionary(int capacity) : base(capacity)
        { }
    }
}