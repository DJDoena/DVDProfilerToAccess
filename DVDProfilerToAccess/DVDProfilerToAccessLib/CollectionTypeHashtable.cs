using DoenaSoft.DVDProfiler.DVDProfilerXML.Version390;
using System;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class CollectionTypeHashtable : Hashtable<CollectionType>
    {
        internal CollectionTypeHashtable(Int32 capacity)
            : base(capacity)
        {
        }
    }
}