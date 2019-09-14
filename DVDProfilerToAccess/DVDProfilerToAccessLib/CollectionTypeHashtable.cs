using System;
using DoenaSoft.DVDProfiler.DVDProfilerXML.Version400;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class CollectionTypeHashtable : Hashtable<CollectionType>
    {
        internal CollectionTypeHashtable(int capacity)
            : base(capacity)
        { }
    }
}