using System;
using System.Collections.Generic;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class Hashtable<TKey> : Dictionary<TKey, Int32>
    {
        internal Hashtable(Int32 capacity)
            : base(capacity)
        { }

        internal void Add(TKey key)
        {
            Add(key, SqlProcessor.Instance.IdCounter++);
        }
    }
}