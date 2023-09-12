using System.Collections.Generic;

namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    internal class Dictionary<TKey> : Dictionary<TKey, int>
    {
        internal Dictionary(int capacity) : base(capacity)
        { }

        internal void Add(TKey key) => this.Add(key, SqlProcessor.Instance.IdCounter++);
    }
}