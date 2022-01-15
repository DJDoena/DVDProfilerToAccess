namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System.Collections.Generic;

    internal class Hashtable<TKey> : Dictionary<TKey, int>
    {
        internal Hashtable(int capacity) : base(capacity)
        { }

        internal void Add(TKey key) => this.Add(key, SqlProcessor.Instance.IdCounter++);
    }
}