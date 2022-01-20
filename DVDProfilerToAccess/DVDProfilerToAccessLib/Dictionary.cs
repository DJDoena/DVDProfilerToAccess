namespace DoenaSoft.DVDProfiler.DVDProfilerToAccess
{
    using System.Collections.Generic;

    internal class Dictionary<TKey> : Dictionary<TKey, int>
    {
        internal Dictionary(int capacity) : base(capacity)
        { }

        internal void Add(TKey key) => this.Add(key, SqlProcessor.Instance.IdCounter++);
    }
}